const { test } = require('node:test');
const assert = require('node:assert/strict');
const { mkdirSync } = require('node:fs');
const { chromium, expect } = require('@playwright/test');
const { startServer } = require('./helpers');

test('real WebRTC: viewers, audio controls, reconnect and async cancellation', { timeout: 180000 }, async (t) => {
  const server = await startServer();
  t.after(() => server.close());
  const browser = await chromium.launch({
    channel: process.env.PLAYWRIGHT_CHANNEL || 'msedge', headless: true,
    args: ['--use-fake-ui-for-media-stream', '--use-fake-device-for-media-stream', '--autoplay-policy=user-gesture-required']
  });
  t.after(() => browser.close());
  const errors = [];
  async function pageIn(context, url = server.url) {
    const page = await context.newPage();
    page.on('pageerror', (error) => errors.push(error.message));
    page.on('dialog', (dialog) => dialog.dismiss());
    await page.goto(url);
    await page.waitForFunction(() => Boolean(wsSessionToken));
    return page;
  }
  const sourceContext = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await sourceContext.addInitScript(() => {
    // Only capture devices are synthetic; encoding, RTP, ICE and playback are real.
    navigator.mediaDevices.getDisplayMedia = async () => {
      const canvas = document.createElement('canvas');
      canvas.width = 1280; canvas.height = 720;
      const ctx = canvas.getContext('2d');
      let frame = 0;
      setInterval(() => {
        ctx.fillStyle = '#152d49'; ctx.fillRect(0, 0, 1280, 720);
        ctx.fillStyle = '#5cdbb7'; ctx.fillRect((frame++ * 8) % 1180, 280, 100, 100);
        ctx.fillStyle = '#fff'; ctx.font = '32px sans-serif'; ctx.fillText('Teste de transmissão • ' + frame, 50, 70);
      }, 50);
      const stream = canvas.captureStream(20);
      window.testCaptureStream = stream;
      // No system audio: verifies microphone can be added after viewers connect.
      return stream;
    };
  });
  const source = await pageIn(sourceContext);
  // Explicit standalone mode retains optional voice capture for users outside a call.
  await source.getByRole('checkbox', { name: 'Já estou em uma call' }).uncheck();
  await source.getByRole('button', { name: 'Transmitir Tela', exact: true }).click();
  await source.waitForFunction(() => isSharing);
  const sourceId = await source.evaluate(() => myId);
  await source.waitForFunction(() => audioStatus.textContent.includes('Sem som da tela'));
  const viewers = [];
  for (let i = 0; i < 3; i++) {
    const context = await browser.newContext();
    const page = await pageIn(context, server.url + '/?watch=' + sourceId);
    viewers.push({ page, context });
  }
  async function hasVideo(page) {
    await page.waitForFunction(() => activePC?.connectionState === 'connected' && viewerVideoHealthy && videoEl.videoWidth > 0, { timeout: 30000 });
  }
  await Promise.all(viewers.map(({ page }) => hasVideo(page)));
  await source.waitForFunction(() => senderPeers.size === 3 && currentAudience.length === 3);
  const sessions = await Promise.all(viewers.map(({ page }) => page.evaluate(() => activeSessionId)));
  assert.equal(new Set(sessions).size, 3);

  // Enabling the microphone reaches every existing observer without new sessions.
  await source.getByRole('button', { name: 'Ativar microfone', exact: true }).click();
  await source.waitForFunction(() => microphoneStream?.getAudioTracks()[0]?.readyState === 'live');
  for (const { page } of viewers) {
    await expect.poll(() => page.evaluate(async () => {
      const report = await activePC.getStats();
      return [...report.values()].some((s) => s.type === 'inbound-rtp' && s.kind === 'audio' && s.totalAudioEnergy > 0);
    }), { timeout: 30000 }).toBe(true);
  }
  assert.deepEqual(await Promise.all(viewers.map(({ page }) => page.evaluate(() => activeSessionId))), sessions);
  assert.equal(await source.evaluate(() => videoEl.muted && videoEl.srcObject.getAudioTracks().length === 0), true);

  const a = viewers[0].page;
  const autoplayFallback = await a.evaluate(async () => {
    const nativePlay = videoEl.play;
    let blocked = true;
    videoEl.play = function () {
      if (blocked) { blocked = false; return Promise.reject(new DOMException('User gesture required', 'NotAllowedError')); }
      return nativePlay.call(this);
    };
    videoEl.muted = false;
    await playViewerMedia();
    videoEl.play = nativePlay;
    return { muted: videoEl.muted, notice: unmuteNotice.style.display };
  });
  assert.deepEqual(autoplayFallback, { muted: true, notice: 'block' });
  await a.getByRole('button', { name: '🔊 Ativar áudio da transmissão', exact: true }).click();
  await a.waitForFunction(() => !videoEl.muted && !videoEl.paused && unmuteNotice.style.display === 'none');
  // A transient signaling drop re-registers the viewer without affecting others.
  await a.evaluate(() => ws.close());
  await a.waitForFunction((old) => activeSessionId && activeSessionId !== old && viewerVideoHealthy, sessions[0]);
  assert.equal(await a.evaluate(() => videoEl.muted), false);
  assert.equal(await viewers[1].page.evaluate(() => activeSessionId), sessions[1]);
  await a.getByRole('button', { name: 'Silenciar áudio', exact: true }).click();
  await a.evaluate(() => ws.close());
  await a.waitForFunction(() => Boolean(wsSessionToken) && viewerVideoHealthy);
  assert.equal(await a.evaluate(() => videoEl.muted), true);

  // Broadcaster signaling loss restores all subscriptions.
  const beforeDrop = await Promise.all(viewers.map(({ page }) => page.evaluate(() => activeSessionId)));
  await source.evaluate(() => ws.close());
  await Promise.all(viewers.map(({ page }, i) => page.waitForFunction((old) => activeSessionId && activeSessionId !== old && viewerVideoHealthy, beforeDrop[i])));
  assert.equal(await source.evaluate(() => senderPeers.size), 3);

  // Offline pauses retries; an online event reconnects automatically.
  const offlineSession = await a.evaluate(() => activeSessionId);
  await viewers[0].context.setOffline(true);
  await a.waitForFunction(() => navigator.onLine === false);
  await viewers[0].context.setOffline(false);
  await a.waitForFunction((old) => activeSessionId && activeSessionId !== old && viewerVideoHealthy, offlineSession);

  // Opening the broadcaster's own link in the same profile uses a separate viewer identity.
  const selfViewer = await pageIn(sourceContext, server.url + '/?watch=' + sourceId);
  await hasVideo(selfViewer);
  assert.notEqual(await selfViewer.evaluate(() => myId), sourceId);
  assert.equal(await source.evaluate(() => allowReconnect && isSharing), true);
  assert.equal(await selfViewer.evaluate(() => videoEl.muted && !viewerWantsAudio), true);
  await selfViewer.evaluate(() => unmute());
  assert.equal(await selfViewer.evaluate(() => videoEl.muted), true);
  await selfViewer.getByRole('button', { name: 'Desconectar', exact: true }).click();
  await source.waitForFunction(() => senderPeers.size === 3);

  // A bandwidth-limited peer keeps its reduced bitrate when the audience changes.
  const budgetResult = await source.evaluate(async () => {
    const peer = [...senderPeers.values()][0];
    peer.targetBitrate = 400000;
    await rebalanceSenderBitrates();
    return { bitrate: peer.targetBitrate, encoding: peer.videoSender.getParameters().encodings[0] };
  });
  assert.equal(budgetResult.bitrate, 400000);
  assert.equal(budgetResult.encoding.maxFramerate, 15);
  assert.equal(budgetResult.encoding.scaleResolutionDownBy, 4);

  // The user leaves while an incoming offer is awaiting TURN config.
  const staleResult = await a.evaluate(async () => {
    let resolveConfig;
    const originalRefresh = refreshRtcConfig;
    refreshRtcConfig = () => new Promise((resolve) => { resolveConfig = resolve; });
    const operation = handleIncomingOffer({ from: desiredWatchId, requestId: watchRequestId, sessionId: 'stale-session', sdp: { type: 'offer', sdp: 'v=0' } });
    disconnectStream();
    resolveConfig();
    await operation;
    refreshRtcConfig = originalRefresh;
    return { desired: desiredWatchId, active: Boolean(activePC), attached: Boolean(videoEl.srcObject) };
  });
  assert.deepEqual(staleResult, { desired: null, active: false, attached: false });
  await source.waitForFunction(() => senderPeers.size === 2);
  await hasVideo(viewers[1].page);

  mkdirSync('test-results', { recursive: true });
  await source.screenshot({ path: 'test-results/transmitter.png' });
  await viewers[1].page.screenshot({ path: 'test-results/viewer.png' });
  await source.getByRole('button', { name: 'Desativar microfone', exact: true }).click();
  const deniedMic = await source.evaluate(async () => {
    const original = navigator.mediaDevices.getUserMedia;
    navigator.mediaDevices.getUserMedia = () => Promise.reject(new DOMException('Denied', 'NotAllowedError'));
    await toggleMicrophone();
    navigator.mediaDevices.getUserMedia = original;
    return { sharing: isSharing, message: audioStatus.textContent, microphone: microphoneStream };
  });
  assert.equal(deniedMic.sharing, true);
  assert.equal(deniedMic.microphone, null);
  assert.match(deniedMic.message, /Permita o acesso/);
  const lateMic = await source.evaluate(async () => {
    const original = navigator.mediaDevices.getUserMedia;
    const stream = await original.call(navigator.mediaDevices, { audio: true });
    let resolveInput;
    navigator.mediaDevices.getUserMedia = () => new Promise((resolve) => { resolveInput = resolve; });
    const operation = toggleMicrophone();
    while (!resolveInput) await new Promise((resolve) => setTimeout(resolve, 0));
    stopShare();
    resolveInput(stream);
    await operation;
    navigator.mediaDevices.getUserMedia = original;
    return stream.getTracks().every((track) => track.readyState === 'ended');
  });
  assert.equal(lateMic, true);
  await viewers[1].page.waitForFunction(() => !desiredWatchId && !activePC);
  assert.equal(await source.evaluate(() => testCaptureStream.getVideoTracks()[0].readyState), 'ended');
  assert.equal(await source.evaluate(() => audioContext === null && microphoneStream === null && senderPeers.size === 0), true);
  assert.deepEqual(errors, []);
});
