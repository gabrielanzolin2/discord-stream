const { test } = require('node:test');
const assert = require('node:assert/strict');
const { chromium, expect } = require('@playwright/test');
const { startServer } = require('./helpers');

test('call mode prevents system loopback and preserves content audio', { timeout: 90000 }, async (t) => {
  const server = await startServer();
  t.after(() => server.close());
  const browser = await chromium.launch({ channel: process.env.PLAYWRIGHT_CHANNEL || 'msedge', headless: true });
  t.after(() => browser.close());

  for (const surface of ['monitor', 'window', 'browser']) {
    await t.test(surface + ' capture', async (t) => {
      const context = await browser.newContext();
      const viewerContext = await browser.newContext();
      t.after(() => context.close());
      t.after(() => viewerContext.close());
      const errors = [];
      await context.addInitScript((surface) => {
        window.microphoneRequests = 0;
        navigator.mediaDevices.getUserMedia = () => {
          window.microphoneRequests += 1;
          throw new Error('A call must not request a second microphone');
        };
        navigator.mediaDevices.getDisplayMedia = async (options) => {
          window.captureOptions = options;
          const canvas = document.createElement('canvas');
          canvas.width = 640; canvas.height = 360;
          const ctx = canvas.getContext('2d');
          let frame = 0;
          setInterval(() => {
            ctx.fillStyle = '#192c3a'; ctx.fillRect(0, 0, 640, 360);
            ctx.fillStyle = '#23a55a'; ctx.fillRect((frame++ * 5) % 600, 100, 40, 40);
          }, 50);
          const stream = canvas.captureStream(20);
          const track = stream.getVideoTracks()[0];
          const getSettings = track.getSettings.bind(track);
          track.getSettings = () => ({ ...getSettings(), displaySurface: surface });
          const toneContext = new AudioContext();
          await toneContext.resume();
          const tone = toneContext.createOscillator();
          const destination = toneContext.createMediaStreamDestination();
          tone.connect(destination); tone.start();
          window.capturedAudioTrack = destination.stream.getAudioTracks()[0];
          // Monitor/window deliberately ignore exclusion hints to exercise the fallback.
          stream.addTrack(window.capturedAudioTrack);
          return stream;
        };
      }, surface);
      const source = await context.newPage();
      source.on('pageerror', (err) => errors.push(err.message));
      await source.goto(server.url);
      await source.waitForFunction(() => Boolean(wsSessionToken));
      assert.equal(await source.getByRole('checkbox', { name: 'Já estou em uma call' }).isChecked(), true);
      await source.getByRole('button', { name: 'Transmitir Tela', exact: true }).click();
      await source.waitForFunction(() => isSharing);
      const sourceId = await source.evaluate(() => myId);
      const capture = await source.evaluate(async () => {
        await toggleMicrophone();
        return {
          options: captureOptions,
          micRequests: microphoneRequests,
          previewTracks: videoEl.srcObject.getAudioTracks().length,
          captureState: capturedAudioTrack.readyState,
          blocked: captureAudioBlocked
        };
      });
      assert.equal(capture.options.systemAudio, 'exclude');
      assert.equal(capture.options.windowAudio, 'exclude');
      assert.equal(capture.options.audio.restrictOwnAudio, true);
      assert.equal(capture.micRequests, 0);
      assert.equal(capture.previewTracks, 0);
      assert.equal(capture.captureState, surface === 'browser' ? 'live' : 'ended');
      assert.equal(capture.blocked, surface !== 'browser');
      assert.equal(await source.getByRole('button', { name: 'Ativar microfone', exact: true }).isVisible(), false);
      const viewer = await viewerContext.newPage();
      viewer.on('pageerror', (err) => errors.push(err.message));
      await viewer.goto(server.url + '/?watch=' + sourceId);
      await expect.poll(() => viewer.evaluate(async (surface) => {
        if (!viewerVideoHealthy || !activePC) return false;
        const stats = [...(await activePC.getStats()).values()];
        const hasVideo = stats.some((s) => s.type === 'inbound-rtp' && s.kind === 'video' && s.framesDecoded > 20);
        const hasAudio = stats.some((s) => s.type === 'inbound-rtp' && s.kind === 'audio' && s.totalSamplesReceived > 8000);
        // An empty audio mix can emit no packets at all. Require progressing video
        // before checking that no system audio leaked; tab capture must carry audio.
        return hasVideo && (surface !== 'browser' || hasAudio);
      }, surface), { timeout: 30000 }).toBe(true);
      const energy = await viewer.evaluate(async () => {
        const stats = await activePC.getStats();
        return [...stats.values()].find((s) => s.type === 'inbound-rtp' && s.kind === 'audio')?.totalAudioEnergy ?? 0;
      });
      if (surface === 'browser') assert.ok(energy > 0, 'Selected content audio must reach the viewer');
      else assert.ok(energy < 1e-7, 'System/call audio must not reach the viewer');
      await source.getByRole('button', { name: 'Parar Transmissão', exact: true }).click();
      await viewer.waitForFunction(() => !desiredWatchId && !activePC);
      assert.deepEqual(errors, []);
    });
  }
});
