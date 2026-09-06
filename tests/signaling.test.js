const { test } = require('node:test');
const assert = require('node:assert/strict');
const WebSocket = require('ws');
const { startServer } = require('./helpers');

test('isolated viewers, stale signaling, reconnect grace and capacity', { timeout: 30000 }, async (t) => {
  const server = await startServer({ MAX_VIEWERS_PER_STREAM: '2' });
  t.after(() => server.close());
  const clients = [];
  t.after(() => clients.forEach((client) => client.ws.terminate()));
  async function join(id, isLive = false, watching = null) {
    const ws = new WebSocket(server.url.replace('http', 'ws'), { origin: server.url });
    const messages = [];
    ws.on('message', (raw) => messages.push(JSON.parse(raw)));
    const client = {
      ws, messages,
      send: (data) => ws.send(JSON.stringify(data)),
      async take(type, predicate = () => true) {
        const deadline = Date.now() + 5000;
        while (Date.now() < deadline) {
          const i = messages.findIndex((m) => m.type === type && predicate(m));
          if (i !== -1) return messages.splice(i, 1)[0];
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
        throw new Error('Missing ' + type + ' for ' + id);
      }
    };
    clients.push(client);
    await new Promise((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
    client.send({ type: 'JOIN', userId: id, nick: id, isLive, watching });
    await client.take('JOIN_OK');
    return client;
  }
  const source = await join('source-one', true);
  const a = await join('viewer-one');
  const b = await join('viewer-two');
  const c = await join('viewer-three');
  a.send({ type: 'REQUEST_STREAM', target: 'source-one', requestId: 'request-a1' });
  b.send({ type: 'REQUEST_STREAM', target: 'source-one', requestId: 'request-b1' });
  await source.take('REQUEST_STREAM', (m) => m.from === 'viewer-one');
  await source.take('REQUEST_STREAM', (m) => m.from === 'viewer-two');
  assert.equal((await source.take('STREAM_AUDIENCE', (m) => m.count === 2)).viewers.length, 2);
  c.send({ type: 'REQUEST_STREAM', target: 'source-one', requestId: 'request-c1' });
  assert.equal((await c.take('STREAM_FULL')).maxViewers, 2);
  source.send({ type: 'OFFER', target: 'viewer-one', requestId: 'request-a1', sessionId: 'session-a1', sdp: { type: 'offer', sdp: 'v=0' } });
  assert.equal((await a.take('OFFER')).sessionId, 'session-a1');
  a.send({ type: 'REQUEST_STREAM', target: 'source-one', requestId: 'request-a2' });
  await source.take('REQUEST_STREAM', (m) => m.requestId === 'request-a2');
  source.send({ type: 'OFFER', target: 'viewer-one', requestId: 'request-a1', sessionId: 'session-old', sdp: { type: 'offer', sdp: 'v=0' } });
  source.send({ type: 'OFFER', target: 'viewer-one', requestId: 'request-a2', sessionId: 'session-a2', sdp: { type: 'offer', sdp: 'v=0' } });
  assert.equal((await a.take('OFFER')).sessionId, 'session-a2');
  a.send({ type: 'ANSWER', target: 'source-one', sessionId: 'session-a1', sdp: { type: 'answer', sdp: 'v=0' } });
  a.send({ type: 'ANSWER', target: 'source-one', sessionId: 'session-a2', sdp: { type: 'answer', sdp: 'v=0' } });
  assert.equal((await source.take('ANSWER')).sessionId, 'session-a2');
  // A live broadcaster remains discoverable while its signaling reconnects.
  source.ws.terminate();
  await new Promise((resolve) => setTimeout(resolve, 50));
  a.send({ type: 'REQUEST_STREAM', target: 'source-one', requestId: 'request-a3' });
  await a.take('STREAM_WAIT');
  const resumed = await join('source-one', true);
  await a.take('STREAM_RETRY');
  assert.equal((await resumed.take('STREAM_AUDIENCE', (m) => m.count === 2)).count, 2);
  a.send({ type: 'STOP_WATCH', target: 'source-one' });
  await resumed.take('STOP_WATCH', (m) => m.from === 'viewer-one');
  c.send({ type: 'REQUEST_STREAM', target: 'source-one', requestId: 'request-c2' });
  await resumed.take('REQUEST_STREAM', (m) => m.from === 'viewer-three');
  // Stopping while offline must remove the old subscription on JOIN.
  b.ws.terminate();
  await new Promise((resolve) => setTimeout(resolve, 30));
  await join('viewer-two', false, null);
  await resumed.take('STOP_WATCH', (m) => m.from === 'viewer-two');
  resumed.send({ type: 'LIVE_STATE_CHANGE', isLive: false });
  await c.take('STREAM_ENDED');
  const health = await (await fetch(server.url + '/health')).json();
  assert.equal(health.activeViewers, 0);
});
