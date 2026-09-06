const express = require('express');
const http = require('http');
const crypto = require('crypto');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT || 3000);
const APP_VERSION = process.env.APP_VERSION || (process.env.RENDER_GIT_COMMIT ? process.env.RENDER_GIT_COMMIT.slice(0, 12) : '2026.09.06-stream-4');
const DEFAULT_ROOM = 'sala-principal';
const STREAM_UPLOAD_BUDGET_BPS = Math.max(8_000_000, Number(process.env.STREAM_UPLOAD_BUDGET_BPS || 36_000_000));
// Reserva 20% para overhead e 96 kbps de áudio por observador.
const MAX_VIEWERS_PER_STREAM = Math.max(2, Math.min(50, Math.floor(STREAM_UPLOAD_BUDGET_BPS * 0.8 / 246_000), Number(process.env.MAX_VIEWERS_PER_STREAM || 8)));
const WS_RATE_WINDOW_MS = 10_000;
const WS_RATE_MAX_MESSAGES = Math.max(100, Number(process.env.WS_RATE_MAX_MESSAGES || 350));
const RTC_CONFIG_RATE_LIMIT = Math.max(10, Number(process.env.RTC_CONFIG_RATE_LIMIT || 60));
const FORCE_TURN_RELAY = String(process.env.FORCE_TURN_RELAY || '').toLowerCase() === 'true';
const DISCONNECT_GRACE_MS = 30_000;

const TURN_URLS = String(process.env.TURN_URLS || '')
  .split(',')
  .map(v => v.trim())
  .filter(Boolean);
const TURN_SECRET = String(process.env.TURN_SECRET || '').trim();
const TURN_USERNAME = String(process.env.TURN_USERNAME || '').trim();
const TURN_CREDENTIAL = String(process.env.TURN_CREDENTIAL || '').trim();
const ALLOW_STATIC_TURN_CREDENTIALS = String(process.env.ALLOW_STATIC_TURN_CREDENTIALS || '').toLowerCase() === 'true';
const TURN_TTL_SECONDS = Math.max(300, Math.min(86400, Number(process.env.TURN_TTL_SECONDS || 3600)));

const ALLOWED_ORIGINS = String(process.env.ALLOWED_ORIGINS || '')
  .split(',')
  .map(v => v.trim().replace(/\/$/, ''))
  .filter(Boolean);

const STUN_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },
  { urls: 'stun:stun.cloudflare.com:3478' }
];

function isAllowedWebSocketOrigin(req) {
  const origin = String(req.headers.origin || '').trim().replace(/\/$/, '');
  if (!origin) return process.env.NODE_ENV !== 'production';

  if (ALLOWED_ORIGINS.length > 0) {
    return ALLOWED_ORIGINS.includes(origin);
  }

  try {
    const originUrl = new URL(origin);
    const forwardedHost = String(req.headers['x-forwarded-host'] || '').split(',')[0].trim();
    const host = forwardedHost || String(req.headers.host || '').trim();
    return Boolean(host) && originUrl.host === host;
  } catch (_) {
    return false;
  }
}

const wss = new WebSocket.Server({
  server,
  maxPayload: 384 * 1024,
  perMessageDeflate: false,
  verifyClient: ({ req }, done) => {
    if (!isAllowedWebSocketOrigin(req)) {
      done(false, 403, 'Origin not allowed');
      return;
    }
    done(true);
  }
});

app.set('trust proxy', 1);
app.disable('x-powered-by');

app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Permissions-Policy', 'camera=(), microphone=(self), display-capture=(self), fullscreen=(self)');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
  res.setHeader('Cross-Origin-Resource-Policy', 'same-origin');
  res.setHeader('Content-Security-Policy', "frame-ancestors 'none'; base-uri 'self'; object-src 'none'; form-action 'none'");

  const forwardedProto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim();
  if (req.secure || forwardedProto === 'https') {
    res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
  }
  next();
});

// Gerenciamento de usuários, salas e audiência
const activeUsers = new Map(); // userId -> { ws, nick, isLive, room, joinedAt, sessionToken }
const rooms = new Map();       // room -> Map(userId -> ws)
const streamViewers = new Map(); // broadcasterId -> Map(viewerId -> { joinedAt })
const viewerWatching = new Map(); // viewerId -> broadcasterId
const disconnectGraceTimers = new Map();
const rtcConfigRateBuckets = new Map();

function normalizeId(value) {
  const id = String(value || '').trim().toLowerCase();
  return /^[a-z0-9_-]{3,64}$/.test(id) ? id : null;
}

function normalizeRoom(value) {
  const room = String(value || DEFAULT_ROOM).trim().toLowerCase();
  return /^[a-z0-9_-]{1,64}$/.test(room) ? room : DEFAULT_ROOM;
}

function normalizeNick(value) {
  const nick = String(value || 'Gamer').trim().replace(/[\u0000-\u001F\u007F]/g, '');
  return (nick || 'Gamer').slice(0, 48);
}

function normalizeSessionId(value) {
  const id = String(value || '').trim();
  return /^[a-zA-Z0-9_-]{8,128}$/.test(id) ? id : null;
}

function validDescription(value, expectedType) {
  return Boolean(
    value &&
    typeof value === 'object' &&
    value.type === expectedType &&
    typeof value.sdp === 'string' &&
    value.sdp.length > 0 &&
    value.sdp.length <= 256 * 1024
  );
}

function validCandidate(value) {
  if (!value || typeof value !== 'object') return false;
  if (typeof value.candidate !== 'string' || value.candidate.length > 8192) return false;
  if (value.sdpMid != null && String(value.sdpMid).length > 128) return false;
  return true;
}

function secureTokenEquals(a, b) {
  const left = Buffer.from(String(a || ''));
  const right = Buffer.from(String(b || ''));
  return left.length > 0 && left.length === right.length && crypto.timingSafeEqual(left, right);
}

function safeSend(ws, payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  if (ws.bufferedAmount > 2 * 1024 * 1024) return false;
  try {
    ws.send(JSON.stringify(payload));
    return true;
  } catch (_) {
    return false;
  }
}

function getRoomClients(room) {
  if (!rooms.has(room)) rooms.set(room, new Map());
  return rooms.get(room);
}

function broadcastToRoom(room, senderId, payload) {
  const clients = rooms.get(room);
  if (!clients) return;
  for (const [id, clientWs] of clients) {
    if (id !== senderId) safeSend(clientWs, payload);
  }
}

function getAudienceMap(broadcasterId) {
  if (!streamViewers.has(broadcasterId)) streamViewers.set(broadcasterId, new Map());
  return streamViewers.get(broadcasterId);
}

function audienceSnapshot(broadcasterId) {
  const broadcaster = activeUsers.get(broadcasterId);
  const audience = streamViewers.get(broadcasterId);
  if (!audience) return [];

  const viewers = [];
  for (const [viewerId, meta] of audience) {
    const viewer = activeUsers.get(viewerId);
    if (!viewer) continue;
    if (broadcaster && viewer.room !== broadcaster.room) continue;
    viewers.push({
      userId: viewerId,
      nick: viewer.nick,
      joinedAt: meta.joinedAt
    });
  }
  viewers.sort((a, b) => a.joinedAt - b.joinedAt);
  return viewers;
}

function sendAudienceSnapshot(broadcasterId) {
  const viewers = audienceSnapshot(broadcasterId);
  const payload = {
    type: 'STREAM_AUDIENCE',
    broadcasterId,
    count: viewers.length,
    maxViewers: MAX_VIEWERS_PER_STREAM,
    viewers
  };

  const broadcaster = activeUsers.get(broadcasterId);
  if (broadcaster) safeSend(broadcaster.ws, payload);

  const audience = streamViewers.get(broadcasterId);
  if (!audience) return;
  for (const viewerId of audience.keys()) {
    const viewer = activeUsers.get(viewerId);
    if (viewer) safeSend(viewer.ws, payload);
  }
}

function removeViewerFromStream(viewerId, notifyBroadcaster = true) {
  const broadcasterId = viewerWatching.get(viewerId);
  if (!broadcasterId) return null;

  viewerWatching.delete(viewerId);
  const audience = streamViewers.get(broadcasterId);
  if (audience) {
    audience.delete(viewerId);
    if (audience.size === 0) streamViewers.delete(broadcasterId);
  }

  if (notifyBroadcaster) {
    const broadcaster = activeUsers.get(broadcasterId);
    if (broadcaster) {
      safeSend(broadcaster.ws, {
        type: 'STOP_WATCH',
        from: viewerId
      });
    }
  }

  sendAudienceSnapshot(broadcasterId);
  return broadcasterId;
}

function addViewerToStream(viewerId, broadcasterId) {
  const currentBroadcaster = viewerWatching.get(viewerId);
  if (currentBroadcaster && currentBroadcaster !== broadcasterId) {
    removeViewerFromStream(viewerId, true);
  }

  const audience = getAudienceMap(broadcasterId);
  if (!audience.has(viewerId) && audience.size >= MAX_VIEWERS_PER_STREAM) {
    return false;
  }

  if (!audience.has(viewerId)) audience.set(viewerId, { joinedAt: Date.now() });
  viewerWatching.set(viewerId, broadcasterId);
  sendAudienceSnapshot(broadcasterId);
  return true;
}

function endStreamAudience(broadcasterId, reason = 'ended') {
  const audience = streamViewers.get(broadcasterId);
  if (!audience) return;

  for (const viewerId of audience.keys()) {
    if (viewerWatching.get(viewerId) === broadcasterId) viewerWatching.delete(viewerId);
    const viewer = activeUsers.get(viewerId);
    if (viewer) {
      safeSend(viewer.ws, {
        type: 'STREAM_ENDED',
        broadcasterId,
        reason
      });
    }
  }

  streamViewers.delete(broadcasterId);
  const broadcaster = activeUsers.get(broadcasterId);
  if (broadcaster) {
    safeSend(broadcaster.ws, {
      type: 'STREAM_AUDIENCE',
      broadcasterId,
      count: 0,
      maxViewers: MAX_VIEWERS_PER_STREAM,
      viewers: []
    });
  }
}

function sameRoom(userA, userB) {
  return Boolean(userA && userB && userA.room === userB.room);
}

function streamPeerAuthorized(fromId, targetId) {
  return viewerWatching.get(fromId) === targetId || viewerWatching.get(targetId) === fromId;
}

function consumeWsRate(ws) {
  const now = Date.now();
  if (!ws.rateWindowStartedAt || now - ws.rateWindowStartedAt >= WS_RATE_WINDOW_MS) {
    ws.rateWindowStartedAt = now;
    ws.rateMessageCount = 0;
  }
  ws.rateMessageCount = (ws.rateMessageCount || 0) + 1;
  return ws.rateMessageCount <= WS_RATE_MAX_MESSAGES;
}

wss.on('connection', (ws) => {
  let userId = null;
  let userNick = null;
  let userRoom = DEFAULT_ROOM;

  ws.isAlive = true;
  ws.rateWindowStartedAt = Date.now();
  ws.rateMessageCount = 0;
  ws.on('pong', () => { ws.isAlive = true; });

  safeSend(ws, {
    type: 'SERVER_HELLO',
    appVersion: APP_VERSION,
    serverTime: Date.now(),
    maxViewers: MAX_VIEWERS_PER_STREAM
  });

  ws.on('message', (message) => {
    try {
      if (!consumeWsRate(ws)) {
        safeSend(ws, { type: 'ERROR', code: 'RATE_LIMITED' });
        try { ws.close(1008, 'Rate limited'); } catch (_) {}
        return;
      }

      const data = JSON.parse(message.toString());
      if (!data || typeof data.type !== 'string') return;

      if (data.type === 'PING') {
        safeSend(ws, { type: 'PONG', t: data.t || Date.now(), serverTime: Date.now() });
        return;
      }

      if (data.type === 'JOIN') {
        if (userId) return; // Uma identidade por socket.
        const nextUserId = normalizeId(data.userId);
        if (!nextUserId) {
          safeSend(ws, { type: 'ERROR', code: 'INVALID_USER_ID' });
          return;
        }

        userId = nextUserId;
        userNick = normalizeNick(data.nick);
        userRoom = normalizeRoom(data.room);
        const isLive = Boolean(data.isLive);
        const desiredBroadcaster = normalizeId(data.watching);
        if (viewerWatching.has(userId) && (isLive || viewerWatching.get(userId) !== desiredBroadcaster)) {
          removeViewerFromStream(userId, true);
        }

        const graceTimer = disconnectGraceTimers.get(userId);
        if (graceTimer) {
          clearTimeout(graceTimer);
          disconnectGraceTimers.delete(userId);
        }

        const previous = activeUsers.get(userId);
        if (previous && previous.room !== userRoom) {
          rooms.get(previous.room)?.delete(userId);
          removeViewerFromStream(userId, true);
          endStreamAudience(userId, 'room_changed');
        }
        if (previous && previous.ws !== ws) {
          safeSend(previous.ws, { type: 'SESSION_REPLACED' });
          try { previous.ws.close(4001, 'Session replaced'); } catch (_) {}
        }

        const sessionToken = crypto.randomBytes(32).toString('base64url');
        activeUsers.set(userId, {
          ws,
          nick: userNick,
          isLive,
          room: userRoom,
          joinedAt: Date.now(),
          sessionToken
        });

        getRoomClients(userRoom).set(userId, ws);

        broadcastToRoom(userRoom, userId, {
          type: 'USER_JOINED',
          userId,
          nick: userNick
        });

        const liveUsers = [];
        for (const [id, user] of activeUsers) {
          if (user.room === userRoom && user.isLive && id !== userId) {
            liveUsers.push({ userId: id, nick: user.nick });
          }
        }

        safeSend(ws, {
          type: 'JOIN_OK',
          userId,
          nick: userNick,
          room: userRoom,
          sessionToken,
          appVersion: APP_VERSION,
          maxViewers: MAX_VIEWERS_PER_STREAM,
          streamUploadBudgetBps: STREAM_UPLOAD_BUDGET_BPS
        });
        safeSend(ws, { type: 'SYNC_LIVE_USERS', liveUsers });

        if (isLive) {
          sendAudienceSnapshot(userId);
          for (const viewerId of (streamViewers.get(userId) || new Map()).keys()) {
            safeSend(activeUsers.get(viewerId)?.ws, { type: 'STREAM_RETRY', from: userId });
          }
        } else {
          endStreamAudience(userId, 'broadcaster_stopped');
        }
        const watchedBroadcaster = viewerWatching.get(userId);
        if (watchedBroadcaster) sendAudienceSnapshot(watchedBroadcaster);
        return;
      }

      const currentUser = userId ? activeUsers.get(userId) : null;
      if (!userId || !currentUser || currentUser.ws !== ws) return;

      if (data.type === 'FRIEND_REQUEST') {
        const targetId = normalizeId(data.targetId);
        if (!targetId) return;
        const targetClient = activeUsers.get(targetId);

        if (targetClient) {
          safeSend(targetClient.ws, {
            type: 'FRIEND_REQUEST_INCOMING',
            fromId: userId,
            fromNick: userNick
          });
        } else {
          safeSend(ws, { type: 'FRIEND_NOT_FOUND', targetId });
        }
        return;
      }

      if (data.type === 'FRIEND_RESPONSE') {
        const targetId = normalizeId(data.targetId);
        if (!targetId) return;
        const targetClient = activeUsers.get(targetId);

        if (targetClient) {
          safeSend(targetClient.ws, {
            type: 'FRIEND_RESPONSE_RESULT',
            fromId: userId,
            fromNick: userNick,
            accepted: Boolean(data.accepted)
          });
        }
        return;
      }

      if (data.type === 'LIVE_STATE_CHANGE') {
        const isLive = Boolean(data.isLive);
        currentUser.isLive = isLive;

        if (!isLive) endStreamAudience(userId, 'broadcaster_stopped');

        broadcastToRoom(userRoom, userId, {
          type: 'USER_LIVE_STATE',
          userId,
          nick: userNick,
          isLive
        });
        return;
      }

      if (data.type === 'REQUEST_STREAM') {
        const targetId = normalizeId(data.target);
        const requestId = normalizeSessionId(data.requestId);
        if (!targetId || targetId === userId || !requestId) return;

        const broadcaster = activeUsers.get(targetId);
        if (!broadcaster || !broadcaster.isLive || !sameRoom(currentUser, broadcaster)) {
          safeSend(ws, { type: 'STREAM_NOT_FOUND', targetId });
          return;
        }

        if (broadcaster.ws.readyState !== WebSocket.OPEN) {
          safeSend(ws, { type: 'STREAM_WAIT', targetId });
          return;
        }

        const existing = streamViewers.get(targetId)?.get(userId);
        if (existing?.requestId === requestId) return;

        if (!addViewerToStream(userId, targetId)) {
          safeSend(ws, {
            type: 'STREAM_FULL',
            targetId,
            maxViewers: MAX_VIEWERS_PER_STREAM
          });
          return;
        }

        Object.assign(getAudienceMap(targetId).get(userId), { requestId, sessionId: null });

        const codecMode = ['vp8', 'h264', 'auto'].includes(String(data.codecMode || '').toLowerCase())
          ? String(data.codecMode).toLowerCase()
          : 'vp8';

        safeSend(broadcaster.ws, {
          type: 'REQUEST_STREAM',
          from: userId,
          fromNick: userNick,
          requestId,
          forceRelay: Boolean((data.forceRelay || FORCE_TURN_RELAY) && TURN_URLS.length),
          codecMode
        });
        return;
      }

      if (data.type === 'STOP_WATCH') {
        const targetId = normalizeId(data.target);
        const actualBroadcaster = viewerWatching.get(userId);
        if (!actualBroadcaster) return;
        if (targetId && targetId !== actualBroadcaster) return;
        removeViewerFromStream(userId, true);
        return;
      }

      if (data.type === 'OFFER') {
        const targetId = normalizeId(data.target);
        const sessionId = normalizeSessionId(data.sessionId);
        const targetClient = targetId ? activeUsers.get(targetId) : null;
        if (!targetId || !targetClient || !sameRoom(currentUser, targetClient)) return;
        if (viewerWatching.get(targetId) !== userId) return;
        if (!sessionId || !validDescription(data.sdp, 'offer')) return;
        const watch = streamViewers.get(userId)?.get(targetId);
        if (!watch || watch.requestId !== data.requestId) return;
        watch.sessionId = sessionId;

        safeSend(targetClient.ws, {
          type: 'OFFER',
          from: userId,
          fromNick: userNick,
          sessionId,
          requestId: watch.requestId,
          sdp: data.sdp,
          quality: data.quality && typeof data.quality === 'object' ? data.quality : undefined,
          forceRelay: Boolean(data.forceRelay),
          codecMode: ['vp8', 'h264', 'auto'].includes(String(data.codecMode || '').toLowerCase())
            ? String(data.codecMode).toLowerCase()
            : 'vp8'
        });
        return;
      }

      if (data.type === 'ANSWER') {
        const targetId = normalizeId(data.target);
        const sessionId = normalizeSessionId(data.sessionId);
        const targetClient = targetId ? activeUsers.get(targetId) : null;
        if (!targetId || !targetClient || !sameRoom(currentUser, targetClient)) return;
        if (viewerWatching.get(userId) !== targetId) return;
        if (!sessionId || !validDescription(data.sdp, 'answer')) return;
        if (streamViewers.get(targetId)?.get(userId)?.sessionId !== sessionId) return;

        safeSend(targetClient.ws, {
          type: 'ANSWER',
          from: userId,
          fromNick: userNick,
          sessionId,
          sdp: data.sdp
        });
        return;
      }

      if (data.type === 'VIDEO_HEALTH') {
        const targetId = normalizeId(data.target);
        const targetClient = targetId ? activeUsers.get(targetId) : null;
        if (!targetId || !targetClient || !sameRoom(currentUser, targetClient)) return;
        if (viewerWatching.get(userId) !== targetId) return;
        if (streamViewers.get(targetId)?.get(userId)?.sessionId !== data.sessionId) return;
        safeSend(targetClient.ws, {
          type: 'VIDEO_HEALTH',
          from: userId,
          sessionId: data.sessionId,
          status: data.status === 'ready' ? 'ready' : 'black',
          framesReceived: Math.max(0, Number(data.framesReceived || 0)),
          framesDecoded: Math.max(0, Number(data.framesDecoded || 0))
        });
        return;
      }

      if (data.type === 'CANDIDATE') {
        const targetId = normalizeId(data.target);
        const sessionId = normalizeSessionId(data.sessionId);
        const targetClient = targetId ? activeUsers.get(targetId) : null;
        if (!targetId || !targetClient || !sameRoom(currentUser, targetClient)) return;
        if (!streamPeerAuthorized(userId, targetId)) return;
        if (!sessionId || !validCandidate(data.candidate)) return;
        const broadcasterId = viewerWatching.get(userId) === targetId ? targetId : userId;
        const viewerId = broadcasterId === userId ? targetId : userId;
        if (streamViewers.get(broadcasterId)?.get(viewerId)?.sessionId !== sessionId) return;

        safeSend(targetClient.ws, {
          type: 'CANDIDATE',
          from: userId,
          fromNick: userNick,
          sessionId,
          candidate: data.candidate
        });
        return;
      }

      if (data.type === 'STREAM_RETRY') {
        const targetId = normalizeId(data.target);
        const watch = streamViewers.get(userId)?.get(targetId);
        if (!watch || watch.sessionId !== data.sessionId) return;
        safeSend(activeUsers.get(targetId)?.ws, {
          type: 'STREAM_RETRY', from: userId, sessionId: data.sessionId
        });
      }
    } catch (err) {
      console.error('Erro WebSocket:', err.message);
      safeSend(ws, { type: 'ERROR', code: 'BAD_MESSAGE' });
    }
  });

  ws.on('error', (err) => {
    console.warn('WebSocket error:', err.message);
  });

  ws.on('close', () => {
    if (!userId) return;

    const current = activeUsers.get(userId);
    if (!current || current.ws !== ws) return;

    const roomClients = rooms.get(userRoom);
    if (roomClients?.get(userId) === ws) {
      roomClients.delete(userId);
      if (roomClients.size === 0) rooms.delete(userRoom);
    }

    const cleanupTimer = setTimeout(() => {
      disconnectGraceTimers.delete(userId);
      if (activeUsers.get(userId)?.ws !== ws) return;
      activeUsers.delete(userId);

      if (viewerWatching.has(userId)) removeViewerFromStream(userId, true);
      if (streamViewers.has(userId)) endStreamAudience(userId, 'broadcaster_offline');

      broadcastToRoom(userRoom, userId, { type: 'USER_LEFT', userId });
      broadcastToRoom(userRoom, userId, {
        type: 'USER_LIVE_STATE',
        userId,
        nick: userNick,
        isLive: false
      });
    }, DISCONNECT_GRACE_MS);

    disconnectGraceTimers.set(userId, cleanupTimer);
  });
});

const wsHeartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      try { ws.terminate(); } catch (_) {}
      continue;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) {}
  }
}, 25000);

wss.on('close', () => clearInterval(wsHeartbeat));

function consumeRtcConfigRate(req) {
  const key = String(req.ip || req.socket.remoteAddress || 'unknown');
  const now = Date.now();
  let bucket = rtcConfigRateBuckets.get(key);
  if (!bucket || now - bucket.startedAt >= 60_000) {
    bucket = { startedAt: now, count: 0 };
    rtcConfigRateBuckets.set(key, bucket);
  }
  bucket.count += 1;
  return bucket.count <= RTC_CONFIG_RATE_LIMIT;
}

setInterval(() => {
  const cutoff = Date.now() - 5 * 60_000;
  for (const [key, bucket] of rtcConfigRateBuckets) {
    if (bucket.startedAt < cutoff) rtcConfigRateBuckets.delete(key);
  }
}, 60_000).unref?.();

app.get('/health', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.json({
    ok: true,
    appVersion: APP_VERSION,
    usersOnline: Array.from(activeUsers.values()).filter((u) => u.ws.readyState === WebSocket.OPEN).length,
    activeStreams: Array.from(activeUsers.values()).filter((u) => u.isLive).length,
    activeViewers: viewerWatching.size,
    maxViewersPerStream: MAX_VIEWERS_PER_STREAM,
    forceTurnRelay: FORCE_TURN_RELAY,
    turnConfigured: Boolean(TURN_URLS.length && (TURN_SECRET || (ALLOW_STATIC_TURN_CREDENTIALS && TURN_USERNAME && TURN_CREDENTIAL))),
    turnTlsEndpointConfigured: TURN_URLS.some((url) => /^turns:/i.test(url)),
    uptimeSeconds: Math.floor(process.uptime())
  });
});

app.get('/api/version', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.json({ appVersion: APP_VERSION, serverTime: Date.now() });
});

app.get('/api/rtc-config', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, private');

  if (!consumeRtcConfigRate(req)) {
    res.setHeader('Retry-After', '60');
    res.status(429).json({ error: 'RATE_LIMITED' });
    return;
  }

  const iceServers = [...STUN_SERVERS];
  const userId = normalizeId(req.query.userId);
  let turnMode = 'none';
  let expiresAt = null;

  const hasEphemeralTurn = Boolean(TURN_URLS.length && TURN_SECRET);
  const hasStaticTurn = Boolean(
    TURN_URLS.length &&
    ALLOW_STATIC_TURN_CREDENTIALS &&
    TURN_USERNAME &&
    TURN_CREDENTIAL
  );

  if (hasEphemeralTurn || hasStaticTurn) {
    const current = userId ? activeUsers.get(userId) : null;
    const suppliedToken = String(req.headers['x-session-token'] || '');
    if (!current || !secureTokenEquals(suppliedToken, current.sessionToken)) {
      res.status(401).json({ error: 'RTC_SESSION_REQUIRED' });
      return;
    }
  }

  if (hasEphemeralTurn) {
    expiresAt = Math.floor(Date.now() / 1000) + TURN_TTL_SECONDS;
    const username = `${expiresAt}:${userId}`;
    const credential = crypto
      .createHmac('sha1', TURN_SECRET)
      .update(username)
      .digest('base64');

    iceServers.push({
      urls: TURN_URLS,
      username,
      credential
    });
    turnMode = 'ephemeral';
  } else if (hasStaticTurn) {
    iceServers.push({
      urls: TURN_URLS,
      username: TURN_USERNAME,
      credential: TURN_CREDENTIAL
    });
    turnMode = 'static';
  }

  res.json({
    appVersion: APP_VERSION,
    iceServers,
    iceTransportPolicy: 'all',
    turnConfigured: turnMode !== 'none',
    turnMode,
    expiresAt,
    expiresInSeconds: turnMode === 'ephemeral' ? TURN_TTL_SECONDS : null,
    hasTurnTls: TURN_URLS.some((url) => /^turns:/i.test(url))
  });
});

// FRONTEND COMPLETO DO DISCORD
app.get('/', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0');
  res.send(`
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Discord ScreenStream - Ultra 60FPS</title>
  <style>
    :root {
      --bg-tertiary: #1e1f22;
      --bg-secondary: #2b2d31;
      --bg-primary: #313338;
      --discord-blurple: #5865f2;
      --discord-blurple-hover: #4752c4;
      --discord-green: #23a55a;
      --discord-red: #f23f43;
      --text-normal: #dbdee1;
      --text-muted: #949ba4;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; user-select: none; }
    body { background: var(--bg-tertiary); color: var(--text-normal); height: 100vh; display: flex; overflow: hidden; }

    /* BARRA 1: SERVIDORES */
    .guild-bar { width: 72px; background: var(--bg-tertiary); display: flex; flex-direction: column; align-items: center; padding: 12px 0; border-right: 1px solid rgba(0,0,0,0.3); gap: 10px; }
    .guild-icon { width: 48px; height: 48px; background: var(--discord-blurple); border-radius: 16px; display: flex; align-items: center; justify-content: center; color: white; font-weight: bold; cursor: pointer; transition: 0.2s; }
    .guild-icon:hover { border-radius: 12px; filter: brightness(1.1); }

    /* BARRA 2: AMIGOS */
    .sidebar { width: 280px; background: var(--bg-secondary); display: flex; flex-direction: column; }
    .sidebar-header { height: 48px; padding: 0 16px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid rgba(0,0,0,0.2); font-size: 14px; font-weight: bold; color: #fff; }
    .btn-add-friend { background: var(--discord-green); color: #fff; border: none; padding: 6px 12px; border-radius: 4px; font-size: 12px; font-weight: bold; cursor: pointer; }
    .btn-add-friend:hover { filter: brightness(0.9); }

    .friends-list { flex: 1; overflow-y: auto; padding: 8px; display: flex; flex-direction: column; gap: 6px; }
    .section-title { font-size: 11px; font-weight: 700; color: var(--text-muted); text-transform: uppercase; padding: 6px 4px; letter-spacing: 0.5px; }

    .friend-item { display: flex; align-items: center; justify-content: space-between; padding: 8px 10px; border-radius: 6px; background: rgba(0,0,0,0.15); transition: 0.15s; }
    .friend-item:hover { background: rgba(255,255,255,0.06); }
    .friend-info { display: flex; align-items: center; gap: 8px; overflow: hidden; }
    .avatar { width: 34px; height: 34px; border-radius: 50%; background: var(--discord-blurple); display: flex; align-items: center; justify-content: center; font-weight: bold; color: #fff; font-size: 13px; flex-shrink: 0; position: relative; }
    .status-dot { width: 9px; height: 9px; background: var(--discord-green); border-radius: 50%; position: absolute; bottom: 0; right: 0; border: 2px solid var(--bg-secondary); }
    .friend-name { font-size: 13px; font-weight: 600; color: #fff; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .friend-id { font-size: 11px; color: var(--text-muted); }

    .friend-actions { display: flex; gap: 4px; }
    .btn-action { background: #1e1f22; border: none; color: #fff; padding: 5px 9px; border-radius: 4px; font-size: 11px; font-weight: bold; cursor: pointer; }
    .btn-action.live { background: var(--discord-red); animation: pulse 1.5s infinite; }
    .btn-action.watch { background: var(--discord-blurple); }
    .btn-action.del:hover { background: var(--discord-red); }

    @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.6; } 100% { opacity: 1; } }

    /* CARD DO USUÁRIO */
    .user-panel { margin-top: auto; height: 60px; background: #1e1f22; display: flex; align-items: center; padding: 0 12px; gap: 10px; border-top: 1px solid rgba(0,0,0,0.2); }
    .user-tag { display: flex; flex-direction: column; overflow: hidden; flex: 1; }
    .user-tag .username { font-size: 13px; font-weight: bold; color: #fff; }
    .user-tag .peer-id { font-size: 11px; color: var(--discord-green); cursor: pointer; font-weight: bold; }
    .user-tag .peer-id:hover { text-decoration: underline; }

    /* PALCO PRINCIPAL */
    .main-stage { flex: 1; background: var(--bg-primary); display: flex; flex-direction: column; position: relative; }
    .top-bar { height: 48px; padding: 0 16px; display: flex; align-items: center; justify-content: space-between; background: var(--bg-secondary); border-bottom: 1px solid rgba(0,0,0,0.2); z-index: 10; }
    .stream-info { display: flex; align-items: center; gap: 10px; font-size: 14px; font-weight: bold; color: #fff; }
    .badge-live { background: var(--discord-red); color: white; font-size: 10px; font-weight: 800; padding: 2px 6px; border-radius: 4px; display: none; }
    .badge-gpu { background: #232428; border: 1px solid var(--discord-green); color: var(--discord-green); font-size: 10px; font-weight: 700; padding: 2px 8px; border-radius: 4px; }
    .badge-audience { background: #232428; border: 1px solid #5f6df5; color: #c7ccff; font-size: 10px; font-weight: 700; padding: 2px 8px; border-radius: 4px; display: none; }

    /* VÍDEO */
    .video-viewport { flex: 1; background: #000; display: flex; align-items: center; justify-content: center; position: relative; overflow: hidden; }
    video { width: 100%; height: 100%; max-width: 100%; max-height: 100%; object-fit: contain; background: #000; }

    .empty-state { position: absolute; text-align: center; color: var(--text-muted); }
    .empty-state svg { width: 72px; height: 72px; fill: #4e5058; margin-bottom: 12px; }

    /* DOCK FLUTUANTE */
    .control-dock { position: absolute; bottom: 24px; max-width: calc(100% - 24px); display: flex; flex-wrap: wrap; justify-content: center; align-items: center; gap: 10px; background: rgba(20,20,22,0.92); padding: 10px 20px; border-radius: 30px; backdrop-filter: blur(10px); z-index: 50; }
    .btn-dock { background: #313338; color: #fff; border: none; padding: 10px 18px; border-radius: 20px; font-weight: bold; cursor: pointer; transition: 0.2s; font-size: 13px; display: flex; align-items: center; gap: 6px; }
    .btn-dock:hover { background: #3f4147; }
    .btn-dock.primary { background: var(--discord-blurple); }
    .btn-dock.primary:hover { background: var(--discord-blurple-hover); }
    .btn-dock.danger { background: var(--discord-red); }
    .btn-dock.copy { background: var(--discord-green); }
    .btn-dock:disabled { opacity: .55; cursor: wait; }
    .call-mode { display: flex; align-items: center; gap: 6px; font-size: 12px; white-space: nowrap; }
    .audio-status { position: absolute; left: 16px; top: 16px; max-width: calc(100% - 340px); padding: 8px 12px; border-radius: 8px; background: rgba(20,20,22,.9); font-size: 12px; line-height: 1.5; pointer-events: none; }
    .audio-status:empty { display: none; }
    @media (max-width: 850px) { .guild-bar { display: none; } .sidebar { width: 210px; } .top-bar { height: auto; min-height: 48px; flex-wrap: wrap; gap: 6px; padding: 8px; } .stream-info { flex-wrap: wrap; } .audio-status { top: auto; bottom: 150px; max-width: calc(100% - 32px); } }

    #unmuteNotice { position: absolute; top: 20px; background: rgba(0,0,0,0.85); border: 1px solid #f0b232; color: #f0b232; padding: 8px 18px; border-radius: 20px; font-weight: bold; font-size: 12px; cursor: pointer; display: none; z-index: 100; }

    .audience-panel { position: absolute; top: 18px; right: 18px; width: min(290px, calc(100% - 36px)); max-height: 42%; overflow-y: auto; background: rgba(20,20,22,0.90); border: 1px solid rgba(255,255,255,0.12); border-radius: 12px; padding: 10px 12px; backdrop-filter: blur(12px); z-index: 45; display: none; }
    .audience-title { display: flex; align-items: center; justify-content: space-between; gap: 10px; color: #fff; font-size: 12px; font-weight: 800; }
    .audience-subtitle { color: var(--text-muted); font-size: 10px; margin-top: 2px; }
    .audience-list { margin-top: 8px; display: flex; flex-wrap: wrap; gap: 6px; }
    .audience-chip { max-width: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; background: #313338; color: var(--text-normal); border-radius: 999px; padding: 4px 8px; font-size: 10px; }

    /* POP-UP DISCORD DE PEDIDO DE AMIZADE */
    .discord-modal { position: fixed; top: 20px; right: 20px; background: #2b2d31; border: 2px solid var(--discord-blurple); border-radius: 8px; padding: 18px; box-shadow: 0 10px 30px rgba(0,0,0,0.8); display: none; flex-direction: column; gap: 12px; z-index: 999999; width: 320px; animation: slideIn 0.3s ease; }
    @keyframes slideIn { from { transform: translateX(100%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
    .discord-modal h4 { color: #fff; font-size: 15px; }
    .discord-modal p { font-size: 13px; color: var(--text-muted); line-height: 1.4; }
    .modal-actions { display: flex; gap: 8px; justify-content: flex-end; }
    .btn-confirm { background: var(--discord-green); color: #fff; border: none; padding: 8px 16px; border-radius: 4px; font-weight: bold; cursor: pointer; }
    .btn-reject { background: var(--discord-red); color: #fff; border: none; padding: 8px 16px; border-radius: 4px; font-weight: bold; cursor: pointer; }
  </style>
</head>
<body>

  <!-- POP-UP DISCORD PEDIDO DE AMIZADE -->
  <div class="discord-modal" id="friendRequestModal">
    <h4>🔔 Pedido de Amizade!</h4>
    <p id="friendRequestText">Alguém quer ser seu amigo.</p>
    <div class="modal-actions">
      <button class="btn-reject" onclick="respondFriendRequest(false)">Recusar</button>
      <button class="btn-confirm" onclick="respondFriendRequest(true)">Aceitar</button>
    </div>
  </div>

  <!-- BARRA 1: SERVIDORES -->
  <div class="guild-bar">
    <div class="guild-icon">DC</div>
  </div>

  <!-- BARRA 2: AMIGOS -->
  <div class="sidebar">
    <div class="sidebar-header">
      <span>AMIGOS</span>
      <button class="btn-add-friend" onclick="sendFriendRequestPrompt()">+ Adicionar</button>
    </div>

    <div class="friends-list" id="friendsContainer">
      <div class="section-title">Lista de Amigos</div>
    </div>

    <!-- PAINEL DO USUÁRIO -->
    <div class="user-panel">
      <div class="avatar" id="avatarLetter">U<div class="status-dot"></div></div>
      <div class="user-tag">
        <span class="username" id="myNickDisplay">Carregando...</span>
        <span class="peer-id" id="myIdDisplay" onclick="copyMyId()" title="Clique para copiar seu ID">...</span>
      </div>
    </div>
  </div>

  <!-- PALCO PRINCIPAL -->
  <div class="main-stage">
    <div class="top-bar">
      <div class="stream-info">
        <span id="stageTitle">Nenhuma transmissão em andamento</span>
        <span class="badge-live" id="liveBadge">AO VIVO</span>
        <span class="badge-gpu" id="statusBadge">⚡ Full HD • 60 FPS • Auto</span>
        <span class="badge-audience" id="audienceBadge">👁 0/0</span>
      </div>
      <span style="font-size: 12px; color: var(--discord-green);" id="connStatusText">🟢 Conectado à Nuvem</span>
    </div>

    <div class="video-viewport">
      <button type="button" id="unmuteNotice" onclick="unmute()">🔊 Ativar áudio da transmissão</button>
      <div id="audioStatus" class="audio-status" role="status" aria-live="polite"></div>
      <video id="remoteVideo" autoplay playsinline></video>

      <div class="audience-panel" id="audiencePanel">
        <div class="audience-title">
          <span>👁 Observadores</span>
          <span id="audienceCountText">0</span>
        </div>
        <div class="audience-subtitle" id="audienceRoleText">Ninguém assistindo ainda.</div>
        <div class="audience-list" id="audienceList"></div>
      </div>
      
      <div class="empty-state" id="emptyState">
        <svg viewBox="0 0 24 24"><path d="M21 3H3c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h5v2h8v-2h5c1.1 0 1.99-.9 1.99-2L23 5c0-1.1-.9-2-2-2zm0 14H3V5h18v12z"/></svg>
        <h3 style="color: #fff;">Pronto para transmitir</h3>
        <p style="margin-top: 6px;">Em call, compartilhe a aba do conteúdo com áudio. Mantenha a call em outra aba ou aplicativo.</p>
      </div>

      <!-- DOCK FLUTUANTE -->
      <div class="control-dock">
        <label class="call-mode" title="Evita reenviar o microfone e o áudio geral do computador. Compartilhe a aba do conteúdo, separada da call."><input type="checkbox" id="callMode" checked onchange="updateAudioUi()">Já estou em uma call</label>
        <button class="btn-dock primary" id="btnShare" onclick="toggleShare()">Transmitir Tela</button>
        <button class="btn-dock copy" id="btnCopyLink" style="display: none;" onclick="copyStreamLink()">🔗 Copiar Link da Live</button>
        <button class="btn-dock" id="btnMic" style="display: none;" onclick="toggleMicrophone()" aria-pressed="false">Ativar microfone</button>
        <button class="btn-dock" id="btnAudio" style="display: none;" onclick="toggleViewerAudio()" aria-pressed="false">Ativar áudio</button>
        <button class="btn-dock danger" id="btnDisconnect" style="display: none;" onclick="disconnectStream()">Desconectar</button>
        <button class="btn-dock" onclick="toggleFullscreen()">Tela Cheia</button>
      </div>
    </div>
  </div>

  <script>
    // --- CONFIGURAÇÃO DA PÁGINA / QUALIDADE ---
    const PAGE_APP_VERSION = ${JSON.stringify(APP_VERSION)};
    const ROOM_NAME = 'sala-principal';
    const MAX_VIEWERS = ${MAX_VIEWERS_PER_STREAM};
    const STREAM_UPLOAD_BUDGET = ${STREAM_UPLOAD_BUDGET_BPS};
    const FORCE_TURN_RELAY_POLICY = ${FORCE_TURN_RELAY};
    const QUALITY = Object.freeze({
      width: 1920,
      height: 1080,
      fps: 60,
      startTargetBitrate: 12_000_000,
      safeStartBitrate: 4_000_000,
      maxBitrate: 18_000_000,
      minBitrate: 150_000,
      safeStartFps: 30,
      statsIntervalMs: 2500
    });

    // --- IDENTIDADE LOCAL ---
    let myId = localStorage.getItem('dc_user_id');
    if (!myId) {
      myId = 'dc-' + Math.floor(10000 + Math.random() * 90000);
      localStorage.setItem('dc_user_id', myId);
    }
    myId = myId.trim().toLowerCase();
    const ownStreamBroadcasterId = myId;
    // Abrir o próprio link em outra aba não deve expulsar o transmissor.
    const ownStreamPreview = new URLSearchParams(location.search).get('watch') === myId;
    if (ownStreamPreview) {
      myId = 'dc-view-' + createSessionId();
    }

    let myNick = localStorage.getItem('dc_user_nick') || 'Gamer#' + myId.slice(-4);
    let friends = [];
    try {
      friends = JSON.parse(localStorage.getItem('dc_saved_friends') || '[]');
      if (!Array.isArray(friends)) friends = [];
    } catch (_) {
      friends = [];
    }

    let liveFriendIds = new Set();
    let pendingRequestFrom = null;

    document.getElementById('myIdDisplay').innerText = myId + ' (Copiar)';
    document.getElementById('myNickDisplay').innerText = myNick;
    document.getElementById('avatarLetter').innerText = myNick.charAt(0).toUpperCase();

    const videoEl = document.getElementById('remoteVideo');
    const emptyState = document.getElementById('emptyState');
    const stageTitle = document.getElementById('stageTitle');
    const liveBadge = document.getElementById('liveBadge');
    const btnShare = document.getElementById('btnShare');
    const btnCopyLink = document.getElementById('btnCopyLink');
    const btnDisconnect = document.getElementById('btnDisconnect');
    const unmuteNotice = document.getElementById('unmuteNotice');
    const audioStatus = document.getElementById('audioStatus');
    const btnMic = document.getElementById('btnMic');
    const callMode = document.getElementById('callMode');
    const btnAudio = document.getElementById('btnAudio');
    const statusBadge = document.getElementById('statusBadge');
    const connStatusText = document.getElementById('connStatusText');
    const audienceBadge = document.getElementById('audienceBadge');
    const audiencePanel = document.getElementById('audiencePanel');
    const audienceCountText = document.getElementById('audienceCountText');
    const audienceRoleText = document.getElementById('audienceRoleText');
    const audienceList = document.getElementById('audienceList');

    // --- ESTADO DE TRANSMISSÃO ---
    let localStream = null;
    let isSharing = false;
    let startingShare = false;
    let captureStream = null;
    let microphoneStream = null;
    let audioContext = null;
    let audioMix = null;
    let systemAudioSource = null;
    let microphoneSource = null;
    let audioOperation = 0;
    let streamCallMode = true;
    let captureAudioBlocked = false;
    let viewerWantsAudio = !ownStreamPreview;
    const senderRequests = new Map();
    const senderProfileUpdates = new WeakMap();
    let currentAudience = [];
    let currentAudienceMax = MAX_VIEWERS;
    let currentAudienceBroadcasterId = null;

    // broadcaster: viewerId -> peer state
    const senderPeers = new Map();

    // viewer
    let activePC = null;
    let activeBroadcasterId = null;
    let activeSessionId = null;
    let activePCRemoteReady = false;
    let activePCQueue = [];
    let viewerLocalCandidates = [];
    let viewerSignalReady = false;
    let viewerStatsTimer = null;
    let viewerFrameTimer = null;
    let viewerRecoveryTimer = null;
    let viewerConnectTimer = null;
    let viewerRecoveryAttempts = 0;
    let viewerVideoHealthy = false;
    let desiredWatchId = null;
    let watchRequestId = null;
    let offerRevision = 0;
    const pendingViewerCandidates = new Map();
    let wsSessionToken = null;

    // --- ICE / STUN / TURN DINÂMICO ---
    let rtcConfig = {
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        { urls: 'stun:stun.cloudflare.com:3478' }
      ],
      iceTransportPolicy: 'all',
      bundlePolicy: 'max-bundle',
      rtcpMuxPolicy: 'require',
      iceCandidatePoolSize: 8
    };
    let turnConfigured = false;
    let turnTlsConfigured = false;
    let rtcConfigLoadedAt = 0;
    let rtcConfigExpiresAt = 0;
    let rtcConfigPromise = null;

    function buildPeerRtcConfig(forceRelay) {
      return {
        iceServers: rtcConfig.iceServers,
        iceTransportPolicy: forceRelay && turnConfigured ? 'relay' : 'all',
        bundlePolicy: 'max-bundle',
        rtcpMuxPolicy: 'require',
        iceCandidatePoolSize: 0
      };
    }

    async function refreshRtcConfig(force) {
      const now = Date.now();
      const age = now - rtcConfigLoadedAt;
      const credentialsStillFresh = !rtcConfigExpiresAt || now < rtcConfigExpiresAt - 60 * 1000;
      if (!force && rtcConfigLoadedAt && age < 30 * 60 * 1000 && credentialsStillFresh) return rtcConfig;
      if (rtcConfigPromise) return rtcConfigPromise;

      // As credenciais TURN só são liberadas para uma sessão WebSocket autenticada.
      if (!wsSessionToken) return rtcConfig;

      rtcConfigPromise = (async () => {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 5000);
        const sessionToken = wsSessionToken;
        try {
          const res = await fetch('/api/rtc-config?userId=' + encodeURIComponent(myId), {
            cache: 'no-store',
            signal: controller.signal,
            headers: {
              'X-Session-Token': sessionToken
            }
          });
          clearTimeout(timeout);

          if (!res.ok) throw new Error('HTTP ' + res.status);
          const data = await res.json();
          if (sessionToken !== wsSessionToken) return rtcConfig;

          if (Array.isArray(data.iceServers) && data.iceServers.length) {
            rtcConfig = {
              iceServers: data.iceServers,
              iceTransportPolicy: 'all',
              bundlePolicy: 'max-bundle',
              rtcpMuxPolicy: 'require',
              iceCandidatePoolSize: 8
            };
          }

          turnConfigured = Boolean(data.turnConfigured);
          turnTlsConfigured = Boolean(data.hasTurnTls);
          rtcConfigLoadedAt = Date.now();
          rtcConfigExpiresAt = Number(data.expiresAt || 0) * 1000;

          if (!turnConfigured) {
            console.warn('TURN não configurado: P2P/STUN funcionará, mas alguns CGNATs/firewalls podem bloquear a conexão.');
          }
        } catch (err) {
          console.warn('Falha ao carregar RTC config; usando configuração ICE já disponível:', err.message);
        } finally {
          clearTimeout(timeout);
          rtcConfigPromise = null;
        }
        return rtcConfig;
      })();

      return rtcConfigPromise;
    }

    function renderAudience() {
      const active = isSharing || Boolean(activeBroadcasterId || desiredWatchId);
      if (!active) {
        audienceBadge.style.display = 'none';
        audiencePanel.style.display = 'none';
        return;
      }

      const count = currentAudience.length;
      audienceBadge.style.display = 'inline-block';
      audienceBadge.textContent = '👁 ' + count + '/' + currentAudienceMax;
      audiencePanel.style.display = 'block';
      audienceCountText.textContent = count + '/' + currentAudienceMax;

      if (isSharing) {
        audienceRoleText.textContent = count === 0
          ? 'Aguardando observadores...'
          : count + ' pessoa(s) assistindo sua transmissão.';
      } else {
        audienceRoleText.textContent = count <= 1
          ? 'Você está assistindo esta transmissão.'
          : 'Você e mais ' + Math.max(0, count - 1) + ' pessoa(s) estão assistindo.';
      }

      audienceList.replaceChildren();
      if (count === 0) {
        const chip = document.createElement('span');
        chip.className = 'audience-chip';
        chip.textContent = 'Nenhum observador';
        audienceList.appendChild(chip);
        return;
      }

      currentAudience.forEach((viewer) => {
        const chip = document.createElement('span');
        chip.className = 'audience-chip';
        const nick = String(viewer.nick || viewer.userId || 'Observador');
        chip.textContent = viewer.userId === myId ? nick + ' (você)' : nick;
        audienceList.appendChild(chip);
      });
    }

    function applyAudienceSnapshot(data) {
      if (!data || !data.broadcasterId) return;
      const broadcasterId = String(data.broadcasterId).toLowerCase();
      const relevant = isSharing
        ? broadcasterId === myId
        : broadcasterId === activeBroadcasterId || broadcasterId === desiredWatchId;
      if (!relevant) return;

      currentAudienceBroadcasterId = broadcasterId;
      currentAudience = Array.isArray(data.viewers) ? data.viewers : [];
      currentAudienceMax = Number(data.maxViewers || MAX_VIEWERS);
      renderAudience();

      if (isSharing) {
        const audienceIds = new Set(currentAudience.map((viewer) => viewer.userId));
        for (const viewerId of senderRequests.keys()) {
          if (!audienceIds.has(viewerId)) { senderRequests.delete(viewerId); closeSenderPeer(viewerId); }
        }
        rebalanceSenderBitrates().catch(() => {});
      }
    }

    function clearAudienceUi() {
      currentAudience = [];
      currentAudienceBroadcasterId = null;
      currentAudienceMax = MAX_VIEWERS;
      renderAudience();
    }

    function createSessionId() {
      if (window.crypto && typeof window.crypto.randomUUID === 'function') {
        return window.crypto.randomUUID();
      }
      return Date.now().toString(36) + '-' + Math.random().toString(36).slice(2);
    }

    function playDiscordChime() {
      try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain);
        gain.connect(ctx.destination);
        osc.frequency.setValueAtTime(587.33, ctx.currentTime);
        osc.frequency.setValueAtTime(880, ctx.currentTime + 0.12);
        gain.gain.setValueAtTime(0.2, ctx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.35);
        osc.start();
        osc.stop(ctx.currentTime + 0.35);
      } catch (_) {}
    }

    // --- AUTO-UPDATE DA APLICAÇÃO ---
    function updateToVersion(version) {
      if (!version || version === PAGE_APP_VERSION) return;

      const key = 'dc_reloaded_for_version';
      if (sessionStorage.getItem(key) === version) {
        console.warn('Servidor está em versão diferente, mas a página já tentou recarregar para esta versão:', version);
        return;
      }

      sessionStorage.setItem(key, version);
      location.reload();
    }

    async function checkAppVersion() {
      try {
        const res = await fetch('/api/version?ts=' + Date.now(), { cache: 'no-store' });
        if (!res.ok) return;
        const data = await res.json();
        updateToVersion(data.appVersion);
      } catch (_) {}
    }

    setInterval(checkAppVersion, 60000);
    window.addEventListener('focus', checkAppVersion);

    // --- WEBSOCKET ROBUSTO COM BACKOFF + RESYNC ---
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    let ws = null;
    let reconnectTimer = null;
    let reconnectAttempt = 0;
    let allowReconnect = true;
    let lastSocketMessageAt = 0;
    let socketOpenedAt = 0;

    function setConnectionUi(state) {
      if (state === 'online') {
        connStatusText.innerText = '🟢 Conectado à Nuvem';
        connStatusText.style.color = 'var(--discord-green)';
      } else if (state === 'connecting') {
        connStatusText.innerText = '🟡 Reconectando...';
        connStatusText.style.color = '#f0b232';
      } else {
        connStatusText.innerText = '🔴 Sem conexão com a Nuvem';
        connStatusText.style.color = 'var(--discord-red)';
      }
    }

    function wsSend(payload) {
      if (!ws || ws.readyState !== WebSocket.OPEN) return false;
      if (payload.type !== 'JOIN' && payload.type !== 'PING' && !wsSessionToken) return false;
      if (ws.bufferedAmount > 512 * 1024) return false;
      try {
        ws.send(JSON.stringify(payload));
        return true;
      } catch (_) {
        return false;
      }
    }

    function scheduleReconnect() {
      if (!allowReconnect || reconnectTimer || !navigator.onLine) return;

      const base = Math.min(10000, 500 * Math.pow(2, Math.min(reconnectAttempt, 5)));
      const jitter = Math.floor(Math.random() * 350);
      const delay = base + jitter;
      reconnectAttempt += 1;

      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connectWS();
      }, delay);
    }

    function connectWS() {
      if (!allowReconnect) return;
      if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;

      setConnectionUi('connecting');
      const socket = new WebSocket(protocol + '//' + location.host);
      ws = socket;
      socketOpenedAt = Date.now();
      lastSocketMessageAt = Date.now();

      socket.onopen = () => {
        if (ws !== socket) return;
        lastSocketMessageAt = Date.now();
        reconnectAttempt = 0;
        setConnectionUi('online');

        wsSend({
          type: 'JOIN',
          userId: myId,
          nick: myNick,
          room: ROOM_NAME,
          isLive: isSharing,
          watching: desiredWatchId
        });
      };

      socket.onmessage = async (event) => {
        if (ws !== socket) return;
        lastSocketMessageAt = Date.now();

        let data;
        try {
          data = JSON.parse(event.data);
        } catch (_) {
          return;
        }

        if (data.type === 'SERVER_HELLO') {
          updateToVersion(data.appVersion);
          return;
        }

        if (data.type === 'SERVER_RESTARTING') {
          setConnectionUi('connecting');
          connStatusText.innerText = '🟡 Servidor atualizando...';
          return;
        }

        if (data.type === 'JOIN_OK') {
          wsSessionToken = String(data.sessionToken || '');
          rtcConfigLoadedAt = 0;
          rtcConfigExpiresAt = 0;
          await refreshRtcConfig(true);
          if (ws !== socket || socket.readyState !== WebSocket.OPEN) return;

          if (isSharing) {
            wsSend({ type: 'LIVE_STATE_CHANGE', isLive: true });
          }

          if (desiredWatchId && desiredWatchId !== myId && !isSharing) {
            setTimeout(() => {
              if (ws === socket && socket.readyState === WebSocket.OPEN) {
                requestWatch(desiredWatchId, true, false);
              }
            }, 250);
          }
          return;
        }

        if (data.type === 'SESSION_REPLACED') {
          allowReconnect = false;
          setConnectionUi('offline');
          alert('Esta identidade foi aberta em outra aba ou dispositivo. Esta sessão foi desconectada para evitar conflito.');
          try { socket.close(); } catch (_) {}
          return;
        }

        if (data.type === 'PONG') return;

        if (data.type === 'FRIEND_REQUEST_INCOMING') {
          playDiscordChime();
          pendingRequestFrom = { id: data.fromId, nick: data.fromNick };
          document.getElementById('friendRequestText').innerText =
            data.fromNick + ' (' + data.fromId + ') quer ser seu amigo!';
          document.getElementById('friendRequestModal').style.display = 'flex';
          return;
        }

        if (data.type === 'FRIEND_RESPONSE_RESULT') {
          if (data.accepted) {
            addFriendToLocal(data.fromId, data.fromNick);
            alert('🎉 ' + data.fromNick + ' aceitou seu pedido de amizade!');
          } else {
            alert('❌ ' + data.fromNick + ' recusou o pedido.');
          }
          return;
        }

        if (data.type === 'FRIEND_NOT_FOUND') {
          alert('❌ O ID ' + data.targetId + ' não está online no momento!');
          return;
        }

        if (data.type === 'SYNC_LIVE_USERS') {
          liveFriendIds.clear();
          (data.liveUsers || []).forEach((u) => {
            if (u && u.userId) liveFriendIds.add(String(u.userId).toLowerCase());
          });
          renderFriends();
          return;
        }

        if (data.type === 'USER_LIVE_STATE') {
          const id = String(data.userId || '').toLowerCase();
          if (data.isLive) liveFriendIds.add(id);
          else liveFriendIds.delete(id);
          renderFriends();
          return;
        }

        if (data.type === 'STREAM_AUDIENCE') {
          applyAudienceSnapshot(data);
          return;
        }

        if (data.type === 'STREAM_FULL') {
          if (desiredWatchId === String(data.targetId || '').toLowerCase()) {
            disconnectStream();
            statusBadge.innerText = '⚠️ Live cheia • limite de ' + Number(data.maxViewers || MAX_VIEWERS) + ' espectadores';
          }
          return;
        }

        if (data.type === 'STREAM_ENDED') {
          const broadcasterId = String(data.broadcasterId || '').toLowerCase();
          if (broadcasterId && (activeBroadcasterId === broadcasterId || desiredWatchId === broadcasterId)) {
            disconnectStream();
            stageTitle.innerText = 'Transmissão encerrada';
            liveBadge.style.display = 'none';
            btnDisconnect.style.display = 'none';
            unmuteNotice.style.display = 'none';
            statusBadge.innerText = '⚡ Full HD • 60 FPS • Auto';
            clearAudienceUi();
          }
          return;
        }

        if (data.type === 'STREAM_RETRY') {
          if (data.from === desiredWatchId && (!data.sessionId || data.sessionId === activeSessionId)) scheduleViewerRecovery(500);
          return;
        }

        if (data.type === 'STREAM_WAIT' || data.type === 'STREAM_NOT_FOUND') {
          if (desiredWatchId === String(data.targetId || '').toLowerCase()) {
            if (data.type === 'STREAM_WAIT' || viewerRecoveryAttempts > 0) {
              statusBadge.innerText = '🟡 Aguardando o transmissor reconectar...';
              scheduleViewerRecovery(1000);
            } else {
              statusBadge.innerText = '⚠️ Transmissor offline';
            }
          }
          return;
        }

        if (data.type === 'STOP_WATCH') {
          senderRequests.delete(data.from);
          closeSenderPeer(data.from);
          return;
        }

        if (data.type === 'REQUEST_STREAM' && isSharing && localStream) {
          await initiateStreamToViewer(data.from, Boolean(data.forceRelay), 0, String(data.codecMode || 'vp8').toLowerCase(), data.requestId);
          return;
        }

        if (data.type === 'OFFER') {
          await handleIncomingOffer(data);
          return;
        }

        if (data.type === 'ANSWER') {
          await handleSenderAnswer(data);
          return;
        }

        if (data.type === 'VIDEO_HEALTH') {
          const peer = senderPeers.get(data.from);
          if (peer && !peer.closed && data.sessionId === peer.sessionId) {
            if (data.status === 'ready') {
              if (!peer.videoReady) {
                peer.videoReady = true;
                peer.targetBitrate = Math.min(peer.targetBitrate, peer.budgetCap || perViewerBitrateBudget());
                await applyVideoSenderProfile(peer.videoSender, peer.targetBitrate, false);
              }
            } else if (data.status === 'black') {
              scheduleSenderPeerRecovery(data.from, peer, 100);
            }
          }
          return;
        }

        if (data.type === 'CANDIDATE') {
          await handleRemoteCandidate(data);
        }
      };

      socket.onerror = () => {
        if (ws === socket) setConnectionUi('connecting');
      };

      socket.onclose = () => {
        if (ws !== socket) return;
        ws = null;
        wsSessionToken = null;
        setConnectionUi('offline');

        if (allowReconnect) scheduleReconnect();
      };
    }

    connectWS();

    setInterval(() => {
      const now = Date.now();
      if (ws && ((ws.readyState === WebSocket.CONNECTING && now - socketOpenedAt > 15000) ||
          (ws.readyState === WebSocket.OPEN && now - lastSocketMessageAt > 45000))) {
        ws.close();
        return;
      }
      wsSend({ type: 'PING', t: Date.now() });
    }, 10000);

    window.addEventListener('online', () => {
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      connectWS();
      if (desiredWatchId && wsSessionToken) scheduleViewerRecovery(1000);
    });

    window.addEventListener('offline', () => {
      setConnectionUi('offline');
      if (ws) ws.close();
    });

    // --- WEBRTC: NEGOCIAÇÃO COMPATÍVEL + PERFIL DE QUALIDADE ---
      // Preferimos um codec comum e preservamos os demais como alternativas.
    function waitForIceGathering(pc, timeoutMs = 3000) {
      if (!pc || pc.iceGatheringState === 'complete') return Promise.resolve(true);

      return new Promise((resolve) => {
        let settled = false;
        const finish = (complete) => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          try { pc.removeEventListener('icegatheringstatechange', onChange); } catch (_) {}
          resolve(complete);
        };
        const onChange = () => {
          if (pc.iceGatheringState === 'complete') finish(true);
        };
        const timer = setTimeout(() => finish(pc.iceGatheringState === 'complete'), timeoutMs);
        pc.addEventListener('icegatheringstatechange', onChange);
      });
    }

    async function applyVideoSenderProfile(sender, targetBitrate, startupSafe = false) {
      if (!sender || !sender.track || sender.track.kind !== 'video') return;
      const bitrate = Math.max(QUALITY.minBitrate, Math.min(QUALITY.maxBitrate, Number(targetBitrate || QUALITY.safeStartBitrate)));
      const update = (senderProfileUpdates.get(sender) || Promise.resolve()).catch(() => {}).then(async () => {
        try {
          const params = sender.getParameters();
          if (!Array.isArray(params.encodings) || params.encodings.length === 0) params.encodings = [{}];
          params.encodings[0].maxBitrate = bitrate;
          params.encodings[0].maxFramerate = bitrate < 700_000 ? 15 : (startupSafe || bitrate < 4_000_000 ? 30 : QUALITY.fps);
          params.encodings[0].scaleResolutionDownBy = bitrate < 700_000 ? 4 : (bitrate < 2_000_000 ? 2 : (startupSafe ? 1.5 : 1));
          await sender.setParameters(params);
        } catch (err) {
          console.warn('Ajuste RTP não suportado; usando controle nativo do navegador:', err.message);
        }
      });
      senderProfileUpdates.set(sender, update);
      await update;
    }

    function preferVideoCodec(pc, sender, codecMode = 'vp8') {
      if (!pc || !sender || codecMode === 'auto') return;
      try {
        const transceiver = pc.getTransceivers().find((t) => t.sender === sender);
        if (!transceiver || typeof transceiver.setCodecPreferences !== 'function') return;
        if (!window.RTCRtpSender || typeof RTCRtpSender.getCapabilities !== 'function') return;
        const caps = RTCRtpSender.getCapabilities('video');
        if (!caps || !Array.isArray(caps.codecs)) return;
        const wanted = codecMode === 'h264' ? 'video/H264' : 'video/VP8';
        const preferred = []; const rest = [];
        for (const codec of caps.codecs) {
          if (String(codec.mimeType || '').toLowerCase() === wanted.toLowerCase()) preferred.push(codec);
          else rest.push(codec);
        }
        if (preferred.length) transceiver.setCodecPreferences([...preferred, ...rest]);
      } catch (err) {
        console.warn('Preferência de codec indisponível; mantendo negociação nativa:', err.message);
      }
    }

    function perViewerBitrateBudget() {
      const connectedOrRequested = Math.max(1, senderPeers.size, currentAudience.length);
      return Math.max(
        QUALITY.minBitrate,
        Math.min(QUALITY.maxBitrate, Math.floor(STREAM_UPLOAD_BUDGET * 0.8 / connectedOrRequested) - 96_000)
      );
    }

    async function rebalanceSenderBitrates() {
      if (!isSharing || senderPeers.size === 0) return;
      const budgetCap = perViewerBitrateBudget();
      const tasks = [];

      senderPeers.forEach((peer) => {
        if (!peer || peer.closed) return;
        peer.budgetCap = budgetCap;
        peer.targetBitrate = Math.min(peer.targetBitrate, budgetCap);
        tasks.push(applyVideoSenderProfile(peer.videoSender, peer.targetBitrate, !peer.videoReady));
      });

      await Promise.allSettled(tasks);
    }

    async function configureCaptureTrack(videoTrack) {
      if (!videoTrack) return;

      if ('contentHint' in videoTrack) {
        videoTrack.contentHint = 'detail';
      }

      try {
        await videoTrack.applyConstraints({
          width: { ideal: QUALITY.width, max: QUALITY.width },
          height: { ideal: QUALITY.height, max: QUALITY.height },
          frameRate: { ideal: QUALITY.fps, max: QUALITY.fps }
        });
      } catch (err) {
        console.warn('O navegador manteve as restrições nativas da captura:', err.message);
      }
    }

    // --- TRANSMISSÃO ---
    function updateAudioUi() {
      btnMic.style.display = isSharing && !streamCallMode ? 'inline-flex' : 'none';
      btnAudio.style.display = !isSharing && desiredWatchId && desiredWatchId !== ownStreamBroadcasterId ? 'inline-flex' : 'none';
      const micOn = Boolean(microphoneStream?.getAudioTracks().some((t) => t.readyState === 'live' && t.enabled));
      btnMic.textContent = micOn ? 'Desativar microfone' : 'Ativar microfone';
      btnMic.setAttribute('aria-pressed', String(micOn));
      btnAudio.textContent = videoEl.muted ? 'Ativar áudio' : 'Silenciar áudio';
      btnAudio.setAttribute('aria-pressed', String(!videoEl.muted));
      if (isSharing) {
        const systemOn = captureStream?.getAudioTracks().some((t) => t.readyState === 'live');
        if (streamCallMode) {
          audioStatus.textContent = (captureAudioBlocked
            ? 'Áudio geral bloqueado para evitar retorno da call. Reinicie e escolha a aba do conteúdo com áudio.'
            : (systemOn ? 'Áudio da aba ativo. Mantenha a call fora da aba compartilhada.' : 'Sem áudio compartilhado. Para enviar som, escolha a aba do conteúdo e marque “Compartilhar áudio”.')) +
            ' Sua voz continua pela call. Prévia sem som.';
        } else {
          audioStatus.textContent = (systemOn ? 'Som da tela ativo.' : 'Sem som da tela: marque “Compartilhar áudio” no seletor ao transmitir.') +
            (micOn ? ' Microfone ativo.' : ' Ative o microfone para transmitir sua voz.') + ' Prévia sem som. Se entrar em uma call, reinicie com o modo de call ligado.';
        }
      } else if (desiredWatchId === ownStreamBroadcasterId) {
        audioStatus.textContent = 'Prévia da sua transmissão sem som para evitar retorno.';
      } else if (!desiredWatchId) audioStatus.textContent = '';
    }

    async function toggleMicrophone() {
      if (!isSharing || streamCallMode || btnMic.disabled) return;
      if (microphoneStream) {
        microphoneSource?.disconnect();
        microphoneSource = null;
        microphoneStream.getTracks().forEach((track) => track.stop());
        microphoneStream = null;
        updateAudioUi();
        return;
      }
      const operation = ++audioOperation;
      btnMic.disabled = true;
      try {
        await audioContext.resume();
        const stream = await navigator.mediaDevices.getUserMedia({
          video: false,
          audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true }
        });
        if (!isSharing || operation !== audioOperation) {
          stream.getTracks().forEach((track) => track.stop());
          return;
        }
        microphoneStream = stream;
        microphoneSource = audioContext.createMediaStreamSource(stream);
        microphoneSource.connect(audioMix);
        stream.getAudioTracks()[0].onended = () => {
          if (microphoneStream !== stream) return;
          microphoneSource?.disconnect();
          microphoneSource = null;
          microphoneStream = null;
          updateAudioUi();
        };
        updateAudioUi();
      } catch (err) {
        if (operation === audioOperation && isSharing) audioStatus.textContent = 'Microfone indisponível. Permita o acesso ao microfone no navegador e tente novamente.';
      } finally {
        if (operation === audioOperation) btnMic.disabled = false;
      }
    }

    function releaseCapture() {
      audioOperation += 1;
      microphoneSource?.disconnect();
      systemAudioSource?.disconnect();
      microphoneSource = null;
      systemAudioSource = null;
      for (const stream of [localStream, captureStream, microphoneStream]) stream?.getTracks().forEach((track) => track.stop());
      localStream = captureStream = microphoneStream = null;
      if (audioContext) audioContext.close().catch(() => {});
      audioContext = audioMix = null;
      captureAudioBlocked = false;
      btnMic.disabled = false;
    }

    async function toggleShare() {
      if (isSharing) {
        stopShare();
        return;
      }
      if (startingShare) return;
      startingShare = true;
      btnShare.disabled = true;
      callMode.disabled = true;
      streamCallMode = callMode.checked;

      try {
        // A captura precisa ser solicitada no clique, antes de qualquer espera de rede.
        audioContext = new (window.AudioContext || window.webkitAudioContext)();
        audioContext.resume().catch(() => {});
        captureStream = await navigator.mediaDevices.getDisplayMedia({
          video: {
            displaySurface: streamCallMode ? 'browser' : 'monitor',
            frameRate: { ideal: QUALITY.fps, max: QUALITY.fps },
            width: { ideal: QUALITY.width, max: QUALITY.width },
            height: { ideal: QUALITY.height, max: QUALITY.height }
          },
          audio: { suppressLocalAudioPlayback: false, restrictOwnAudio: true },
          systemAudio: streamCallMode ? 'exclude' : 'include',
          windowAudio: streamCallMode ? 'exclude' : 'system',
          selfBrowserSurface: 'exclude',
          surfaceSwitching: 'exclude'
        });

        if (activeBroadcasterId || desiredWatchId) disconnectStream();
        // Alguns navegadores ignoram as opções: confira a origem antes de ligar
        // o áudio ao mix para não devolver o som do sistema à call.
        if (streamCallMode && captureStream.getVideoTracks()[0]?.getSettings().displaySurface !== 'browser') {
          for (const track of captureStream.getAudioTracks()) {
            captureAudioBlocked = true;
            track.stop();
            captureStream.removeTrack(track);
          }
        }
        audioMix = audioContext.createMediaStreamDestination();
        if (captureStream.getAudioTracks().length) {
          systemAudioSource = audioContext.createMediaStreamSource(new MediaStream(captureStream.getAudioTracks()));
          systemAudioSource.connect(audioMix);
          captureStream.getAudioTracks().forEach((track) => { track.onended = updateAudioUi; });
        }
        // Um único canal de áudio estável permite ligar o microfone sem renegociar os espectadores.
        localStream = new MediaStream([...captureStream.getVideoTracks(), ...audioMix.stream.getAudioTracks()]);

        const videoTrack = localStream.getVideoTracks()[0];
        const audioTrack = localStream.getAudioTracks()[0];

        await configureCaptureTrack(videoTrack);

        if (audioTrack && 'contentHint' in audioTrack) {
          audioTrack.contentHint = 'music';
        }

        isSharing = true;
        currentAudience = [];
        currentAudienceBroadcasterId = myId;
        currentAudienceMax = MAX_VIEWERS;
        renderAudience();
        btnShare.innerText = 'Parar Transmissão';
        btnShare.classList.add('danger');
        btnCopyLink.style.display = 'inline-flex';
        stageTitle.innerText = 'Você está transmitindo sua tela';
        liveBadge.style.display = 'inline-block';
        emptyState.style.display = 'none';

        // A prévia recebe só vídeo; o áudio é enviado exclusivamente aos observadores.
        videoEl.srcObject = new MediaStream(localStream.getVideoTracks());
        videoEl.muted = true;
        videoEl.play().catch(() => {});

        const settings = videoTrack ? videoTrack.getSettings() : {};
        const width = settings.width || QUALITY.width;
        const height = settings.height || QUALITY.height;
        const fps = Math.round(settings.frameRate || QUALITY.fps);
        statusBadge.innerText =
          '⚡ ' + width + '×' + height + ' • ' + fps + ' FPS • ' +
          (turnConfigured ? 'TURN pronto' : 'P2P/STUN');

        if (videoTrack) videoTrack.onended = () => stopShare();
        unmuteNotice.style.display = 'none';
        updateAudioUi();

        wsSend({ type: 'LIVE_STATE_CHANGE', isLive: true });
        refreshRtcConfig(false).catch(console.warn);
      } catch (err) {
        releaseCapture();
        console.error('Erro ao capturar tela:', err);
        statusBadge.innerText = '⚠️ Captura cancelada ou indisponível';
      } finally {
        startingShare = false;
        btnShare.disabled = false;
        callMode.disabled = isSharing;
      }
    }

    function stopShare() {
      if (!isSharing && !localStream) return;
      isSharing = false;
      senderRequests.clear();

      for (const viewerId of Array.from(senderPeers.keys())) {
        closeSenderPeer(viewerId);
      }

      releaseCapture();
      callMode.disabled = false;

      isSharing = false;
      btnShare.innerText = 'Transmitir Tela';
      btnShare.classList.remove('danger');
      btnCopyLink.style.display = 'none';
      stageTitle.innerText = 'Nenhuma transmissão em andamento';
      liveBadge.style.display = 'none';
      videoEl.srcObject = null;
      emptyState.style.display = 'block';
      statusBadge.innerText = '⚡ Full HD • 60 FPS • Auto';
      clearAudienceUi();
      updateAudioUi();

      wsSend({ type: 'LIVE_STATE_CHANGE', isLive: false });
    }

    async function initiateStreamToViewer(viewerId, forceRelay = false, recoveryAttempts = 0, codecMode = 'vp8', requestId) {
      if (!isSharing || !localStream || !viewerId || !requestId) return;
      if (senderRequests.get(viewerId) === requestId) return;
      senderRequests.set(viewerId, requestId);

      await refreshRtcConfig(false);
      if (!isSharing || !localStream || senderRequests.get(viewerId) !== requestId) return;
      closeSenderPeer(viewerId);

      forceRelay = Boolean((forceRelay || FORCE_TURN_RELAY_POLICY) && turnConfigured);
      const pc = new RTCPeerConnection(buildPeerRtcConfig(forceRelay));
      const sessionId = createSessionId();

      const peer = {
        pc,
        sessionId,
        requestId,
        remoteReady: false,
        remoteCandidates: [],
        localCandidates: [],
        signalReady: false,
        videoSender: null,
        videoTrackClone: null,
        mediaStreamClone: null,
        codecMode: ['vp8', 'h264', 'auto'].includes(codecMode) ? codecMode : 'vp8',
        videoReady: false,
        startupSamples: 0,
        statsTimer: null,
        reconnectTimer: null,
        connectTimer: null,
        forceRelay,
        recoveryAttempts,
        budgetCap: perViewerBitrateBudget(),
        targetBitrate: perViewerBitrateBudget(),
        weakSamples: 0,
        strongSamples: 0,
        lastBytesSent: null,
        lastStatsAt: null,
        currentMbps: null,
        closed: false
      };

      senderPeers.set(viewerId, peer);

      try {
        const sourceVideoTrack = localStream.getVideoTracks()[0];
        const audioTracks = localStream.getAudioTracks();

        if (sourceVideoTrack) {
          peer.videoTrackClone = sourceVideoTrack.clone();
          if ('contentHint' in peer.videoTrackClone) peer.videoTrackClone.contentHint = 'detail';
          peer.mediaStreamClone = new MediaStream([peer.videoTrackClone, ...audioTracks]);
          peer.videoSender = pc.addTrack(peer.videoTrackClone, peer.mediaStreamClone);
          preferVideoCodec(pc, peer.videoSender, peer.codecMode);
        }

        audioTracks.forEach((track) => pc.addTrack(track, peer.mediaStreamClone || localStream));

        pc.onicecandidate = (event) => {
          if (!event.candidate || peer.closed) return;

          const packet = {
            type: 'CANDIDATE',
            target: viewerId,
            sessionId,
            candidate: event.candidate
          };

          if (!peer.signalReady) peer.localCandidates.push(packet);
          else wsSend(packet);
        };

        const markSenderConnected = () => {
          if (peer.closed) return;
          peer.recoveryAttempts = 0;
          if (peer.reconnectTimer) {
            clearTimeout(peer.reconnectTimer);
            peer.reconnectTimer = null;
          }
          if (peer.connectTimer) {
            clearTimeout(peer.connectTimer);
            peer.connectTimer = null;
          }
        };

        pc.onconnectionstatechange = () => {
          if (peer.closed) return;
          const state = pc.connectionState;
          if (state === 'connected') markSenderConnected();
          else if (state === 'failed') scheduleSenderPeerRecovery(viewerId, peer, 300);
          else if (state === 'disconnected') scheduleSenderPeerRecovery(viewerId, peer, 1800);
        };

        pc.oniceconnectionstatechange = () => {
          if (peer.closed) return;
          const state = pc.iceConnectionState;
          if (state === 'connected' || state === 'completed') markSenderConnected();
          else if (state === 'failed') scheduleSenderPeerRecovery(viewerId, peer, 300);
        };

        pc.onicecandidateerror = (event) => {
          if (peer.closed) return;
          console.warn('ICE sender error:', event.errorCode || '', event.errorText || '');
        };

        peer.targetBitrate = Math.min(QUALITY.safeStartBitrate, peer.budgetCap);
        await rebalanceSenderBitrates();
        await applyVideoSenderProfile(peer.videoSender, peer.targetBitrate, true);
        if (peer.closed) return;

        const offer = await pc.createOffer();
        if (peer.closed) return;
        await pc.setLocalDescription(offer);

        // Dá uma pequena janela para colocar candidatos ICE diretamente no SDP.
        // Isso torna a conexão mais robusta quando o trickle ICE sofre atraso.
        const gatheredCompletely = await waitForIceGathering(pc, peer.forceRelay ? 4500 : 2800);

        if (peer.closed || senderPeers.get(viewerId) !== peer) return;

        wsSend({
          type: 'OFFER',
          target: viewerId,
          sessionId,
          requestId,
          sdp: pc.localDescription,
          quality: {
            width: QUALITY.width,
            height: QUALITY.height,
            fps: QUALITY.fps,
            maxBitrate: peer.targetBitrate
          },
          forceRelay: peer.forceRelay,
          codecMode: peer.codecMode
        });

        peer.signalReady = true;
        if (gatheredCompletely) {
          // Os candidatos já estão no SDP; evita mandar duplicados.
          peer.localCandidates.length = 0;
        } else {
          while (peer.localCandidates.length) wsSend(peer.localCandidates.shift());
        }

        // connectionState pode permanecer em 'connecting' por bastante tempo em
        // NAT/firewall ruim. Não deixamos a tentativa travar indefinidamente.
        peer.connectTimer = setTimeout(() => {
          if (peer.closed || senderPeers.get(viewerId) !== peer) return;
          if (pc.connectionState === 'connected' || ['connected', 'completed'].includes(pc.iceConnectionState)) return;
          scheduleSenderPeerRecovery(viewerId, peer, 0);
        }, peer.forceRelay ? 20000 : 15000);
      } catch (err) {
        if (!peer.closed) {
          console.warn('Falha ao preparar transmissão:', err.message);
          scheduleSenderPeerRecovery(viewerId, peer, 1000);
        }
      }
    }

    function scheduleSenderPeerRecovery(viewerId, peer, delay) {
      if (peer.closed || peer.reconnectTimer || !isSharing) return;

      peer.reconnectTimer = setTimeout(() => {
        peer.reconnectTimer = null;
        if (peer.closed || senderPeers.get(viewerId) !== peer || !isSharing) return;
        // O observador coordena a recuperação; duas ofertas simultâneas derrubavam a sessão.
        wsSend({ type: 'STREAM_RETRY', target: viewerId, sessionId: peer.sessionId });
      }, Math.max(1000, delay));
    }

    async function handleSenderAnswer(data) {
      const peer = senderPeers.get(data.from);
      if (!peer || peer.closed) return;
      if (data.sessionId !== peer.sessionId) return;
      if (peer.remoteReady || peer.applyingAnswer) return;
      peer.applyingAnswer = true;

      try {
        await peer.pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
        if (peer.closed) return;
        peer.remoteReady = true;

        while (peer.remoteCandidates.length) {
          const candidate = peer.remoteCandidates.shift();
          await peer.pc.addIceCandidate(candidate).catch(console.warn);
        }

        await applyVideoSenderProfile(peer.videoSender, peer.targetBitrate, true);
        if (peer.closed) return;
        startSenderQualityManager(data.from, peer);
      } catch (err) {
        console.warn('Erro ao aplicar ANSWER:', err.message);
        scheduleSenderPeerRecovery(data.from, peer, 700);
      } finally {
        peer.applyingAnswer = false;
      }
    }

    function closeSenderPeer(viewerId) {
      const peer = senderPeers.get(viewerId);
      if (!peer) return;

      peer.closed = true;
      if (peer.statsTimer) clearInterval(peer.statsTimer);
      if (peer.reconnectTimer) clearTimeout(peer.reconnectTimer);
      if (peer.connectTimer) clearTimeout(peer.connectTimer);

      try { if (peer.videoTrackClone) peer.videoTrackClone.stop(); } catch (_) {}

      try {
        peer.pc.onicecandidate = null;
        peer.pc.onicecandidateerror = null;
        peer.pc.oniceconnectionstatechange = null;
        peer.pc.onconnectionstatechange = null;
        peer.pc.close();
      } catch (_) {}

      senderPeers.delete(viewerId);
      rebalanceSenderBitrates().catch(() => {});

      if (isSharing && senderPeers.size > 0) statusBadge.innerText = 'Transmitindo • ' + senderPeers.size + ' espectador(es)';

      if (isSharing && senderPeers.size === 0 && localStream) {
        const track = localStream.getVideoTracks()[0];
        const settings = track ? track.getSettings() : {};
        statusBadge.innerText =
          '⚡ ' + (settings.width || QUALITY.width) + '×' + (settings.height || QUALITY.height) +
          ' • ' + Math.round(settings.frameRate || QUALITY.fps) + ' FPS • Aguardando espectador';
      }
    }

    async function startSenderQualityManager(viewerId, peer) {
      if (peer.statsTimer) clearInterval(peer.statsTimer);

      peer.statsTimer = setInterval(async () => {
        if (peer.statsBusy || peer.closed || peer.pc.connectionState !== 'connected') return;
        peer.statsBusy = true;

        try {
          const report = await peer.pc.getStats();
          if (peer.closed) return;
          let outbound = null;
          let pair = null;

          report.forEach((stat) => {
            if (stat.type === 'outbound-rtp' && stat.kind === 'video' && !stat.isRemote) outbound = stat;
            if (stat.type === 'candidate-pair' && stat.state === 'succeeded' && stat.nominated) pair = stat;
          });

          if (!outbound) return;

          const now = performance.now();
          let currentMbps = null;

          if (peer.lastBytesSent !== null && peer.lastStatsAt !== null) {
            const seconds = Math.max(0.001, (now - peer.lastStatsAt) / 1000);
            currentMbps = ((outbound.bytesSent - peer.lastBytesSent) * 8 / seconds) / 1_000_000;
          }

          peer.lastBytesSent = outbound.bytesSent;
          peer.lastStatsAt = now;

          peer.currentMbps = currentMbps;
          peer.budgetCap = perViewerBitrateBudget();

          const framesEncoded = Number(outbound.framesEncoded || 0);
          if (framesEncoded > 0) peer.startupSamples += 1;
          if (!peer.videoReady) {
            peer.targetBitrate = Math.min(QUALITY.safeStartBitrate, peer.budgetCap);
            return;
          }

          const available = Number(pair && pair.availableOutgoingBitrate || 0);
          if (available > 0) {
            if (available < peer.targetBitrate * 0.65) {
              peer.weakSamples += 1;
              peer.strongSamples = 0;
            } else if (available > peer.targetBitrate * 1.35) {
              peer.strongSamples += 1;
              peer.weakSamples = 0;
            } else {
              peer.weakSamples = 0;
              peer.strongSamples = 0;
            }

            if (peer.weakSamples >= 3) {
              peer.targetBitrate = Math.max(
                QUALITY.minBitrate,
                Math.min(peer.budgetCap, Math.floor(available * 0.90))
              );
              peer.weakSamples = 0;
              await applyVideoSenderProfile(peer.videoSender, peer.targetBitrate);
            } else if (peer.strongSamples >= 3 && peer.targetBitrate < peer.budgetCap) {
              peer.targetBitrate = Math.min(
                peer.budgetCap,
                Math.max(peer.targetBitrate + 2_000_000, Math.floor(available * 0.80))
              );
              peer.strongSamples = 0;
              await applyVideoSenderProfile(peer.videoSender, peer.targetBitrate);
            }
          }

          if (isSharing) {
            const fps = Math.round(outbound.framesPerSecond || QUALITY.fps);
            const width = outbound.frameWidth || QUALITY.width;
            const height = outbound.frameHeight || QUALITY.height;
            let totalMbps = 0;
            let measuredPeers = 0;
            let relayPeers = 0;
            senderPeers.forEach((p) => {
              if (Number.isFinite(p.currentMbps)) {
                totalMbps += p.currentMbps;
                measuredPeers += 1;
              }
              if (p.forceRelay) relayPeers += 1;
            });
            const mbpsText = measuredPeers === 0 ? 'iniciando' : totalMbps.toFixed(1) + ' Mbps upload';
            const routeText = relayPeers > 0 ? ' • ' + relayPeers + ' via TURN' : '';

            statusBadge.innerText =
              '⚡ ' + width + '×' + height + ' • ' + fps + ' FPS • ' +
              mbpsText + ' • ' + senderPeers.size + ' espectador(es)' + routeText;
          }
        } catch (_) {} finally { peer.statsBusy = false; }
      }, QUALITY.statsIntervalMs);
    }

    // --- RECEBENDO A TRANSMISSÃO ---
    async function handleIncomingOffer(data) {
      if (!data.from || !data.sessionId || !data.sdp) return;
      if (isSharing || data.from !== desiredWatchId || data.requestId !== watchRequestId) return;
      if (data.sessionId === activeSessionId) return;
      const revision = ++offerRevision;

      await refreshRtcConfig(false);
      if (revision !== offerRevision || isSharing || data.from !== desiredWatchId || data.requestId !== watchRequestId) return;
      const earlyCandidates = pendingViewerCandidates.get(data.sessionId) || [];
      closeViewerConnection(false);

      activeBroadcasterId = data.from;
      activeSessionId = data.sessionId;
      desiredWatchId = data.from;
      activePCRemoteReady = false;
      activePCQueue = earlyCandidates;
      viewerLocalCandidates = [];
      viewerSignalReady = false;
      viewerVideoHealthy = false;

      const useRelayOnly = Boolean((data.forceRelay || FORCE_TURN_RELAY_POLICY) && turnConfigured);
      const pc = new RTCPeerConnection(buildPeerRtcConfig(useRelayOnly));
      activePC = pc;
      currentAudienceBroadcasterId = data.from;
      renderAudience();

      const remoteMediaStream = new MediaStream();
      videoEl.autoplay = true;
      videoEl.playsInline = true;
      videoEl.muted = !viewerWantsAudio;
      videoEl.srcObject = remoteMediaStream;

      const ensureRemotePlayback = () => {
        if (activePC !== pc) return;
        playViewerMedia(pc);
      };

      pc.ontrack = (event) => {
        if (activePC !== pc) return;

        if (!remoteMediaStream.getTracks().some((t) => t.id === event.track.id)) {
          remoteMediaStream.addTrack(event.track);
          if (videoEl.srcObject !== remoteMediaStream) videoEl.srcObject = remoteMediaStream;
        }

        ensureRemotePlayback();
        event.track.onunmute = ensureRemotePlayback;
        videoEl.onloadedmetadata = ensureRemotePlayback;
        videoEl.onloadeddata = ensureRemotePlayback;

        emptyState.style.display = 'none';
        stageTitle.innerText = 'Assistindo tela de ' + (data.fromNick || data.from);
        liveBadge.style.display = 'inline-block';
        btnDisconnect.style.display = 'flex';

        updateAudioUi();
      };

      pc.onicecandidate = (event) => {
        if (!event.candidate || activePC !== pc) return;

        const packet = {
          type: 'CANDIDATE',
          target: data.from,
          sessionId: data.sessionId,
          candidate: event.candidate
        };

        if (!viewerSignalReady) viewerLocalCandidates.push(packet);
        else wsSend(packet);
      };

      const markViewerConnected = () => {
        if (activePC !== pc) return;
        if (viewerRecoveryTimer) {
          clearTimeout(viewerRecoveryTimer);
          viewerRecoveryTimer = null;
        }
        if (viewerConnectTimer) {
          clearTimeout(viewerConnectTimer);
          viewerConnectTimer = null;
        }
        statusBadge.innerText = '⚡ Conectado • aguardando primeiro quadro...';
        startViewerStats(pc);
        startViewerFrameWatchdog(pc, data.from);
      };

      pc.onconnectionstatechange = () => {
        if (activePC !== pc) return;
        const state = pc.connectionState;
        if (state === 'connected') markViewerConnected();
        else if (state === 'connecting') statusBadge.innerText = '🔄 Conectando WebRTC...';
        else if (state === 'failed') {
          statusBadge.innerText = '⚠️ Rota falhou • tentando outra...';
          scheduleViewerRecovery(250);
        } else if (state === 'disconnected') {
          statusBadge.innerText = '🟡 Rede instável • tentando recuperar...';
          scheduleViewerRecovery(5000);
        }
      };

      pc.oniceconnectionstatechange = () => {
        if (activePC !== pc) return;
        const state = pc.iceConnectionState;
        if (state === 'connected' || state === 'completed') markViewerConnected();
        else if (state === 'checking') statusBadge.innerText = '🔄 Testando rota P2P/TURN...';
        else if (state === 'failed') {
          statusBadge.innerText = '⚠️ ICE falhou • trocando rota...';
          scheduleViewerRecovery(250);
        }
      };

      pc.onicecandidateerror = (event) => {
        if (activePC !== pc) return;
        console.warn('ICE viewer error:', event.errorCode || '', event.errorText || '');
      };

      try {
        await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
        if (activePC !== pc) return;
        activePCRemoteReady = true;

        while (activePCQueue.length) {
          const candidate = activePCQueue.shift();
          await pc.addIceCandidate(candidate).catch(console.warn);
          if (activePC !== pc) return;
        }

        const answer = await pc.createAnswer();
        if (activePC !== pc) return;
        await pc.setLocalDescription(answer);
        const gatheredCompletely = await waitForIceGathering(pc, useRelayOnly ? 4500 : 2800);

        if (activePC !== pc) return;

        wsSend({
          type: 'ANSWER',
          target: data.from,
          sessionId: data.sessionId,
          sdp: pc.localDescription
        });

        viewerSignalReady = true;
        if (gatheredCompletely) viewerLocalCandidates.length = 0;
        else while (viewerLocalCandidates.length) wsSend(viewerLocalCandidates.shift());

        if (viewerConnectTimer) clearTimeout(viewerConnectTimer);
        viewerConnectTimer = setTimeout(() => {
          if (activePC !== pc) return;
          const connected = pc.connectionState === 'connected' || ['connected', 'completed'].includes(pc.iceConnectionState);
          if (connected) return;

          if (!turnConfigured) {
            statusBadge.innerText = '⚠️ P2P bloqueado nesta rede • configure TURN para acesso global';
          } else {
            statusBadge.innerText = useRelayOnly
              ? '⚠️ TURN não conectou • tentando novamente...'
              : '🔄 P2P bloqueado • tentando TURN...';
          }
          scheduleViewerRecovery(0);
        }, useRelayOnly ? 15000 : 10000);
      } catch (err) {
        if (activePC !== pc) return;
        console.warn('Erro ao receber OFFER:', err.message);
        scheduleViewerRecovery(700);
      }
    }

    async function handleRemoteCandidate(data) {
      if (!data.candidate || !data.sessionId) return;

      // Candidato do transmissor -> espectador.
      if (
        activePC &&
        data.from === activeBroadcasterId &&
        data.sessionId === activeSessionId
      ) {
        if (!activePCRemoteReady) {
          activePCQueue.push(data.candidate);
        } else {
          await activePC.addIceCandidate(data.candidate).catch(console.warn);
        }
        return;
      }

      if (data.from === desiredWatchId && !isSharing) {
        if (!pendingViewerCandidates.has(data.sessionId)) {
          if (pendingViewerCandidates.size >= 4) pendingViewerCandidates.delete(pendingViewerCandidates.keys().next().value);
          pendingViewerCandidates.set(data.sessionId, []);
        }
        const queue = pendingViewerCandidates.get(data.sessionId);
        if (queue.length < 128) queue.push(data.candidate);
        return;
      }

      // Candidato do espectador -> transmissor.
      const peer = senderPeers.get(data.from);
      if (!peer || peer.closed || data.sessionId !== peer.sessionId) return;

      if (!peer.remoteReady) {
        peer.remoteCandidates.push(data.candidate);
      } else {
        await peer.pc.addIceCandidate(data.candidate).catch(console.warn);
      }
    }

    function scheduleViewerRecovery(delay) {
      if (!desiredWatchId || isSharing || viewerRecoveryTimer) return;
      if (!navigator.onLine || !wsSessionToken || !ws || ws.readyState !== WebSocket.OPEN) return;

      viewerRecoveryAttempts += 1;
      const backoff = Math.min(15000, Math.max(delay, 750 * Math.pow(2, Math.min(viewerRecoveryAttempts - 1, 4))));

      viewerRecoveryTimer = setTimeout(() => {
        viewerRecoveryTimer = null;
        const target = desiredWatchId;
        if (!navigator.onLine || !wsSessionToken) return;
        const useRelay = Boolean(turnConfigured && viewerRecoveryAttempts >= 2);
        const codecMode = viewerRecoveryAttempts % 2 ? 'h264' : 'vp8';
        closeViewerConnection(false);
        if (target) requestWatch(target, true, useRelay, codecMode);
      }, backoff);
    }

    function startViewerFrameWatchdog(pc, broadcasterId) {
      if (viewerFrameTimer) clearInterval(viewerFrameTimer);
      const startedAt = Date.now();
      let lastFrames = 0;
      let lastFrameAt = startedAt;
      let bytesAtLastFrame = 0;
      let busy = false;
      viewerFrameTimer = setInterval(async () => {
        if (busy || activePC !== pc || pc.connectionState !== 'connected') return;
        busy = true;
        try {
          const report = await pc.getStats();
          if (activePC !== pc) return;
          let inbound = null;
          report.forEach((stat) => { if (stat.type === 'inbound-rtp' && stat.kind === 'video' && !stat.isRemote) inbound = stat; });
          if (!inbound) {
            if (Date.now() - startedAt > 7000) { statusBadge.innerText = '⚠️ Conectou, mas nenhum vídeo chegou • renegociando...'; scheduleViewerRecovery(150); }
            return;
          }
          const received = Number(inbound.framesReceived || 0);
          const decoded = Number(inbound.framesDecoded || 0);
          const bytes = Number(inbound.bytesReceived || 0);
          const elementHasFrame = videoEl.videoWidth > 0 && videoEl.videoHeight > 0 && videoEl.readyState >= 2;
          const frames = inbound.framesDecoded == null && elementHasFrame ? received : decoded;
          if (frames > lastFrames) {
            lastFrames = frames;
            lastFrameAt = Date.now();
            bytesAtLastFrame = bytes;
            if (!viewerVideoHealthy) {
              viewerVideoHealthy = true;
              wsSend({ type: 'VIDEO_HEALTH', target: broadcasterId, sessionId: activeSessionId, status: 'ready', framesReceived: received, framesDecoded: decoded });
            }
            if (Date.now() - startedAt > 15000) viewerRecoveryAttempts = 0;
          }
          // Uma tela parada pode deixar de gerar quadros. Só classifica travamento
          // após o primeiro quadro se novos dados chegaram sem decodificação.
          if ((!viewerVideoHealthy && Date.now() - startedAt > 10000) ||
              (Date.now() - lastFrameAt > 12000 && bytes - bytesAtLastFrame > 32000)) {
            wsSend({ type: 'VIDEO_HEALTH', target: broadcasterId, sessionId: activeSessionId, status: 'black', framesReceived: received, framesDecoded: decoded });
            statusBadge.innerText = '⚠️ Vídeo chegou sem decodificar • trocando codec...';
            scheduleViewerRecovery(150); clearInterval(viewerFrameTimer); viewerFrameTimer = null;
          }
        } catch (_) {} finally { busy = false; }
      }, 1000);
    }

    function startViewerStats(pc) {
      if (viewerStatsTimer) clearInterval(viewerStatsTimer);

      let lastBytes = null;
      let lastAt = null;

      viewerStatsTimer = setInterval(async () => {
        if (activePC !== pc || pc.connectionState !== 'connected') return;

        try {
          const report = await pc.getStats();
          if (activePC !== pc) return;
          let inbound = null;
          let pair = null;
          let localCandidate = null;
          let remoteCandidate = null;

          report.forEach((stat) => {
            if (stat.type === 'inbound-rtp' && stat.kind === 'video' && !stat.isRemote) inbound = stat;
            if (stat.type === 'candidate-pair' && stat.state === 'succeeded' && stat.nominated) pair = stat;
          });

          if (pair) {
            localCandidate = report.get(pair.localCandidateId);
            remoteCandidate = report.get(pair.remoteCandidateId);
          }

          if (!inbound) return;

          const decodedFrames = Number(inbound.framesDecoded || 0);
          if (!viewerVideoHealthy && decodedFrames > 0) {
            viewerVideoHealthy = true;
            wsSend({ type: 'VIDEO_HEALTH', target: activeBroadcasterId, sessionId: activeSessionId, status: 'ready', framesReceived: Number(inbound.framesReceived || 0), framesDecoded: decodedFrames });
          }

          const now = performance.now();
          let mbps = null;

          if (lastBytes !== null && lastAt !== null) {
            const seconds = Math.max(0.001, (now - lastAt) / 1000);
            mbps = ((inbound.bytesReceived - lastBytes) * 8 / seconds) / 1_000_000;
          }

          lastBytes = inbound.bytesReceived;
          lastAt = now;

          const width = inbound.frameWidth || QUALITY.width;
          const height = inbound.frameHeight || QUALITY.height;
          const fps = Math.round(inbound.framesPerSecond || 0);
          const relay =
            (localCandidate && localCandidate.candidateType === 'relay') ||
            (remoteCandidate && remoteCandidate.candidateType === 'relay');

          statusBadge.innerText =
            '⚡ ' + width + '×' + height +
            ' • ' + (fps || '—') + ' FPS' +
            (mbps === null ? '' : ' • ' + mbps.toFixed(1) + ' Mbps') +
            ' • ' + (relay ? 'TURN global' : 'P2P direto');
        } catch (_) {}
      }, QUALITY.statsIntervalMs);
    }

    function closeViewerConnection(sendStop) {
      const oldBroadcaster = activeBroadcasterId || desiredWatchId;
      offerRevision += 1;
      pendingViewerCandidates.clear();

      if (viewerRecoveryTimer) {
        clearTimeout(viewerRecoveryTimer);
        viewerRecoveryTimer = null;
      }
      if (viewerStatsTimer) {
        clearInterval(viewerStatsTimer);
        viewerStatsTimer = null;
      }
      if (viewerFrameTimer) {
        clearInterval(viewerFrameTimer);
        viewerFrameTimer = null;
      }
      viewerVideoHealthy = false;
      if (viewerConnectTimer) {
        clearTimeout(viewerConnectTimer);
        viewerConnectTimer = null;
      }

      if (sendStop && oldBroadcaster) {
        wsSend({ type: 'STOP_WATCH', target: oldBroadcaster });
      }

      if (activePC) {
        try {
          activePC.ontrack = null;
          activePC.onicecandidate = null;
          activePC.onicecandidateerror = null;
          activePC.oniceconnectionstatechange = null;
          activePC.onconnectionstatechange = null;
          activePC.close();
        } catch (_) {}
      }

      activePC = null;
      activeBroadcasterId = null;
      activeSessionId = null;
      activePCRemoteReady = false;
      activePCQueue = [];
      viewerLocalCandidates = [];
      viewerSignalReady = false;
      videoEl.onloadedmetadata = null;
      videoEl.onloadeddata = null;
    }

    function disconnectStream() {
      closeViewerConnection(true);
      desiredWatchId = null;
      watchRequestId = null;
      viewerRecoveryAttempts = 0;

      videoEl.srcObject = null;
      emptyState.style.display = 'block';
      stageTitle.innerText = 'Nenhuma transmissão em andamento';
      liveBadge.style.display = 'none';
      btnDisconnect.style.display = 'none';
      unmuteNotice.style.display = 'none';
      statusBadge.innerText = '⚡ Full HD • 60 FPS • Auto';
      clearAudienceUi();
      updateAudioUi();
    }

    // --- COPIAR LINK DA LIVE ---
    function copyStreamLink() {
      const liveUrl = window.location.origin + '/?watch=' + encodeURIComponent(myId);
      navigator.clipboard.writeText(liveUrl).then(() => {
        alert('📋 Link da Live copiado! Envie para seus amigos:\\n' + liveUrl);
      }).catch(() => {
        prompt('Copie o link da sua transmissão:', liveUrl);
      });
    }

    // --- SISTEMA DE AMIZADES ---
    function sendFriendRequestPrompt() {
      const targetId = prompt('Digite o ID do seu amigo (ex: dc-12345):');
      if (!targetId) return;
      const cleanId = targetId.trim().toLowerCase();

      if (cleanId === myId) {
        alert('Você não pode adicionar seu próprio ID!');
        return;
      }

      if (!wsSend({ type: 'FRIEND_REQUEST', targetId: cleanId })) {
        alert('Sem conexão com o servidor. Tente novamente quando reconectar.');
        return;
      }

      alert('Pedido de amizade enviado para ' + cleanId + '!');
    }

    function respondFriendRequest(accepted) {
      document.getElementById('friendRequestModal').style.display = 'none';
      if (!pendingRequestFrom) return;

      wsSend({
        type: 'FRIEND_RESPONSE',
        targetId: pendingRequestFrom.id,
        accepted: Boolean(accepted)
      });

      if (accepted) {
        addFriendToLocal(pendingRequestFrom.id, pendingRequestFrom.nick);
      }
      pendingRequestFrom = null;
    }

    function addFriendToLocal(id, name) {
      id = String(id || '').trim().toLowerCase();
      if (!id) return;

      const safeName = String(name || ('Amigo#' + id.slice(-4))).slice(0, 48);

      const existing = friends.find((f) => String(f.id || '').toLowerCase() === id);
      if (existing) {
        if (name && existing.name !== safeName) existing.name = safeName;
      } else {
        friends.push({ id, name: safeName });
      }

      localStorage.setItem('dc_saved_friends', JSON.stringify(friends));
      renderFriends();
    }

    function removeFriend(idx) {
      friends.splice(idx, 1);
      localStorage.setItem('dc_saved_friends', JSON.stringify(friends));
      renderFriends();
    }

    function renderFriends() {
      const container = document.getElementById('friendsContainer');
      container.innerHTML = '';

      const title = document.createElement('div');
      title.className = 'section-title';
      title.textContent = 'Lista de Amigos';
      container.appendChild(title);

      if (friends.length === 0) {
        const empty = document.createElement('div');
        empty.style.cssText = 'font-size:12px;color:var(--text-muted);padding:8px;';
        empty.textContent = 'Nenhum amigo ainda. Clique em "+ Adicionar".';
        container.appendChild(empty);
        return;
      }

      friends.forEach((friend, idx) => {
        const id = String(friend.id || '').toLowerCase();
        const name = String(friend.name || ('Amigo#' + id.slice(-4)));
        const isLive = liveFriendIds.has(id);

        const item = document.createElement('div');
        item.className = 'friend-item';

        const info = document.createElement('div');
        info.className = 'friend-info';

        const avatar = document.createElement('div');
        avatar.className = 'avatar';
        avatar.textContent = (name.charAt(0) || '?').toUpperCase();

        const dot = document.createElement('div');
        dot.className = 'status-dot';
        avatar.appendChild(dot);

        const textWrap = document.createElement('div');
        const nameEl = document.createElement('div');
        nameEl.className = 'friend-name';
        nameEl.textContent = name;

        const idEl = document.createElement('div');
        idEl.className = 'friend-id';
        idEl.textContent = id;

        textWrap.appendChild(nameEl);
        textWrap.appendChild(idEl);
        info.appendChild(avatar);
        info.appendChild(textWrap);

        const actions = document.createElement('div');
        actions.className = 'friend-actions';

        const watchBtn = document.createElement('button');
        watchBtn.className = 'btn-action ' + (isLive ? 'live' : 'watch');
        watchBtn.textContent = isLive ? '🔴 AO VIVO' : 'Assistir';
        watchBtn.addEventListener('click', () => requestWatch(id, false));

        const delBtn = document.createElement('button');
        delBtn.className = 'btn-action del';
        delBtn.textContent = '✕';
        delBtn.addEventListener('click', () => removeFriend(idx));

        actions.appendChild(watchBtn);
        actions.appendChild(delBtn);

        item.appendChild(info);
        item.appendChild(actions);
        container.appendChild(item);
      });
    }

    function requestWatch(friendId, fromReconnect = false, forceRelay = false, codecMode = 'vp8') {
      friendId = String(friendId || '').trim().toLowerCase();
      if (!friendId || friendId === myId || isSharing) return;

      if (!fromReconnect) {
        if (friendId === desiredWatchId && (activePC || viewerConnectTimer)) return;
        viewerRecoveryAttempts = 0;
        addFriendToLocal(friendId, 'Amigo#' + friendId.slice(-4));
      }

      if (desiredWatchId && desiredWatchId !== friendId) {
        closeViewerConnection(true);
      } else closeViewerConnection(false);

      desiredWatchId = friendId;
      if (!fromReconnect) viewerWantsAudio = friendId !== ownStreamBroadcasterId;
      watchRequestId = createSessionId();
      currentAudienceBroadcasterId = friendId;
      if (!fromReconnect) currentAudience = [];
      renderAudience();
      btnDisconnect.style.display = 'inline-flex';
      updateAudioUi();

      const relayRequested = Boolean((forceRelay || FORCE_TURN_RELAY_POLICY) && turnConfigured);
      statusBadge.innerText = relayRequested
        ? '🔄 Reconectando pela rota TURN segura...'
        : '🔄 Solicitando transmissão...';

      const requestedCodec = ['vp8', 'h264', 'auto'].includes(codecMode) ? codecMode : 'vp8';
      if (!wsSend({ type: 'REQUEST_STREAM', target: friendId, requestId: watchRequestId, forceRelay: relayRequested, codecMode: requestedCodec })) {
        scheduleReconnect();
        return;
      }
      viewerConnectTimer = setTimeout(() => {
        viewerConnectTimer = null;
        if (desiredWatchId === friendId) scheduleViewerRecovery(1000);
      }, 20000);
    }

    function copyMyId() {
      navigator.clipboard.writeText(myId).then(() => {
        alert('Seu ID copiado: ' + myId + '\\nEnvie para seu amigo te adicionar!');
      }).catch(() => {
        prompt('Copie seu ID:', myId);
      });
    }

    async function playViewerMedia(pc = activePC) {
      if (!pc || activePC !== pc || isSharing) return;
      try {
        await videoEl.play();
        if (activePC !== pc) return;
        unmuteNotice.style.display = videoEl.muted && viewerWantsAudio ? 'block' : 'none';
      } catch (err) {
        if (activePC !== pc) return;
        if (err.name === 'NotAllowedError') {
          videoEl.muted = true;
          unmuteNotice.style.display = 'block';
          await videoEl.play().catch(() => {});
        } else if (err.name !== 'AbortError') {
          unmuteNotice.style.display = 'block';
        }
      }
      updateAudioUi();
    }

    function unmute() {
      if (isSharing || desiredWatchId === ownStreamBroadcasterId) return;
      viewerWantsAudio = true;
      videoEl.muted = false;
      videoEl.volume = 1;
      playViewerMedia();
    }

    function toggleViewerAudio() {
      if (isSharing || desiredWatchId === ownStreamBroadcasterId) return;
      if (videoEl.muted) unmute();
      else {
        viewerWantsAudio = false;
        videoEl.muted = true;
        unmuteNotice.style.display = 'none';
        updateAudioUi();
      }
    }

    function toggleFullscreen() {
      const viewport = document.querySelector('.video-viewport');
      if (!document.fullscreenElement) {
        viewport.requestFullscreen().catch(() => {});
      } else {
        document.exitFullscreen().catch(() => {});
      }
    }

    // Link direto ?watch=dc-12345
    const initialParams = new URLSearchParams(window.location.search);
    const initialWatchId = String(initialParams.get('watch') || '').trim().toLowerCase();
    if (initialWatchId && initialWatchId !== myId) desiredWatchId = initialWatchId;

    window.addEventListener('beforeunload', () => {
      const target = activeBroadcasterId || desiredWatchId;
      if (target) {
        wsSend({ type: 'STOP_WATCH', target });
      }
    });

    renderFriends();
  </script>
</body>
</html>
  `);
});

server.keepAliveTimeout = 120000;
server.headersTimeout = 125000;

let shuttingDown = false;
function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;

  console.log(signal + ' recebido. Encerrando conexões com segurança...');

  for (const ws of wss.clients) {
    safeSend(ws, { type: 'SERVER_RESTARTING', appVersion: APP_VERSION });
    try { ws.close(1012, 'Service restart'); } catch (_) {}
  }

  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 10000).unref();
}

process.once('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.once('SIGINT', () => gracefulShutdown('SIGINT'));

server.listen(PORT, '0.0.0.0', () => {
  console.log('Servidor Discord ativo na porta: ' + server.address().port + ' | versão ' + APP_VERSION);
});
