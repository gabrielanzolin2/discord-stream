const express = require('express');
const http = require('http');
const crypto = require('crypto');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({
  server,
  maxPayload: 512 * 1024,
  perMessageDeflate: false
});

const PORT = Number(process.env.PORT || 3000);
const APP_VERSION = process.env.APP_VERSION || (process.env.RENDER_GIT_COMMIT ? process.env.RENDER_GIT_COMMIT.slice(0, 12) : '2026.09.04-ultra-1');
const DEFAULT_ROOM = 'sala-principal';

const TURN_URLS = String(process.env.TURN_URLS || '')
  .split(',')
  .map(v => v.trim())
  .filter(Boolean);
const TURN_SECRET = String(process.env.TURN_SECRET || '').trim();
const TURN_USERNAME = String(process.env.TURN_USERNAME || '').trim();
const TURN_CREDENTIAL = String(process.env.TURN_CREDENTIAL || '').trim();
const TURN_TTL_SECONDS = Math.max(300, Number(process.env.TURN_TTL_SECONDS || 86400));

const STUN_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },
  { urls: 'stun:stun.cloudflare.com:3478' }
];

app.disable('x-powered-by');

app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Permissions-Policy', 'camera=(), microphone=(self), display-capture=(self), fullscreen=(self)');
  next();
});

// Gerenciamento de usuários e salas
const activeUsers = new Map(); // userId -> { ws, nick, isLive, room, joinedAt }
const rooms = new Map();       // room -> Map(userId -> ws)

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

function safeSend(ws, payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  if (ws.bufferedAmount > 2 * 1024 * 1024) return false;
  ws.send(JSON.stringify(payload));
  return true;
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

wss.on('connection', (ws) => {
  let userId = null;
  let userNick = null;
  let userRoom = DEFAULT_ROOM;

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  safeSend(ws, {
    type: 'SERVER_HELLO',
    appVersion: APP_VERSION,
    serverTime: Date.now()
  });

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message.toString());
      if (!data || typeof data.type !== 'string') return;

      if (data.type === 'PING') {
        safeSend(ws, { type: 'PONG', t: data.t || Date.now(), serverTime: Date.now() });
        return;
      }

      if (data.type === 'JOIN') {
        const nextUserId = normalizeId(data.userId);
        if (!nextUserId) {
          safeSend(ws, { type: 'ERROR', code: 'INVALID_USER_ID' });
          return;
        }

        userId = nextUserId;
        userNick = normalizeNick(data.nick);
        userRoom = normalizeRoom(data.room);
        const isLive = Boolean(data.isLive);

        const previous = activeUsers.get(userId);
        if (previous && previous.ws !== ws) {
          safeSend(previous.ws, { type: 'SESSION_REPLACED' });
          try { previous.ws.close(4001, 'Session replaced'); } catch (_) {}
        }

        activeUsers.set(userId, {
          ws,
          nick: userNick,
          isLive,
          room: userRoom,
          joinedAt: Date.now()
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
          appVersion: APP_VERSION
        });
        safeSend(ws, { type: 'SYNC_LIVE_USERS', liveUsers });
        return;
      }

      if (!userId || activeUsers.get(userId)?.ws !== ws) return;

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
        const current = activeUsers.get(userId);
        if (current && current.ws === ws) current.isLive = Boolean(data.isLive);

        broadcastToRoom(userRoom, userId, {
          type: 'USER_LIVE_STATE',
          userId,
          nick: userNick,
          isLive: Boolean(data.isLive)
        });
        return;
      }

      if (['OFFER', 'ANSWER', 'CANDIDATE', 'REQUEST_STREAM', 'STOP_WATCH'].includes(data.type)) {
        const targetId = normalizeId(data.target);
        if (!targetId || targetId === userId) return;

        const targetClient = activeUsers.get(targetId);
        if (!targetClient) {
          if (data.type === 'REQUEST_STREAM') {
            safeSend(ws, { type: 'STREAM_NOT_FOUND', targetId });
          }
          return;
        }

        safeSend(targetClient.ws, {
          ...data,
          from: userId,
          fromNick: userNick
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

    activeUsers.delete(userId);

    const roomClients = rooms.get(userRoom);
    if (roomClients?.get(userId) === ws) {
      roomClients.delete(userId);
      if (roomClients.size === 0) rooms.delete(userRoom);
    }

    broadcastToRoom(userRoom, userId, { type: 'USER_LEFT', userId });
    broadcastToRoom(userRoom, userId, {
      type: 'USER_LIVE_STATE',
      userId,
      nick: userNick,
      isLive: false
    });
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

app.get('/health', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.json({
    ok: true,
    appVersion: APP_VERSION,
    usersOnline: activeUsers.size,
    uptimeSeconds: Math.floor(process.uptime())
  });
});

app.get('/api/version', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.json({ appVersion: APP_VERSION, serverTime: Date.now() });
});

app.get('/api/rtc-config', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');

  const iceServers = [...STUN_SERVERS];
  let turnMode = 'none';

  if (TURN_URLS.length && TURN_SECRET) {
    const userId = normalizeId(req.query.userId) || 'guest';
    const expiresAt = Math.floor(Date.now() / 1000) + TURN_TTL_SECONDS;
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
  } else if (TURN_URLS.length && TURN_USERNAME && TURN_CREDENTIAL) {
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
    expiresInSeconds: turnMode === 'ephemeral' ? TURN_TTL_SECONDS : null
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

    /* VÍDEO */
    .video-viewport { flex: 1; background: #000; display: flex; align-items: center; justify-content: center; position: relative; overflow: hidden; }
    video { width: 100%; height: 100%; max-width: 100%; max-height: 100%; object-fit: contain; background: #000; }

    .empty-state { position: absolute; text-align: center; color: var(--text-muted); }
    .empty-state svg { width: 72px; height: 72px; fill: #4e5058; margin-bottom: 12px; }

    /* DOCK FLUTUANTE */
    .control-dock { position: absolute; bottom: 24px; display: flex; align-items: center; gap: 10px; background: rgba(20,20,22,0.92); padding: 10px 20px; border-radius: 30px; backdrop-filter: blur(10px); z-index: 50; }
    .btn-dock { background: #313338; color: #fff; border: none; padding: 10px 18px; border-radius: 20px; font-weight: bold; cursor: pointer; transition: 0.2s; font-size: 13px; display: flex; align-items: center; gap: 6px; }
    .btn-dock:hover { background: #3f4147; }
    .btn-dock.primary { background: var(--discord-blurple); }
    .btn-dock.primary:hover { background: var(--discord-blurple-hover); }
    .btn-dock.danger { background: var(--discord-red); }
    .btn-dock.copy { background: var(--discord-green); }

    #unmuteNotice { position: absolute; top: 20px; background: rgba(0,0,0,0.85); border: 1px solid #f0b232; color: #f0b232; padding: 8px 18px; border-radius: 20px; font-weight: bold; font-size: 12px; cursor: pointer; display: none; z-index: 100; }

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
      </div>
      <span style="font-size: 12px; color: var(--discord-green);" id="connStatusText">🟢 Conectado à Nuvem</span>
    </div>

    <div class="video-viewport">
      <div id="unmuteNotice" onclick="unmute()">🔊 Clique aqui para ativar o áudio</div>
      <video id="remoteVideo" autoplay playsinline></video>
      
      <div class="empty-state" id="emptyState">
        <svg viewBox="0 0 24 24"><path d="M21 3H3c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h5v2h8v-2h5c1.1 0 1.99-.9 1.99-2L23 5c0-1.1-.9-2-2-2zm0 14H3V5h18v12z"/></svg>
        <h3 style="color: #fff;">Pronto para transmitir</h3>
        <p style="margin-top: 6px;">Transmita sua tela em 60 FPS sem tela preta!</p>
      </div>

      <!-- DOCK FLUTUANTE -->
      <div class="control-dock">
        <button class="btn-dock primary" id="btnShare" onclick="toggleShare()">Transmitir Tela</button>
        <button class="btn-dock copy" id="btnCopyLink" style="display: none;" onclick="copyStreamLink()">🔗 Copiar Link da Live</button>
        <button class="btn-dock danger" id="btnDisconnect" style="display: none;" onclick="disconnectStream()">Desconectar</button>
        <button class="btn-dock" onclick="toggleFullscreen()">Tela Cheia</button>
      </div>
    </div>
  </div>

  <script>
    // --- CONFIGURAÇÃO DA PÁGINA / QUALIDADE ---
    const PAGE_APP_VERSION = ${JSON.stringify(APP_VERSION)};
    const ROOM_NAME = 'sala-principal';
    const QUALITY = Object.freeze({
      width: 1920,
      height: 1080,
      fps: 60,
      startTargetBitrate: 12_000_000,
      maxBitrate: 18_000_000,
      minBitrate: 3_000_000,
      statsIntervalMs: 2500
    });

    // --- IDENTIDADE LOCAL ---
    let myId = localStorage.getItem('dc_user_id');
    if (!myId) {
      myId = 'dc-' + Math.floor(10000 + Math.random() * 90000);
      localStorage.setItem('dc_user_id', myId);
    }
    myId = myId.trim().toLowerCase();

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
    const statusBadge = document.getElementById('statusBadge');
    const connStatusText = document.getElementById('connStatusText');

    // --- ESTADO DE TRANSMISSÃO ---
    let localStream = null;
    let isSharing = false;

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
    let viewerRecoveryTimer = null;
    let viewerRecoveryAttempts = 0;
    let desiredWatchId = null;

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
      iceCandidatePoolSize: 6
    };
    let turnConfigured = false;
    let rtcConfigLoadedAt = 0;
    let rtcConfigPromise = null;

    async function refreshRtcConfig(force) {
      const age = Date.now() - rtcConfigLoadedAt;
      if (!force && rtcConfigLoadedAt && age < 12 * 60 * 60 * 1000) return rtcConfig;
      if (rtcConfigPromise) return rtcConfigPromise;

      rtcConfigPromise = (async () => {
        try {
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort(), 5000);
          const res = await fetch('/api/rtc-config?userId=' + encodeURIComponent(myId), {
            cache: 'no-store',
            signal: controller.signal
          });
          clearTimeout(timeout);

          if (!res.ok) throw new Error('HTTP ' + res.status);
          const data = await res.json();

          if (Array.isArray(data.iceServers) && data.iceServers.length) {
            rtcConfig = {
              iceServers: data.iceServers,
              iceTransportPolicy: data.iceTransportPolicy || 'all',
              bundlePolicy: 'max-bundle',
              rtcpMuxPolicy: 'require',
              iceCandidatePoolSize: 6
            };
          }

          turnConfigured = Boolean(data.turnConfigured);
          rtcConfigLoadedAt = Date.now();

          if (!turnConfigured) {
            console.warn('TURN não configurado: P2P/STUN funcionará, mas alguns CGNATs/firewalls podem bloquear a conexão.');
          }
        } catch (err) {
          console.warn('Falha ao carregar RTC config; usando STUN padrão:', err.message);
        } finally {
          rtcConfigPromise = null;
        }
        return rtcConfig;
      })();

      return rtcConfigPromise;
    }

    refreshRtcConfig(false);

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

      socket.onopen = () => {
        if (ws !== socket) return;
        reconnectAttempt = 0;
        setConnectionUi('online');

        wsSend({
          type: 'JOIN',
          userId: myId,
          nick: myNick,
          room: ROOM_NAME,
          isLive: isSharing
        });
      };

      socket.onmessage = async (event) => {
        if (ws !== socket) return;

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
          if (isSharing) {
            wsSend({ type: 'LIVE_STATE_CHANGE', isLive: true });
          }

          const viewerNeedsResync =
            !activePC ||
            ['failed', 'closed'].includes(activePC.connectionState);

          if (desiredWatchId && desiredWatchId !== myId && !isSharing && viewerNeedsResync) {
            setTimeout(() => {
              if (ws === socket && socket.readyState === WebSocket.OPEN) {
                requestWatch(desiredWatchId, true);
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

        if (data.type === 'STREAM_NOT_FOUND') {
          if (desiredWatchId === String(data.targetId || '').toLowerCase()) {
            statusBadge.innerText = '⚠️ Transmissor offline';
          }
          return;
        }

        if (data.type === 'STOP_WATCH') {
          closeSenderPeer(data.from);
          return;
        }

        if (data.type === 'REQUEST_STREAM' && isSharing && localStream) {
          await initiateStreamToViewer(data.from);
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
        setConnectionUi('offline');

        if (allowReconnect) scheduleReconnect();
      };
    }

    connectWS();

    setInterval(() => {
      wsSend({ type: 'PING', t: Date.now() });
    }, 15000);

    window.addEventListener('online', () => {
      allowReconnect = true;
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      connectWS();
    });

    window.addEventListener('offline', () => {
      setConnectionUi('offline');
    });

    // --- WEBRTC: CODECS E PERFIL DE ALTA QUALIDADE ---
    function preferScreenShareCodecs(transceiver) {
      if (!transceiver || typeof transceiver.setCodecPreferences !== 'function') return;
      if (!window.RTCRtpSender || typeof RTCRtpSender.getCapabilities !== 'function') return;

      try {
        const caps = RTCRtpSender.getCapabilities('video');
        if (!caps || !Array.isArray(caps.codecs)) return;

        const rank = {
          'video/VP9': 0,
          'video/H264': 1,
          'video/VP8': 2,
          'video/AV1': 3
        };

        const primary = [];
        const auxiliary = [];

        caps.codecs.forEach((codec) => {
          if (Object.prototype.hasOwnProperty.call(rank, codec.mimeType)) primary.push(codec);
          else auxiliary.push(codec);
        });

        primary.sort((a, b) => rank[a.mimeType] - rank[b.mimeType]);
        transceiver.setCodecPreferences(primary.concat(auxiliary));
      } catch (err) {
        console.warn('Não foi possível ajustar preferência de codec:', err.message);
      }
    }

    async function applyVideoSenderProfile(sender, targetBitrate) {
      if (!sender || !sender.track || sender.track.kind !== 'video') return;

      const bitrate = Math.max(
        QUALITY.minBitrate,
        Math.min(QUALITY.maxBitrate, Number(targetBitrate || QUALITY.maxBitrate))
      );

      try {
        const params = sender.getParameters();
        if (!Array.isArray(params.encodings) || params.encodings.length === 0) {
          params.encodings = [{}];
        }

        params.encodings[0].maxBitrate = bitrate;
        params.encodings[0].maxFramerate = QUALITY.fps;
        params.encodings[0].scaleResolutionDownBy = 1;
        params.degradationPreference = 'maintain-resolution';

        await sender.setParameters(params);
        return;
      } catch (err) {
        console.warn('Perfil RTP completo não suportado; tentando modo compatível:', err.message);
      }

      try {
        const fallback = sender.getParameters();
        if (!Array.isArray(fallback.encodings) || fallback.encodings.length === 0) {
          fallback.encodings = [{}];
        }
        fallback.encodings[0].maxBitrate = bitrate;
        fallback.encodings[0].maxFramerate = QUALITY.fps;
        await sender.setParameters(fallback);
      } catch (err) {
        console.warn('Ajuste RTP de bitrate/FPS não suportado por este navegador:', err.message);
      }
    }

    async function configureCaptureTrack(videoTrack) {
      if (!videoTrack) return;

      if ('contentHint' in videoTrack) {
        videoTrack.contentHint = 'motion';
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
    async function toggleShare() {
      if (isSharing) {
        stopShare();
        return;
      }

      try {
        await refreshRtcConfig(false);

        localStream = await navigator.mediaDevices.getDisplayMedia({
          video: {
            frameRate: { ideal: QUALITY.fps, max: QUALITY.fps },
            width: { ideal: QUALITY.width, max: QUALITY.width },
            height: { ideal: QUALITY.height, max: QUALITY.height }
          },
          audio: true
        });

        const videoTrack = localStream.getVideoTracks()[0];
        const audioTrack = localStream.getAudioTracks()[0];

        await configureCaptureTrack(videoTrack);

        if (audioTrack && 'contentHint' in audioTrack) {
          audioTrack.contentHint = 'music';
        }

        isSharing = true;
        btnShare.innerText = 'Parar Transmissão';
        btnShare.classList.add('danger');
        btnCopyLink.style.display = 'inline-flex';
        stageTitle.innerText = 'Você está transmitindo sua tela';
        liveBadge.style.display = 'inline-block';
        emptyState.style.display = 'none';

        videoEl.srcObject = localStream;
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

        wsSend({ type: 'LIVE_STATE_CHANGE', isLive: true });
      } catch (err) {
        console.error('Erro ao capturar tela:', err);
        statusBadge.innerText = '⚠️ Captura cancelada ou indisponível';
      }
    }

    function stopShare() {
      if (!isSharing && !localStream) return;

      for (const viewerId of Array.from(senderPeers.keys())) {
        closeSenderPeer(viewerId);
      }

      if (localStream) {
        localStream.getTracks().forEach((track) => {
          try { track.stop(); } catch (_) {}
        });
        localStream = null;
      }

      isSharing = false;
      btnShare.innerText = 'Transmitir Tela';
      btnShare.classList.remove('danger');
      btnCopyLink.style.display = 'none';
      stageTitle.innerText = 'Nenhuma transmissão em andamento';
      liveBadge.style.display = 'none';
      videoEl.srcObject = null;
      emptyState.style.display = 'block';
      statusBadge.innerText = '⚡ Full HD • 60 FPS • Auto';

      wsSend({ type: 'LIVE_STATE_CHANGE', isLive: false });
    }

    async function initiateStreamToViewer(viewerId) {
      if (!isSharing || !localStream || !viewerId) return;

      await refreshRtcConfig(false);
      closeSenderPeer(viewerId);

      const pc = new RTCPeerConnection(rtcConfig);
      const sessionId = createSessionId();

      const peer = {
        pc,
        sessionId,
        remoteReady: false,
        remoteCandidates: [],
        localCandidates: [],
        signalReady: false,
        videoSender: null,
        statsTimer: null,
        reconnectTimer: null,
        targetBitrate: QUALITY.maxBitrate,
        weakSamples: 0,
        strongSamples: 0,
        lastBytesSent: null,
        lastStatsAt: null,
        closed: false
      };

      senderPeers.set(viewerId, peer);

      const videoTrack = localStream.getVideoTracks()[0];
      const audioTracks = localStream.getAudioTracks();

      if (videoTrack) {
        try {
          const transceiver = pc.addTransceiver(videoTrack, {
            direction: 'sendonly',
            streams: [localStream],
            sendEncodings: [{
              maxBitrate: QUALITY.maxBitrate,
              maxFramerate: QUALITY.fps,
              scaleResolutionDownBy: 1
            }]
          });
          peer.videoSender = transceiver.sender;
          preferScreenShareCodecs(transceiver);
        } catch (err) {
          console.warn('addTransceiver avançado indisponível; usando addTrack:', err.message);
          peer.videoSender = pc.addTrack(videoTrack, localStream);
        }
      }

      audioTracks.forEach((track) => pc.addTrack(track, localStream));

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

      pc.onconnectionstatechange = () => {
        if (peer.closed) return;
        const state = pc.connectionState;

        if (state === 'connected') {
          if (peer.reconnectTimer) {
            clearTimeout(peer.reconnectTimer);
            peer.reconnectTimer = null;
          }
        } else if (state === 'failed') {
          scheduleSenderPeerRecovery(viewerId, peer, 500);
        } else if (state === 'disconnected') {
          scheduleSenderPeerRecovery(viewerId, peer, 3000);
        }
      };

      await applyVideoSenderProfile(peer.videoSender, QUALITY.maxBitrate);

      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);

      if (peer.closed || senderPeers.get(viewerId) !== peer) return;

      wsSend({
        type: 'OFFER',
        target: viewerId,
        sessionId,
        sdp: pc.localDescription,
        quality: {
          width: QUALITY.width,
          height: QUALITY.height,
          fps: QUALITY.fps,
          maxBitrate: QUALITY.maxBitrate
        }
      });

      peer.signalReady = true;
      while (peer.localCandidates.length) {
        wsSend(peer.localCandidates.shift());
      }
    }

    function scheduleSenderPeerRecovery(viewerId, peer, delay) {
      if (peer.closed || peer.reconnectTimer || !isSharing) return;

      peer.reconnectTimer = setTimeout(() => {
        peer.reconnectTimer = null;
        if (peer.closed || senderPeers.get(viewerId) !== peer || !isSharing) return;
        initiateStreamToViewer(viewerId).catch(console.warn);
      }, delay);
    }

    async function handleSenderAnswer(data) {
      const peer = senderPeers.get(data.from);
      if (!peer || peer.closed) return;
      if (data.sessionId !== peer.sessionId) return;

      try {
        await peer.pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
        peer.remoteReady = true;

        while (peer.remoteCandidates.length) {
          const candidate = peer.remoteCandidates.shift();
          await peer.pc.addIceCandidate(candidate).catch(console.warn);
        }

        await applyVideoSenderProfile(peer.videoSender, peer.targetBitrate);
        startSenderQualityManager(data.from, peer);
      } catch (err) {
        console.warn('Erro ao aplicar ANSWER:', err.message);
        scheduleSenderPeerRecovery(data.from, peer, 700);
      }
    }

    function closeSenderPeer(viewerId) {
      const peer = senderPeers.get(viewerId);
      if (!peer) return;

      peer.closed = true;
      if (peer.statsTimer) clearInterval(peer.statsTimer);
      if (peer.reconnectTimer) clearTimeout(peer.reconnectTimer);

      try {
        peer.pc.onicecandidate = null;
        peer.pc.onconnectionstatechange = null;
        peer.pc.close();
      } catch (_) {}

      senderPeers.delete(viewerId);

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
        if (peer.closed || peer.pc.connectionState !== 'connected') return;

        try {
          const report = await peer.pc.getStats();
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
                Math.min(QUALITY.maxBitrate, Math.floor(available * 0.90))
              );
              peer.weakSamples = 0;
              await applyVideoSenderProfile(peer.videoSender, peer.targetBitrate);
            } else if (peer.strongSamples >= 3 && peer.targetBitrate < QUALITY.maxBitrate) {
              peer.targetBitrate = Math.min(
                QUALITY.maxBitrate,
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
            const mbpsText = currentMbps === null ? 'iniciando' : currentMbps.toFixed(1) + ' Mbps';

            statusBadge.innerText =
              '⚡ ' + width + '×' + height + ' • ' + fps + ' FPS • ' +
              mbpsText + ' • ' + senderPeers.size + ' espectador(es)';
          }
        } catch (_) {}
      }, QUALITY.statsIntervalMs);
    }

    // --- RECEBENDO A TRANSMISSÃO ---
    async function handleIncomingOffer(data) {
      if (!data.from || !data.sessionId || !data.sdp) return;

      await refreshRtcConfig(false);
      closeViewerConnection(false);

      activeBroadcasterId = data.from;
      activeSessionId = data.sessionId;
      desiredWatchId = data.from;
      activePCRemoteReady = false;
      activePCQueue = [];
      viewerLocalCandidates = [];
      viewerSignalReady = false;

      const pc = new RTCPeerConnection(rtcConfig);
      activePC = pc;

      const remoteMediaStream = new MediaStream();
      videoEl.srcObject = remoteMediaStream;

      pc.ontrack = (event) => {
        if (activePC !== pc) return;

        if (!remoteMediaStream.getTracks().some((t) => t.id === event.track.id)) {
          remoteMediaStream.addTrack(event.track);
        }

        videoEl.muted = true;
        videoEl.play().catch(() => {
          unmuteNotice.style.display = 'block';
        });

        event.track.onunmute = () => {
          if (activePC === pc) videoEl.play().catch(() => {});
        };

        emptyState.style.display = 'none';
        stageTitle.innerText = 'Assistindo tela de ' + (data.fromNick || data.from);
        liveBadge.style.display = 'inline-block';
        btnDisconnect.style.display = 'flex';

        if (event.track.kind === 'audio') {
          unmuteNotice.style.display = 'block';
        }
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

      pc.onconnectionstatechange = () => {
        if (activePC !== pc) return;

        const state = pc.connectionState;
        if (state === 'connected') {
          viewerRecoveryAttempts = 0;
          if (viewerRecoveryTimer) {
            clearTimeout(viewerRecoveryTimer);
            viewerRecoveryTimer = null;
          }
          startViewerStats(pc);
        } else if (state === 'connecting') {
          statusBadge.innerText = '🔄 Conectando WebRTC...';
        } else if (state === 'failed') {
          statusBadge.innerText = '⚠️ Reconectando rota de internet...';
          scheduleViewerRecovery(400);
        } else if (state === 'disconnected') {
          statusBadge.innerText = '🟡 Rede instável • tentando recuperar...';
          scheduleViewerRecovery(2500);
        }
      };

      try {
        await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
        activePCRemoteReady = true;

        while (activePCQueue.length) {
          const candidate = activePCQueue.shift();
          await pc.addIceCandidate(candidate).catch(console.warn);
        }

        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);

        if (activePC !== pc) return;

        wsSend({
          type: 'ANSWER',
          target: data.from,
          sessionId: data.sessionId,
          sdp: pc.localDescription
        });

        viewerSignalReady = true;
        while (viewerLocalCandidates.length) {
          wsSend(viewerLocalCandidates.shift());
        }
      } catch (err) {
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

      viewerRecoveryAttempts += 1;
      const backoff = Math.min(6000, delay * Math.max(1, viewerRecoveryAttempts));

      viewerRecoveryTimer = setTimeout(() => {
        viewerRecoveryTimer = null;
        const target = desiredWatchId;
        closeViewerConnection(false);
        if (target) requestWatch(target, true);
      }, backoff);
    }

    function startViewerStats(pc) {
      if (viewerStatsTimer) clearInterval(viewerStatsTimer);

      let lastBytes = null;
      let lastAt = null;

      viewerStatsTimer = setInterval(async () => {
        if (activePC !== pc || pc.connectionState !== 'connected') return;

        try {
          const report = await pc.getStats();
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
      const oldBroadcaster = activeBroadcasterId;

      if (viewerRecoveryTimer) {
        clearTimeout(viewerRecoveryTimer);
        viewerRecoveryTimer = null;
      }
      if (viewerStatsTimer) {
        clearInterval(viewerStatsTimer);
        viewerStatsTimer = null;
      }

      if (sendStop && oldBroadcaster) {
        wsSend({ type: 'STOP_WATCH', target: oldBroadcaster });
      }

      if (activePC) {
        try {
          activePC.ontrack = null;
          activePC.onicecandidate = null;
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
    }

    function disconnectStream() {
      desiredWatchId = null;
      closeViewerConnection(true);

      videoEl.srcObject = null;
      emptyState.style.display = 'block';
      stageTitle.innerText = 'Nenhuma transmissão em andamento';
      liveBadge.style.display = 'none';
      btnDisconnect.style.display = 'none';
      unmuteNotice.style.display = 'none';
      statusBadge.innerText = '⚡ Full HD • 60 FPS • Auto';
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

    function requestWatch(friendId, fromReconnect) {
      friendId = String(friendId || '').trim().toLowerCase();
      if (!friendId || friendId === myId || isSharing) return;

      if (!fromReconnect) {
        addFriendToLocal(friendId, 'Amigo#' + friendId.slice(-4));
      }

      if (activeBroadcasterId && activeBroadcasterId !== friendId) {
        closeViewerConnection(true);
      }

      desiredWatchId = friendId;
      statusBadge.innerText = '🔄 Solicitando transmissão...';

      if (!wsSend({ type: 'REQUEST_STREAM', target: friendId })) {
        scheduleReconnect();
      }
    }

    function copyMyId() {
      navigator.clipboard.writeText(myId).then(() => {
        alert('Seu ID copiado: ' + myId + '\\nEnvie para seu amigo te adicionar!');
      }).catch(() => {
        prompt('Copie seu ID:', myId);
      });
    }

    function unmute() {
      videoEl.muted = false;
      unmuteNotice.style.display = 'none';
      videoEl.play().catch(() => {});
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
      if (activeBroadcasterId) {
        wsSend({ type: 'STOP_WATCH', target: activeBroadcasterId });
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
  console.log('Servidor Discord ativo na porta: ' + PORT + ' | versão ' + APP_VERSION);
});
