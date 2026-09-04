const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;

// Gerenciamento de Usuários
const activeUsers = new Map(); // [userId -> { ws, nick, isLive, room }]
const rooms = new Map();

wss.on('connection', (ws) => {
  let userId = null;
  let userNick = null;
  let userRoom = 'sala-principal';

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      // Heartbeat para manter o Render ativo
      if (data.type === 'PING') {
        ws.send(JSON.stringify({ type: 'PONG' }));
        return;
      }

      // 1. Entrada de Usuário
      if (data.type === 'JOIN') {
        userId = String(data.userId).trim().toLowerCase();
        userNick = data.nick || 'Gamer';
        userRoom = data.room || 'sala-principal';

        activeUsers.set(userId, { ws, nick: userNick, isLive: false, room: userRoom });

        if (!rooms.has(userRoom)) rooms.set(userRoom, new Map());
        rooms.get(userRoom).set(userId, ws);

        broadcastToRoom(userRoom, userId, { type: 'USER_JOINED', userId, nick: userNick });

        // Envia lista de quem já está ao vivo
        const liveUsers = [];
        for (const [id, u] of activeUsers) {
          if (u.isLive && id !== userId) liveUsers.push({ userId: id, nick: u.nick });
        }
        ws.send(JSON.stringify({ type: 'SYNC_LIVE_USERS', liveUsers }));
      }

      // 2. Pedido de Amizade
      if (data.type === 'FRIEND_REQUEST') {
        const targetId = String(data.targetId).trim().toLowerCase();
        const targetClient = activeUsers.get(targetId);

        if (targetClient && targetClient.ws.readyState === WebSocket.OPEN) {
          targetClient.ws.send(JSON.stringify({
            type: 'FRIEND_REQUEST_INCOMING',
            fromId: userId,
            fromNick: userNick
          }));
        } else {
          ws.send(JSON.stringify({ type: 'FRIEND_NOT_FOUND', targetId }));
        }
      }

      // 3. Resposta do Pedido de Amizade
      if (data.type === 'FRIEND_RESPONSE') {
        const targetId = String(data.targetId).trim().toLowerCase();
        const targetClient = activeUsers.get(targetId);
        if (targetClient && targetClient.ws.readyState === WebSocket.OPEN) {
          targetClient.ws.send(JSON.stringify({
            type: 'FRIEND_RESPONSE_RESULT',
            fromId: userId,
            fromNick: userNick,
            accepted: data.accepted
          }));
        }
      }

      // 4. Mudança de Estado (Ao Vivo)
      if (data.type === 'LIVE_STATE_CHANGE') {
        if (activeUsers.has(userId)) {
          activeUsers.get(userId).isLive = data.isLive;
        }
        broadcastToAll({
          type: 'USER_LIVE_STATE',
          userId,
          nick: userNick,
          isLive: data.isLive
        });
      }

      // 5. Sinalização WebRTC
      if (['OFFER', 'ANSWER', 'CANDIDATE', 'REQUEST_STREAM'].includes(data.type)) {
        const target = String(data.target).trim().toLowerCase();
        const targetClient = activeUsers.get(target);
        if (targetClient && targetClient.ws.readyState === WebSocket.OPEN) {
          targetClient.ws.send(JSON.stringify({ ...data, from: userId, fromNick: userNick }));
        }
      }

    } catch (err) {
      console.error('Erro WebSocket:', err);
    }
  });

  ws.on('close', () => {
    if (userId) {
      activeUsers.delete(userId);
      if (rooms.has(userRoom)) {
        rooms.get(userRoom).delete(userId);
        broadcastToRoom(userRoom, userId, { type: 'USER_LEFT', userId });
      }
      broadcastToAll({ type: 'USER_LIVE_STATE', userId, isLive: false });
    }
  });
});

function broadcastToRoom(room, senderId, data) {
  const clients = rooms.get(room);
  if (!clients) return;
  for (const [id, clientWs] of clients) {
    if (id !== senderId && clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(JSON.stringify(data));
    }
  }
}

function broadcastToAll(data) {
  for (const [, client] of activeUsers) {
    if (client.ws.readyState === WebSocket.OPEN) {
      client.ws.send(JSON.stringify(data));
    }
  }
}

// FRONTEND COMPLETO DO DISCORD
app.get('/', (req, res) => {
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
        <span class="badge-gpu" id="statusBadge">⚡ 60 FPS • Full HD</span>
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
    // --- IDENTIDADE LOCAL ---
    let myId = localStorage.getItem('dc_user_id');
    if (!myId) {
      myId = 'dc-' + Math.floor(10000 + Math.random() * 90000);
      localStorage.setItem('dc_user_id', myId);
    }
    myId = myId.trim().toLowerCase();

    let myNick = localStorage.getItem('dc_user_nick') || 'Gamer#' + myId.slice(-4);
    let friends = JSON.parse(localStorage.getItem('dc_saved_friends') || '[]');
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

    let localStream = null;
    let isSharing = false;

    // Conexão WebRTC (Assistindo)
    let activePC = null;
    let activePCQueue = [];
    let activePCRemoteReady = false;

    // Conexões WebRTC (Transmitindo para N amigos)
    let senderPCs = new Map();
    let senderQueues = new Map();
    let senderRemoteReady = new Map();

    // SERVIDORES STUN + TURN ATIVOS (FURA CGNAT BRASIL)
    const rtcConfig = {
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        { urls: 'stun:stun2.l.google.com:19302' },
        { urls: 'stun:stun.cloudflare.com:3478' },
        { urls: 'stun:freeturn.net:3478' },
        {
          urls: [
            'turn:freeturn.net:3478?transport=udp',
            'turn:freeturn.net:3478?transport=tcp'
          ],
          username: 'free',
          credential: 'free'
        }
      ],
      iceCandidatePoolSize: 10
    };

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
      } catch (e) {}
    }

    // --- CONEXÃO WEBSOCKET COM AUTO-RECONNECT ---
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    let ws = null;

    function connectWS() {
      ws = new WebSocket(protocol + '//' + location.host);

      ws.onopen = () => {
        ws.send(JSON.stringify({ type: 'JOIN', userId: myId, nick: myNick, room: 'sala-principal' }));

        // Se entrou pelo link direto (?watch=dc-12345)
        const params = new URLSearchParams(window.location.search);
        const autoWatchId = params.get('watch');
        if (autoWatchId && autoWatchId.trim().toLowerCase() !== myId) {
          setTimeout(() => {
            requestWatch(autoWatchId.trim().toLowerCase());
          }, 600);
        }
      };

      ws.onmessage = async (event) => {
        const data = JSON.parse(event.data);

        // PEDIDO DE AMIZADE
        if (data.type === 'FRIEND_REQUEST_INCOMING') {
          playDiscordChime();
          pendingRequestFrom = { id: data.fromId, nick: data.fromNick };
          document.getElementById('friendRequestText').innerText = data.fromNick + ' (' + data.fromId + ') quer ser seu amigo!';
          document.getElementById('friendRequestModal').style.display = 'flex';
        }

        if (data.type === 'FRIEND_RESPONSE_RESULT') {
          if (data.accepted) {
            addFriendToLocal(data.fromId, data.fromNick);
            alert('🎉 ' + data.fromNick + ' aceitou seu pedido de amizade!');
          } else {
            alert('❌ ' + data.fromNick + ' recusou o pedido.');
          }
        }

        if (data.type === 'FRIEND_NOT_FOUND') {
          alert('❌ O ID ' + data.targetId + ' não está online no momento!');
        }

        // SINCRONIZAÇÃO DE STATUS AO VIVO
        if (data.type === 'SYNC_LIVE_USERS') {
          data.liveUsers.forEach(u => liveFriendIds.add(u.userId.toLowerCase()));
          renderFriends();
        }

        if (data.type === 'USER_LIVE_STATE') {
          if (data.isLive) liveFriendIds.add(data.userId.toLowerCase());
          else liveFriendIds.delete(data.userId.toLowerCase());
          renderFriends();
        }

        // SOLICITAÇÃO DE STREAM
        if (data.type === 'REQUEST_STREAM' && isSharing && localStream) {
          initiateStreamToViewer(data.from);
        }

        // SINALIZAÇÃO WEBRTC
        if (data.type === 'OFFER') {
          handleIncomingOffer(data);
        }

        if (data.type === 'ANSWER') {
          const pc = senderPCs.get(data.from);
          if (pc) {
            await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
            senderRemoteReady.set(data.from, true);
            const queue = senderQueues.get(data.from) || [];
            while (queue.length > 0) {
              const c = queue.shift();
              await pc.addIceCandidate(c).catch(console.warn);
            }
          }
        }

        if (data.type === 'CANDIDATE') {
          if (activePC) {
            if (!activePCRemoteReady) {
              activePCQueue.push(data.candidate);
            } else {
              await activePC.addIceCandidate(data.candidate).catch(console.warn);
            }
          } else if (senderPCs.has(data.from)) {
            const pc = senderPCs.get(data.from);
            const ready = senderRemoteReady.get(data.from);
            if (!ready) {
              if (!senderQueues.has(data.from)) senderQueues.set(data.from, []);
              senderQueues.get(data.from).push(data.candidate);
            } else {
              await pc.addIceCandidate(data.candidate).catch(console.warn);
            }
          }
        }
      };

      ws.onclose = () => {
        setTimeout(connectWS, 2000);
      };
    }

    connectWS();

    // Heartbeat regular
    setInterval(() => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'PING' }));
      }
    }, 15000);

    // --- TRANSMISSÃO NATIVA SEM TELA PRETA ---
    async function toggleShare() {
      if (isSharing) {
        stopShare();
        return;
      }

      try {
        localStream = await navigator.mediaDevices.getDisplayMedia({
          video: {
            frameRate: { ideal: 60, max: 60 },
            width: { ideal: 1920 },
            height: { ideal: 1080 }
          },
          audio: true
        });

        const videoTrack = localStream.getVideoTracks()[0];
        if (videoTrack && 'contentHint' in videoTrack) {
          videoTrack.contentHint = 'motion';
        }

        isSharing = true;
        btnShare.innerText = 'Parar Transmissão';
        btnShare.classList.add('danger');
        btnCopyLink.style.display = 'inline-flex';
        stageTitle.innerText = 'Você está transmitindo sua tela (60 FPS)';
        liveBadge.style.display = 'inline-block';
        emptyState.style.display = 'none';

        videoEl.srcObject = localStream;
        videoEl.muted = true;
        videoEl.play();

        videoTrack.onended = () => stopShare();

        ws.send(JSON.stringify({ type: 'LIVE_STATE_CHANGE', isLive: true }));

      } catch (err) {
        console.error('Erro ao capturar tela:', err);
      }
    }

    function stopShare() {
      if (localStream) {
        localStream.getTracks().forEach(t => t.stop());
        localStream = null;
      }
      senderPCs.forEach(pc => pc.close());
      senderPCs.clear();
      senderQueues.clear();
      senderRemoteReady.clear();

      isSharing = false;
      btnShare.innerText = 'Transmitir Tela';
      btnShare.classList.remove('danger');
      btnCopyLink.style.display = 'none';
      stageTitle.innerText = 'Nenhuma transmissão em andamento';
      liveBadge.style.display = 'none';
      videoEl.srcObject = null;
      emptyState.style.display = 'block';

      ws.send(JSON.stringify({ type: 'LIVE_STATE_CHANGE', isLive: false }));
    }

    async function initiateStreamToViewer(viewerId) {
      const pc = new RTCPeerConnection(rtcConfig);
      senderPCs.set(viewerId, pc);
      senderQueues.set(viewerId, []);
      senderRemoteReady.set(viewerId, false);

      localStream.getTracks().forEach(track => pc.addTrack(track, localStream));

      pc.onicecandidate = (e) => {
        if (e.candidate) {
          ws.send(JSON.stringify({ type: 'CANDIDATE', target: viewerId, candidate: e.candidate }));
        }
      };

      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);

      ws.send(JSON.stringify({ type: 'OFFER', target: viewerId, sdp: pc.localDescription }));
    }

    // --- RECEBENDO A TRANSMISSÃO (CORREÇÃO DA TELA PRETA) ---
    async function handleIncomingOffer(data) {
      if (activePC) {
        activePC.close();
      }
      activePC = new RTCPeerConnection(rtcConfig);
      activePCRemoteReady = false;

      // Cria ou reutiliza o MediaStream para que Áudio E Vídeo funcionem juntos
      const remoteMediaStream = new MediaStream();
      videoEl.srcObject = remoteMediaStream;

      activePC.ontrack = (event) => {
        remoteMediaStream.addTrack(event.track);

        // Força play imediato assim que os quadros de vídeo chegarem
        videoEl.muted = true;
        videoEl.play().catch(() => {
          unmuteNotice.style.display = 'block';
        });

        event.track.onunmute = () => {
          videoEl.play().catch(() => {});
        };

        emptyState.style.display = 'none';
        stageTitle.innerText = 'Assistindo tela de ' + (data.fromNick || data.from);
        liveBadge.style.display = 'inline-block';
        btnDisconnect.style.display = 'flex';
        unmuteNotice.style.display = 'block';
      };

      activePC.onicecandidate = (e) => {
        if (e.candidate) {
          ws.send(JSON.stringify({ type: 'CANDIDATE', target: data.from, candidate: e.candidate }));
        }
      };

      // Monitor de conexão ICE para feedback visual
      activePC.oniceconnectionstatechange = () => {
        if (activePC.iceConnectionState === 'connected') {
          statusBadge.innerText = '⚡ Conectado P2P • 60 FPS';
        } else if (activePC.iceConnectionState === 'checking') {
          statusBadge.innerText = '🔄 Conectando P2P...';
        } else if (activePC.iceConnectionState === 'failed') {
          statusBadge.innerText = '⚠️ Reconectando via TURN...';
          activePC.restartIce();
        }
      };

      await activePC.setRemoteDescription(new RTCSessionDescription(data.sdp));
      activePCRemoteReady = true;

      // Esvazia candidatos que chegaram antes da descrição remota
      while (activePCQueue.length > 0) {
        const c = activePCQueue.shift();
        await activePC.addIceCandidate(c).catch(console.warn);
      }

      const answer = await activePC.createAnswer();
      await activePC.setLocalDescription(answer);

      ws.send(JSON.stringify({ type: 'ANSWER', target: data.from, sdp: activePC.localDescription }));
    }

    function disconnectStream() {
      if (activePC) {
        activePC.close();
        activePC = null;
      }
      activePCQueue = [];
      activePCRemoteReady = false;
      videoEl.srcObject = null;
      emptyState.style.display = 'block';
      stageTitle.innerText = 'Nenhuma transmissão em andamento';
      liveBadge.style.display = 'none';
      btnDisconnect.style.display = 'none';
      unmuteNotice.style.display = 'none';
      statusBadge.innerText = '⚡ 60 FPS • Full HD';
    }

    // --- COPIAR LINK DA LIVE ---
    function copyStreamLink() {
      const liveUrl = window.location.origin + '/?watch=' + myId;
      navigator.clipboard.writeText(liveUrl).then(() => {
        alert('📋 Link da Live copiado! Envie para seus amigos:\\n' + liveUrl);
      }).catch(() => {
        prompt('Copie o link da sua transmissão:', liveUrl);
      });
    }

    // --- SISTEMA DE AMIZADES DISCORD ---
    function sendFriendRequestPrompt() {
      const targetId = prompt("Digite o ID do seu amigo (ex: dc-12345):");
      if (!targetId) return;
      const cleanId = targetId.trim().toLowerCase();

      if (cleanId === myId) {
        alert("Você não pode adicionar seu próprio ID!");
        return;
      }

      ws.send(JSON.stringify({
        type: 'FRIEND_REQUEST',
        targetId: cleanId
      }));

      alert("Pedido de amizade enviado para " + cleanId + "!");
    }

    function respondFriendRequest(accepted) {
      document.getElementById('friendRequestModal').style.display = 'none';
      if (!pendingRequestFrom) return;

      ws.send(JSON.stringify({
        type: 'FRIEND_RESPONSE',
        targetId: pendingRequestFrom.id,
        accepted
      }));

      if (accepted) {
        addFriendToLocal(pendingRequestFrom.id, pendingRequestFrom.nick);
      }
      pendingRequestFrom = null;
    }

    function addFriendToLocal(id, name) {
      id = id.toLowerCase();
      if (!friends.some(f => f.id.toLowerCase() === id)) {
        friends.push({ id, name });
        localStorage.setItem('dc_saved_friends', JSON.stringify(friends));
        renderFriends();
      }
    }

    function removeFriend(idx) {
      friends.splice(idx, 1);
      localStorage.setItem('dc_saved_friends', JSON.stringify(friends));
      renderFriends();
    }

    function renderFriends() {
      const container = document.getElementById('friendsContainer');
      container.innerHTML = '<div class="section-title">Lista de Amigos</div>';

      if (friends.length === 0) {
        container.innerHTML += '<div style="font-size: 12px; color: var(--text-muted); padding: 8px;">Nenhum amigo ainda. Clique em "+ Adicionar".</div>';
        return;
      }

      friends.forEach((f, idx) => {
        const isLive = liveFriendIds.has(f.id.toLowerCase());
        const item = document.createElement('div');
        item.className = 'friend-item';
        item.innerHTML = \`
          <div class="friend-info">
            <div class="avatar">\${f.name.charAt(0).toUpperCase()}<div class="status-dot"></div></div>
            <div>
              <div class="friend-name">\${f.name}</div>
              <div class="friend-id">\${f.id}</div>
            </div>
          </div>
          <div class="friend-actions">
            \${isLive ? \`<button class="btn-action live" onclick="requestWatch('\${f.id}')">🔴 AO VIVO</button>\` : \`<button class="btn-action watch" onclick="requestWatch('\${f.id}')">Assistir</button>\`}
            <button class="btn-action del" onclick="removeFriend(\${idx})">✕</button>
          </div>
        \`;
        container.appendChild(item);
      });
    }

    function requestWatch(friendId) {
      friendId = friendId.toLowerCase();
      addFriendToLocal(friendId, 'Amigo#' + friendId.slice(-4));
      ws.send(JSON.stringify({ type: 'REQUEST_STREAM', target: friendId }));
    }

    function copyMyId() {
      navigator.clipboard.writeText(myId).then(() => {
        alert("Seu ID copiado: " + myId + "\\nEnvie para seu amigo te adicionar!");
      });
    }

    function unmute() {
      videoEl.muted = false;
      unmuteNotice.style.display = 'none';
    }

    function toggleFullscreen() {
      if (!document.fullscreenElement) document.querySelector('.video-viewport').requestFullscreen();
      else document.exitFullscreen();
    }

    renderFriends();
  </script>
</body>
</html>
  `);
});

server.listen(PORT, () => {
  console.log('Servidor Discord ativo na porta: ' + PORT);
});