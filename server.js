const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;

// Gerenciamento de Salas em Memória
const rooms = new Map();

wss.on('connection', (ws) => {
  let currentRoom = null;
  let currentUserId = null;

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      // Entrar em uma sala
      if (data.type === 'JOIN') {
        currentRoom = data.room || 'geral';
        currentUserId = data.userId;

        if (!rooms.has(currentRoom)) {
          rooms.set(currentRoom, new Map());
        }
        rooms.get(currentRoom).set(currentUserId, ws);

        // Avisa aos outros que alguém entrou
        broadcastToRoom(currentRoom, currentUserId, {
          type: 'USER_JOINED',
          userId: currentUserId,
          nick: data.nick
        });
      }

      // Repasse de Sinais WebRTC (Fura CGNAT e roteia entre os dois)
      if (['OFFER', 'ANSWER', 'CANDIDATE', 'REQUEST_STREAM'].includes(data.type)) {
        const targetWs = rooms.get(currentRoom)?.get(data.target);
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(JSON.stringify({ ...data, from: currentUserId }));
        }
      }
    } catch (err) {
      console.error('Erro no processamento do WebSocket:', err);
    }
  });

  ws.on('close', () => {
    if (currentRoom && currentUserId && rooms.has(currentRoom)) {
      rooms.get(currentRoom).delete(currentUserId);
      broadcastToRoom(currentRoom, currentUserId, { type: 'USER_LEFT', userId: currentUserId });
      if (rooms.get(currentRoom).size === 0) rooms.delete(currentRoom);
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

// FRONTEND COMPLETO DO DISCORD (HTML + CSS + JS)
app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <title>Discord ScreenStream - Nuvem Global</title>
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
    .guild-bar { width: 72px; background: var(--bg-tertiary); display: flex; flex-direction: column; align-items: center; padding: 12px 0; border-right: 1px solid rgba(0,0,0,0.3); }
    .guild-icon { width: 48px; height: 48px; background: var(--discord-blurple); border-radius: 16px; display: flex; align-items: center; justify-content: center; color: white; font-weight: bold; cursor: pointer; }
    .sidebar { width: 260px; background: var(--bg-secondary); display: flex; flex-direction: column; }
    .sidebar-header { height: 48px; padding: 0 16px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid rgba(0,0,0,0.2); font-size: 15px; font-weight: bold; color: #fff; }
    .room-card { padding: 14px 16px; font-size: 13px; color: var(--text-muted); border-bottom: 1px solid rgba(0,0,0,0.1); }
    .room-card strong { color: #fff; }
    .btn-link { background: var(--discord-blurple); color: #fff; border: none; padding: 6px 12px; border-radius: 4px; font-size: 12px; font-weight: bold; cursor: pointer; margin-top: 8px; width: 100%; }
    .btn-link:hover { background: var(--discord-blurple-hover); }
    .user-panel { margin-top: auto; height: 60px; background: #1e1f22; display: flex; align-items: center; padding: 0 10px; gap: 10px; }
    .avatar { width: 34px; height: 34px; border-radius: 50%; background: var(--discord-blurple); display: flex; align-items: center; justify-content: center; font-weight: bold; color: #fff; }
    .main-stage { flex: 1; background: var(--bg-primary); display: flex; flex-direction: column; position: relative; }
    .top-bar { height: 48px; padding: 0 16px; display: flex; align-items: center; justify-content: space-between; background: var(--bg-secondary); border-bottom: 1px solid rgba(0,0,0,0.2); }
    .video-viewport { flex: 1; background: #000; display: flex; align-items: center; justify-content: center; position: relative; overflow: hidden; }
    video { width: 100%; height: 100%; object-fit: contain; }
    .empty-state { position: absolute; text-align: center; color: var(--text-muted); padding: 20px; }
    .control-dock { position: absolute; bottom: 24px; display: flex; gap: 10px; background: rgba(20,20,22,0.92); padding: 10px 20px; border-radius: 30px; backdrop-filter: blur(10px); }
    .btn-dock { background: #313338; color: #fff; border: none; padding: 10px 18px; border-radius: 20px; font-weight: bold; cursor: pointer; transition: 0.2s; font-size: 13px; }
    .btn-dock:hover { background: #3f4147; }
    .btn-dock.primary { background: var(--discord-blurple); }
    .btn-dock.danger { background: var(--discord-red); }
    #unmuteNotice { position: absolute; top: 20px; background: rgba(0,0,0,0.85); border: 1px solid #f0b232; color: #f0b232; padding: 8px 18px; border-radius: 20px; font-weight: bold; font-size: 12px; cursor: pointer; display: none; z-index: 10; }
  </style>
</head>
<body>
  <div class="guild-bar">
    <div class="guild-icon">DC</div>
  </div>

  <div class="sidebar">
    <div class="sidebar-header">Canal Global</div>
    <div class="room-card">
      Sala: <strong id="roomNameDisplay">...</strong><br/>
      Seu ID: <strong id="myIdDisplay">...</strong>
      <button class="btn-link" onclick="copyInviteLink()">📋 Copiar Link para Amigo</button>
    </div>
    <div class="user-panel">
      <div class="avatar" id="avatarLetter">U</div>
      <div>
        <div style="font-size: 13px; font-weight: bold; color: #fff;" id="myNickDisplay">...</div>
        <div style="font-size: 11px; color: var(--discord-green);">🟢 Conectado na Nuvem</div>
      </div>
    </div>
  </div>

  <div class="main-stage">
    <div class="top-bar">
      <span style="font-weight: bold; color: #fff;" id="stageTitle">Nenhuma transmissão em andamento</span>
      <span id="connStatus" style="font-size: 12px; color: var(--discord-green);">Nuvem HTTPS Ativa</span>
    </div>

    <div class="video-viewport">
      <div id="unmuteNotice" onclick="unmute()">🔊 Clique aqui para ativar o áudio</div>
      <video id="remoteVideo" autoplay playsinline muted></video>
      
      <div class="empty-state" id="emptyState">
        <h3 style="color: #fff;">Pronto para transmitir</h3>
        <p style="margin-top: 6px;">Compartilhe sua tela ou envie o link da sala para seu amigo entrar!</p>
      </div>

      <div class="control-dock">
        <button class="btn-dock primary" id="btnShare" onclick="toggleShare()">Transmitir Tela</button>
        <button class="btn-dock" onclick="toggleFullscreen()">Tela Cheia</button>
      </div>
    </div>
  </div>

  <script>
    // Configura Sala via URL (ex: ?sala=amigos)
    const urlParams = new URLSearchParams(window.location.search);
    const currentRoom = urlParams.get('sala') || 'sala-principal';
    const myId = 'user_' + Math.random().toString(36).substr(2, 6);
    const myNick = 'Gamer#' + Math.floor(1000 + Math.random() * 9000);

    document.getElementById('roomNameDisplay').innerText = currentRoom;
    document.getElementById('myIdDisplay').innerText = myId;
    document.getElementById('myNickDisplay').innerText = myNick;
    document.getElementById('avatarLetter').innerText = myNick.charAt(0);

    const videoEl = document.getElementById('remoteVideo');
    const emptyState = document.getElementById('emptyState');
    const stageTitle = document.getElementById('stageTitle');
    const btnShare = document.getElementById('btnShare');
    const unmuteNotice = document.getElementById('unmuteNotice');

    let localStream = null;
    let isSharing = false;
    let peerConnections = new Map();

    // Servidores STUN + TURN Relay (Garante a conexão furando o CGNAT do interior)
    const rtcConfig = {
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun.cloudflare.com:3478' },
        { urls: 'stun:openrelay.metered.ca:80' },
        { urls: 'turn:openrelay.metered.ca:80', username: 'openrelayproject', credential: 'openrelayproject' },
        { urls: 'turn:openrelay.metered.ca:443', username: 'openrelayproject', credential: 'openrelayproject' }
      ]
    };

    // Conexão WebSocket com o Servidor na Nuvem
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(protocol + '//' + location.host);

    ws.onopen = () => {
      ws.send(JSON.stringify({ type: 'JOIN', room: currentRoom, userId: myId, nick: myNick }));
    };

    ws.onmessage = async (event) => {
      const data = JSON.parse(event.data);

      if (data.type === 'USER_JOINED' && isSharing && localStream) {
        initiateCall(data.userId);
      }

      if (data.type === 'REQUEST_STREAM' && isSharing && localStream) {
        initiateCall(data.from);
      }

      if (data.type === 'OFFER') {
        handleOffer(data);
      }

      if (data.type === 'ANSWER') {
        const pc = peerConnections.get(data.from);
        if (pc) await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
      }

      if (data.type === 'CANDIDATE') {
        const pc = peerConnections.get(data.from);
        if (pc && pc.remoteDescription && data.candidate) {
          await pc.addIceCandidate(new RTCIceCandidate(data.candidate)).catch(console.warn);
        }
      }
    };

    async function toggleShare() {
      if (isSharing) {
        stopShare();
        return;
      }

      try {
        localStream = await navigator.mediaDevices.getDisplayMedia({
          video: { frameRate: { ideal: 60, max: 60 } },
          audio: true
        });

        isSharing = true;
        btnShare.innerText = 'Parar Transmissão';
        btnShare.classList.add('danger');
        stageTitle.innerText = 'Você está transmitindo sua tela';
        emptyState.style.display = 'none';
        videoEl.srcObject = localStream;
        videoEl.muted = true;
        videoEl.play();

        localStream.getVideoTracks()[0].onended = () => stopShare();

        ws.send(JSON.stringify({ type: 'REQUEST_STREAM', room: currentRoom }));

      } catch (err) {
        console.error('Captura cancelada:', err);
      }
    }

    function stopShare() {
      if (localStream) {
        localStream.getTracks().forEach(t => t.stop());
        localStream = null;
      }
      isSharing = false;
      btnShare.innerText = 'Transmitir Tela';
      btnShare.classList.remove('danger');
      stageTitle.innerText = 'Nenhuma transmissão em andamento';
      videoEl.srcObject = null;
      emptyState.style.display = 'block';
    }

    async function initiateCall(targetUserId) {
      const pc = new RTCPeerConnection(rtcConfig);
      peerConnections.set(targetUserId, pc);

      localStream.getTracks().forEach(track => pc.addTrack(track, localStream));

      pc.onicecandidate = (e) => {
        if (e.candidate) ws.send(JSON.stringify({ type: 'CANDIDATE', target: targetUserId, candidate: e.candidate }));
      };

      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);

      ws.send(JSON.stringify({ type: 'OFFER', target: targetUserId, sdp: pc.localDescription }));
    }

    async function handleOffer(data) {
      const pc = new RTCPeerConnection(rtcConfig);
      peerConnections.set(data.from, pc);

      pc.ontrack = (event) => {
        if (videoEl.srcObject !== event.streams[0]) {
          videoEl.srcObject = event.streams[0];
          videoEl.muted = true;
          videoEl.play().catch(console.warn);
          emptyState.style.display = 'none';
          stageTitle.innerText = 'Assistindo Tela de Amigo';
          unmuteNotice.style.display = 'block';
        }
      };

      pc.onicecandidate = (e) => {
        if (e.candidate) ws.send(JSON.stringify({ type: 'CANDIDATE', target: data.from, candidate: e.candidate }));
      };

      await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);

      ws.send(JSON.stringify({ type: 'ANSWER', target: data.from, sdp: pc.localDescription }));
    }

    function unmute() {
      videoEl.muted = false;
      unmuteNotice.style.display = 'none';
    }

    function copyInviteLink() {
      navigator.clipboard.writeText(window.location.href);
      alert('Link da sala copiado! Envie para o seu amigo entrar.');
    }

    function toggleFullscreen() {
      if (!document.fullscreenElement) document.querySelector('.video-viewport').requestFullscreen();
      else document.exitFullscreen();
    }
  </script>
</body>
</html>
  `);
});

server.listen(PORT, () => {
  console.log('Servidor em nuvem ativo na porta: ' + PORT);
});