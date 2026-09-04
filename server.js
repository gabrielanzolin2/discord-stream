const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;

// Gerenciamento de Conexões e Usuários
const rooms = new Map();
const activeUsers = new Map(); // [userId -> { ws, nick, isLive }]

wss.on('connection', (ws) => {
  let userRoom = 'sala-principal';
  let userId = null;
  let userNick = null;

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      // 1. Entrada de Usuário
      if (data.type === 'JOIN') {
        userId = data.userId;
        userNick = data.nick;
        userRoom = data.room || 'sala-principal';

        activeUsers.set(userId, { ws, nick: userNick, isLive: false });

        if (!rooms.has(userRoom)) {
          rooms.set(userRoom, new Map());
        }
        rooms.get(userRoom).set(userId, ws);

        // Notifica membros da sala
        broadcastToRoom(userRoom, userId, {
          type: 'USER_JOINED',
          userId,
          nick: userNick
        });

        // Envia lista de quem já está ao vivo na sala
        const liveUsers = [];
        for (const [id, client] of activeUsers) {
          if (client.isLive && id !== userId) {
            liveUsers.push({ userId: id, nick: client.nick });
          }
        }
        ws.send(JSON.stringify({ type: 'LIVE_LIST', liveUsers }));
      }

      // 2. Transmissão Iniciada / Encerrada
      if (data.type === 'LIVE_STATE_CHANGE') {
        if (activeUsers.has(userId)) {
          activeUsers.get(userId).isLive = data.isLive;
        }
        broadcastToRoom(userRoom, userId, {
          type: 'USER_LIVE_STATE',
          userId,
          nick: userNick,
          isLive: data.isLive
        });
      }

      // 3. Roteamento WebRTC (Offer, Answer, Candidates)
      if (['OFFER', 'ANSWER', 'CANDIDATE', 'REQUEST_STREAM'].includes(data.type)) {
        const targetClient = activeUsers.get(data.target);
        if (targetClient && targetClient.ws.readyState === WebSocket.OPEN) {
          targetClient.ws.send(JSON.stringify({ ...data, from: userId, fromNick: userNick }));
        }
      }
    } catch (err) {
      console.error('Erro na mensagem WebSocket:', err);
    }
  });

  ws.on('close', () => {
    if (userId) {
      activeUsers.delete(userId);
      if (rooms.has(userRoom)) {
        rooms.get(userRoom).delete(userId);
        broadcastToRoom(userRoom, userId, { type: 'USER_LEFT', userId });
        if (rooms.get(userRoom).size === 0) rooms.delete(userRoom);
      }
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

// FRONTEND COMPLETO COM ALTO BITRATE E SISTEMA DE AMIGOS
app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Discord ScreenStream - High Bitrate GPU</title>
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
      --text-header: #ffffff;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; user-select: none; }
    body { background: var(--bg-tertiary); color: var(--text-normal); height: 100vh; display: flex; overflow: hidden; }

    /* BARRA 1: SERVIDORES */
    .guild-bar { width: 72px; background: var(--bg-tertiary); display: flex; flex-direction: column; align-items: center; padding: 12px 0; border-right: 1px solid rgba(0,0,0,0.3); gap: 10px; }
    .guild-icon { width: 48px; height: 48px; background: var(--discord-blurple); border-radius: 16px; display: flex; align-items: center; justify-content: center; color: white; font-weight: bold; cursor: pointer; transition: 0.2s; }
    .guild-icon:hover { border-radius: 12px; filter: brightness(1.1); }

    /* BARRA 2: AMIGOS E CANAIS */
    .sidebar { width: 280px; background: var(--bg-secondary); display: flex; flex-direction: column; }
    .sidebar-header { height: 48px; padding: 0 16px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid rgba(0,0,0,0.2); font-size: 14px; font-weight: bold; color: #fff; }
    .btn-add-friend { background: var(--discord-green); color: #fff; border: none; padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; cursor: pointer; }
    .btn-add-friend:hover { filter: brightness(0.9); }

    .friends-list { flex: 1; overflow-y: auto; padding: 8px; display: flex; flex-direction: column; gap: 4px; }
    .section-title { font-size: 11px; font-weight: 700; color: var(--text-muted); text-transform: uppercase; padding: 8px 6px 4px 6px; letter-spacing: 0.5px; }

    .friend-item { display: flex; align-items: center; justify-content: space-between; padding: 8px; border-radius: 6px; background: rgba(0,0,0,0.1); transition: 0.15s; }
    .friend-item:hover { background: rgba(255,255,255,0.06); }
    .friend-info { display: flex; align-items: center; gap: 8px; overflow: hidden; }
    .avatar { width: 32px; height: 32px; border-radius: 50%; background: var(--discord-blurple); display: flex; align-items: center; justify-content: center; font-weight: bold; color: #fff; font-size: 13px; flex-shrink: 0; position: relative; }
    .status-dot { width: 9px; height: 9px; background: var(--discord-green); border-radius: 50%; position: absolute; bottom: 0; right: 0; border: 2px solid var(--bg-secondary); }
    .friend-name { font-size: 13px; font-weight: 600; color: var(--text-normal); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .friend-id { font-size: 11px; color: var(--text-muted); }

    .friend-actions { display: flex; gap: 4px; }
    .btn-action { background: #1e1f22; border: none; color: #fff; padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; cursor: pointer; transition: 0.2s; }
    .btn-action.watch { background: var(--discord-blurple); }
    .btn-action.watch:hover { background: var(--discord-blurple-hover); }
    .btn-action.del:hover { background: var(--discord-red); }

    /* CARD DO USUÁRIO */
    .user-panel { margin-top: auto; height: 60px; background: #1e1f22; display: flex; align-items: center; padding: 0 12px; gap: 10px; border-top: 1px solid rgba(0,0,0,0.2); }
    .user-tag { display: flex; flex-direction: column; overflow: hidden; flex: 1; }
    .user-tag .username { font-size: 13px; font-weight: bold; color: #fff; }
    .user-tag .peer-id { font-size: 11px; color: var(--discord-green); cursor: pointer; }
    .user-tag .peer-id:hover { text-decoration: underline; }

    /* PALCO PRINCIPAL (STREAM) */
    .main-stage { flex: 1; background: var(--bg-primary); display: flex; flex-direction: column; position: relative; }
    .top-bar { height: 48px; padding: 0 16px; display: flex; align-items: center; justify-content: space-between; background: var(--bg-secondary); border-bottom: 1px solid rgba(0,0,0,0.2); }
    .stream-info { display: flex; align-items: center; gap: 10px; font-size: 14px; font-weight: bold; color: #fff; }
    .badge-live { background: var(--discord-red); color: white; font-size: 10px; font-weight: 800; padding: 2px 6px; border-radius: 4px; display: none; }
    .badge-gpu { background: #232428; border: 1px solid var(--discord-green); color: var(--discord-green); font-size: 10px; font-weight: 700; padding: 2px 6px; border-radius: 4px; display: flex; align-items: center; gap: 4px; }

    /* PLAYER DE VÍDEO */
    .video-viewport { flex: 1; background: #000; display: flex; align-items: center; justify-content: center; position: relative; overflow: hidden; }
    video { width: 100%; height: 100%; object-fit: contain; transform: translateZ(0); will-change: transform; }

    .empty-state { position: absolute; text-align: center; color: var(--text-muted); }
    .empty-state svg { width: 72px; height: 72px; fill: #4e5058; margin-bottom: 12px; }

    /* CONTROLES FLUTUANTES */
    .control-dock { position: absolute; bottom: 24px; display: flex; align-items: center; gap: 10px; background: rgba(20,20,22,0.92); padding: 10px 20px; border-radius: 30px; backdrop-filter: blur(10px); }
    .btn-dock { background: #313338; color: #fff; border: none; padding: 10px 18px; border-radius: 20px; font-weight: bold; cursor: pointer; transition: 0.2s; font-size: 13px; display: flex; align-items: center; gap: 6px; }
    .btn-dock:hover { background: #3f4147; }
    .btn-dock.primary { background: var(--discord-blurple); }
    .btn-dock.danger { background: var(--discord-red); }

    .bitrate-selector { background: #1e1f22; color: #fff; border: 1px solid #333; padding: 8px 12px; border-radius: 16px; font-size: 12px; font-weight: bold; outline: none; cursor: pointer; }

    #unmuteNotice { position: absolute; top: 20px; background: rgba(0,0,0,0.85); border: 1px solid #f0b232; color: #f0b232; padding: 8px 18px; border-radius: 20px; font-weight: bold; font-size: 12px; cursor: pointer; display: none; z-index: 10; }
  </style>
</head>
<body>

  <!-- BARRA 1: SERVIDORES -->
  <div class="guild-bar">
    <div class="guild-icon" title="Discord Stream GPU">DC</div>
  </div>

  <!-- BARRA 2: AMIGOS -->
  <div class="sidebar">
    <div class="sidebar-header">
      <span>AMIGOS</span>
      <button class="btn-add-friend" onclick="promptAddFriend()">+ Adicionar</button>
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
        <span class="badge-gpu" id="bitrateStatus">⚡ 60 FPS • 8.0 Mbps GPU</span>
      </div>
      <span style="font-size: 12px; color: var(--discord-green);">🟢 Servidor em Nuvem Ativo</span>
    </div>

    <div class="video-viewport">
      <div id="unmuteNotice" onclick="unmute()">🔊 Clique aqui para ativar o áudio</div>
      <video id="remoteVideo" autoplay playsinline></video>
      
      <div class="empty-state" id="emptyState">
        <svg viewBox="0 0 24 24"><path d="M21 3H3c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h5v2h8v-2h5c1.1 0 1.99-.9 1.99-2L23 5c0-1.1-.9-2-2-2zm0 14H3V5h18v12z"/></svg>
        <h3 style="color: #fff;">Pronto para transmitir</h3>
        <p style="margin-top: 6px;">Transmita sua tela com aceleração por GPU e sem travamentos!</p>
      </div>

      <!-- DOCK FLUTUANTE -->
      <div class="control-dock">
        <!-- Controle de Bitrate / Puxar Internet -->
        <select class="bitrate-selector" id="bitrateSelect" onchange="updateBitrateSelection()">
          <option value="8000">🔥 Ultra Fluido (8.0 Mbps - 60 FPS)</option>
          <option value="5000">⚡ Equilibrado (5.0 Mbps - 60 FPS)</option>
          <option value="2500">🍃 Leve (2.5 Mbps - 30 FPS)</option>
        </select>

        <button class="btn-dock primary" id="btnShare" onclick="toggleShare()">Transmitir Tela</button>
        <button class="btn-dock danger" id="btnDisconnect" style="display: none;" onclick="disconnectStream()">Desconectar</button>
        <button class="btn-dock" onclick="toggleFullscreen()">Tela Cheia</button>
      </div>
    </div>
  </div>

  <script>
    // --- IDENTIDADE & SALA ---
    let myId = localStorage.getItem('dc_user_id');
    if (!myId) {
      myId = 'dc-' + Math.floor(10000 + Math.random() * 90000);
      localStorage.setItem('dc_user_id', myId);
    }
    let myNick = localStorage.getItem('dc_user_nick') || 'Gamer#' + myId.slice(-4);
    let friends = JSON.parse(localStorage.getItem('dc_saved_friends') || '[]');

    document.getElementById('myIdDisplay').innerText = myId + ' (Copiar ID)';
    document.getElementById('myNickDisplay').innerText = myNick;
    document.getElementById('avatarLetter').innerText = myNick.charAt(0).toUpperCase();

    const videoEl = document.getElementById('remoteVideo');
    const emptyState = document.getElementById('emptyState');
    const stageTitle = document.getElementById('stageTitle');
    const liveBadge = document.getElementById('liveBadge');
    const btnShare = document.getElementById('btnShare');
    const btnDisconnect = document.getElementById('btnDisconnect');
    const unmuteNotice = document.getElementById('unmuteNotice');

    let localStream = null;
    let isSharing = false;
    let activePC = null; // Conexão ativa como espectador
    let senderPCs = new Map(); // Conexões com amigos que estão me assistindo

    // Servidores STUN + TURN Relay (Anti-CGNAT para longa distância)
    const rtcConfig = {
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun.cloudflare.com:3478' },
        { urls: 'stun:openrelay.metered.ca:80' },
        { urls: 'turn:openrelay.metered.ca:80', username: 'openrelayproject', credential: 'openrelayproject' },
        { urls: 'turn:openrelay.metered.ca:443', username: 'openrelayproject', credential: 'openrelayproject' }
      ]
    };

    // --- CONEXÃO WEBSOCKET DO SERVIDOR ---
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(protocol + '//' + location.host);

    ws.onopen = () => {
      ws.send(JSON.stringify({ type: 'JOIN', userId: myId, nick: myNick, room: 'sala-principal' }));
    };

    ws.onmessage = async (event) => {
      const data = JSON.parse(event.data);

      // Amigo entrou ou solicitou tela
      if ((data.type === 'USER_JOINED' || data.type === 'REQUEST_STREAM') && isSharing && localStream) {
        initiateStreamToViewer(data.userId || data.from);
      }

      if (data.type === 'USER_LIVE_STATE') {
        renderFriends();
      }

      // WebRTC: Recebendo chamada de amigo
      if (data.type === 'OFFER') {
        handleIncomingOffer(data);
      }

      if (data.type === 'ANSWER') {
        const pc = senderPCs.get(data.from);
        if (pc) await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
      }

      if (data.type === 'CANDIDATE') {
        const pc = activePC || senderPCs.get(data.from);
        if (pc && pc.remoteDescription && data.candidate) {
          await pc.addIceCandidate(new RTCIceCandidate(data.candidate)).catch(console.warn);
        }
      }
    };

    // --- BOOST DE BITRATE E ACELERAÇÃO DE GPU NO SDP ---
    function boostSDP(sdp, bitrateKbps = 8000) {
      return sdp.replace(/a=mid:video\\r\\n/g, \`a=mid:video\\r\\nb=AS:\${bitrateKbps}\\r\\nb=TIAS:\${bitrateKbps * 1000}\\r\\n\`);
    }

    // --- TRANSMISSÃO DE ALTA PERFORMANCE (60 FPS + GPU) ---
    async function toggleShare() {
      if (isSharing) {
        stopShare();
        return;
      }

      try {
        localStream = await navigator.mediaDevices.getDisplayMedia({
          video: {
            frameRate: { ideal: 60, max: 60 },
            cursor: "always"
          },
          audio: true
        });

        // O SEGREDO DO 60 FPS SEM TRAVAR: Ativa prioridade de movimento para a GPU
        const videoTrack = localStream.getVideoTracks()[0];
        if (videoTrack && 'contentHint' in videoTrack) {
          videoTrack.contentHint = 'motion';
        }

        isSharing = true;
        btnShare.innerText = 'Parar Transmissão';
        btnShare.classList.add('danger');
        stageTitle.innerText = 'Você está transmitindo sua tela (60 FPS)';
        liveBadge.style.display = 'inline-block';
        emptyState.style.display = 'none';

        videoEl.srcObject = localStream;
        videoEl.muted = true;
        videoEl.play();

        videoTrack.onended = () => stopShare();

        // Notifica o servidor que fiquei AO VIVO
        ws.send(JSON.stringify({ type: 'LIVE_STATE_CHANGE', isLive: true }));

      } catch (err) {
        console.error('Falha ao iniciar tela:', err);
      }
    }

    function stopShare() {
      if (localStream) {
        localStream.getTracks().forEach(t => t.stop());
        localStream = null;
      }
      senderPCs.forEach(pc => pc.close());
      senderPCs.clear();

      isSharing = false;
      btnShare.innerText = 'Transmitir Tela';
      btnShare.classList.remove('danger');
      stageTitle.innerText = 'Nenhuma transmissão em andamento';
      liveBadge.style.display = 'none';
      videoEl.srcObject = null;
      emptyState.style.display = 'block';

      ws.send(JSON.stringify({ type: 'LIVE_STATE_CHANGE', isLive: false }));
    }

    // Envia a tela com Bitrate elevado para quem assistir
    async function initiateStreamToViewer(viewerId) {
      const pc = new RTCPeerConnection(rtcConfig);
      senderPCs.set(viewerId, pc);

      localStream.getTracks().forEach(track => {
        const sender = pc.addTrack(track, localStream);

        // Desbloqueia Bitrate de 8 Mbps na placa de vídeo
        if (track.kind === 'video' && sender.setParameters) {
          setTimeout(async () => {
            try {
              const params = sender.getParameters();
              if (!params.encodings) params.encodings = [{}];
              const selectedBitrate = parseInt(document.getElementById('bitrateSelect').value) * 1000;
              params.encodings[0].maxBitrate = selectedBitrate;
              params.encodings[0].networkPriority = 'high';
              params.degradationPreference = 'maintain-framerate';
              await sender.setParameters(params);
            } catch (e) { console.warn(e); }
          }, 500);
        }
      });

      pc.onicecandidate = (e) => {
        if (e.candidate) ws.send(JSON.stringify({ type: 'CANDIDATE', target: viewerId, candidate: e.candidate }));
      };

      const offer = await pc.createOffer();
      // Aplica o boost de banda na oferta SDP
      const boostedSdp = boostSDP(offer.sdp, parseInt(document.getElementById('bitrateSelect').value));
      await pc.setLocalDescription({ type: 'offer', sdp: boostedSdp });

      ws.send(JSON.stringify({ type: 'OFFER', target: viewerId, sdp: pc.localDescription }));
    }

    // Recebe a tela do amigo com decodificação rápida
    async function handleIncomingOffer(data) {
      if (activePC) activePC.close();
      activePC = new RTCPeerConnection(rtcConfig);

      activePC.ontrack = (event) => {
        if (videoEl.srcObject !== event.streams[0]) {
          videoEl.srcObject = event.streams[0];
          videoEl.muted = true;
          videoEl.play().catch(() => unmuteNotice.style.display = 'block');
          emptyState.style.display = 'none';
          stageTitle.innerText = \`Assistindo tela de \${data.fromNick || data.from}\`;
          liveBadge.style.display = 'inline-block';
          btnDisconnect.style.display = 'flex';
          unmuteNotice.style.display = 'block';
        }
      };

      activePC.onicecandidate = (e) => {
        if (e.candidate) ws.send(JSON.stringify({ type: 'CANDIDATE', target: data.from, candidate: e.candidate }));
      };

      await activePC.setRemoteDescription(new RTCSessionDescription(data.sdp));
      const answer = await activePC.createAnswer();
      const boostedAnswer = boostSDP(answer.sdp, parseInt(document.getElementById('bitrateSelect').value));
      await activePC.setLocalDescription({ type: 'answer', sdp: boostedAnswer });

      ws.send(JSON.stringify({ type: 'ANSWER', target: data.from, sdp: activePC.localDescription }));
    }

    function disconnectStream() {
      if (activePC) {
        activePC.close();
        activePC = null;
      }
      videoEl.srcObject = null;
      emptyState.style.display = 'block';
      stageTitle.innerText = 'Nenhuma transmissão em andamento';
      liveBadge.style.display = 'none';
      btnDisconnect.style.display = 'none';
      unmuteNotice.style.display = 'none';
    }

    // --- SISTEMA DE AMIGOS ---
    function renderFriends() {
      const container = document.getElementById('friendsContainer');
      container.innerHTML = '<div class="section-title">Lista de Amigos</div>';

      if (friends.length === 0) {
        container.innerHTML += '<div style="font-size: 12px; color: var(--text-muted); padding: 8px;">Nenhum amigo adicionado ainda. Clique em "+ Adicionar".</div>';
        return;
      }

      friends.forEach((f, idx) => {
        const item = document.createElement('div');
        item.className = 'friend-item';
        item.innerHTML = \`
          <div class="friend-info">
            <div class="avatar">\${f.name.charAt(0).toUpperCase()}</div>
            <div>
              <div class="friend-name">\${f.name}</div>
              <div class="friend-id">\${f.id}</div>
            </div>
          </div>
          <div class="friend-actions">
            <button class="btn-action watch" onclick="watchFriend('\${f.id}')" title="Assistir tela">📺 Assistir</button>
            <button class="btn-action del" onclick="removeFriend(\${idx})" title="Remover">✕</button>
          </div>
        \`;
        container.appendChild(item);
      });
    }

    function promptAddFriend() {
      const name = prompt("Nome do amigo (ex: Bruno):");
      if (!name) return;
      const id = prompt("Cole o ID do seu amigo (ex: dc-12345):");
      if (!id) return;

      friends.push({ name: name.trim(), id: id.trim() });
      localStorage.setItem('dc_saved_friends', JSON.stringify(friends));
      renderFriends();
    }

    function removeFriend(idx) {
      friends.splice(idx, 1);
      localStorage.setItem('dc_saved_friends', JSON.stringify(friends));
      renderFriends();
    }

    function watchFriend(friendId) {
      ws.send(JSON.stringify({ type: 'REQUEST_STREAM', target: friendId }));
    }

    function copyMyId() {
      navigator.clipboard.writeText(myId).then(() => {
        alert("Seu ID foi copiado: " + myId + "\\nEnvie para o seu amigo te adicionar!");
      });
    }

    function updateBitrateSelection() {
      const val = document.getElementById('bitrateSelect').value;
      document.getElementById('bitrateStatus').innerText = \`⚡ 60 FPS • \${(val/1000).toFixed(1)} Mbps GPU\`;
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
  console.log('Servidor em nuvem ativo na porta: ' + PORT);
});