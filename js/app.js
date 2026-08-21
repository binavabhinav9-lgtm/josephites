// Josephites Global Multi-Device Realtime Cloud Engine v3.5
let currentChannel = 'general';

// User Session State
let userRole = localStorage.getItem('josephites_role') || null; // 'NORMAL' or 'ADMIN'
let currentUserName = localStorage.getItem('josephites_name') || '';
let myUserId = localStorage.getItem('josephites_user_id') || '';
let blockedUsers = JSON.parse(localStorage.getItem('josephites_blocked') || '[]');

let isRecording = false;
let mediaRecorder = null;
let audioChunks = [];
let recordingInterval = null;
let recordingSeconds = 0;

// Members Directory (Dynamic - Only Real Joined Users)
let members = JSON.parse(localStorage.getItem('josephites_members') || '[]');

function saveMembers() {
  localStorage.setItem('josephites_members', JSON.stringify(members));
}

// Persistent Shared Message Storage
function getStoredMessages() {
  const data = localStorage.getItem('josephites_messages');
  if (data) {
    try {
      const parsed = JSON.parse(data);
      if (parsed && parsed.length > 0) return parsed;
    } catch (e) {}
  }
  return [
    {
      id: "m_welcome",
      channelId: "general",
      senderId: "system",
      senderName: "Josephites",
      content: "✨ Welcome to **Josephites**! Connect with fellow Josephites in real-time. All previously sent messages, events, and polls are saved here for all members.",
      timestamp: Date.now() - 3600000,
      type: "TEXT",
      reactions: [{ emoji: "🎓", count: 1 }, { emoji: "🚀", count: 1 }]
    }
  ];
}

let messages = getStoredMessages();

function saveStoredMessages() {
  localStorage.setItem('josephites_messages', JSON.stringify(messages));
}

// Channels Store
let channels = JSON.parse(localStorage.getItem('josephites_channels') || 'null') || [
  { id: "general", name: "general", topic: "Josephites Hub - Main Discussions" },
  { id: "academics", name: "academics", topic: "Study groups, Projects & Notes" },
  { id: "events", name: "events", topic: "Campus events, Meetups & Celebrations" },
  { id: "tech-club", name: "tech-club", topic: "Coding, AI & Innovations" },
  { id: "hostel-life", name: "hostel-life", topic: "Hostel announcements & community" }
];

function saveChannels() {
  localStorage.setItem('josephites_channels', JSON.stringify(channels));
}

// ----------------------------------------------------
// Global Cloud Real-Time MQTT WebSocket Broker
// ----------------------------------------------------
const MQTT_BROKER = "broker.emqx.io";
const MQTT_PORT = 8084; // WSS
const MQTT_PATH = "/mqtt";
const clientId = "josephites_dev_" + (myUserId || 'init') + "_" + Math.random().toString(36).substr(2, 4);

let mqttClient = null;

function initMqtt() {
  const statusBadge = document.getElementById('liveStatusBadge');
  const cloudStatus = document.getElementById('cloudSyncStatus');

  try {
    mqttClient = new Paho.MQTT.Client(MQTT_BROKER, MQTT_PORT, MQTT_PATH, clientId);

    mqttClient.onConnectionLost = responseObject => {
      console.warn("MQTT Connection Lost: ", responseObject.errorMessage);
      if (statusBadge) statusBadge.innerText = "🔄 Reconnecting...";
      if (cloudStatus) cloudStatus.innerText = "🟡 Connecting...";
      setTimeout(initMqtt, 3000);
    };

    mqttClient.onMessageArrived = message => {
      try {
        const payload = JSON.parse(message.payloadString);
        handleIncomingCloudEvent(payload);
      } catch (e) {
        console.error("Error parsing incoming event", e);
      }
    };

    mqttClient.connect({
      useSSL: true,
      timeout: 10,
      keepAliveInterval: 30,
      cleanSession: true,
      onSuccess: () => {
        console.log("Connected to Global Josephites Realtime Hub!");
        if (statusBadge) statusBadge.innerText = "⚡ Live Sync";
        if (cloudStatus) cloudStatus.innerText = "🟢 Global Cloud Synced";

        // Subscribe to global network
        mqttClient.subscribe("josephites/cloud/#", { qos: 1 });

        // Request global history from online peers and send presence
        requestGlobalHistory();
        sendPresencePing();
        setInterval(sendPresencePing, 20000);
      },
      onFailure: err => {
        console.error("MQTT Connect Failed: ", err);
        if (statusBadge) statusBadge.innerText = "⚠️ Offline";
        if (cloudStatus) cloudStatus.innerText = "🔴 Offline";
        setTimeout(initMqtt, 5000);
      }
    });
  } catch (err) {
    console.error("Failed to initialize MQTT client: ", err);
  }
}

function broadcastToCloud(topic, eventData) {
  if (mqttClient && mqttClient.isConnected()) {
    const msg = new Paho.MQTT.Message(JSON.stringify(eventData));
    msg.destinationName = topic;
    msg.qos = 1;
    mqttClient.send(msg);
  }
}

function requestGlobalHistory() {
  broadcastToCloud("josephites/cloud/history_sync", {
    type: 'REQUEST_HISTORY',
    senderId: myUserId
  });
}

function sendPresencePing() {
  if (!currentUserName || !myUserId) return;
  broadcastToCloud("josephites/cloud/presence", {
    type: 'PRESENCE_PING',
    member: {
      id: myUserId,
      name: currentUserName,
      role: userRole || 'NORMAL',
      isOnline: true,
      lastSeen: Date.now()
    }
  });
}

function handleIncomingCloudEvent(event) {
  if (!event || !event.type) return;

  if (event.type === 'NEW_MESSAGE') {
    const msg = event.message;
    if (msg.senderId === myUserId) return;

    if (!messages.find(m => m.id === msg.id)) {
      messages.push(msg);
      saveStoredMessages();

      if (msg.channelId === currentChannel) {
        appendMessage(msg);
        playBeep(880, 'sine', 0.12);
      }
    }
  } else if (event.type === 'REQUEST_HISTORY') {
    if (event.senderId !== myUserId && messages.length > 1) {
      broadcastToCloud("josephites/cloud/history_sync", {
        type: 'RESPONSE_HISTORY',
        targetId: event.senderId,
        messages: messages,
        channels: channels,
        members: members
      });
    }
  } else if (event.type === 'RESPONSE_HISTORY') {
    if (event.targetId === myUserId && event.messages && Array.isArray(event.messages)) {
      let added = false;
      event.messages.forEach(remoteMsg => {
        if (!messages.find(m => m.id === remoteMsg.id)) {
          messages.push(remoteMsg);
          added = true;
        }
      });
      if (added) {
        messages.sort((a, b) => a.timestamp - b.timestamp);
        saveStoredMessages();
        renderCurrentChat();
      }
      if (event.channels && Array.isArray(event.channels)) {
        event.channels.forEach(ch => {
          if (!channels.find(c => c.id === ch.id)) channels.push(ch);
        });
        saveChannels();
        renderChannels();
      }
      if (event.members && Array.isArray(event.members)) {
        event.members.forEach(mem => {
          if (!members.find(m => m.id === mem.id)) members.push(mem);
        });
        saveMembers();
        renderMembers();
      }
    }
  } else if (event.type === 'REACTION') {
    addReactionToDOM(event.messageId, event.emoji);
    const msg = messages.find(m => m.id === event.messageId);
    if (msg) {
      msg.reactions = msg.reactions || [];
      const r = msg.reactions.find(rx => rx.emoji === event.emoji);
      if (r) r.count++;
      else msg.reactions.push({ emoji: event.emoji, count: 1 });
      saveStoredMessages();
    }
  } else if (event.type === 'NEW_CHANNEL') {
    if (!channels.find(c => c.id === event.channel.id)) {
      channels.push(event.channel);
      saveChannels();
      renderChannels();
    }
  } else if (event.type === 'PUBLISH_EVENT') {
    displayEventBanner(event.eventData);
  } else if (event.type === 'BLOCK_USER') {
    if (event.block) {
      if (!blockedUsers.includes(event.userName.toLowerCase())) {
        blockedUsers.push(event.userName.toLowerCase());
      }
    } else {
      blockedUsers = blockedUsers.filter(u => u !== event.userName.toLowerCase());
    }
    localStorage.setItem('josephites_blocked', JSON.stringify(blockedUsers));
    checkBlockedStatus();
  } else if (event.type === 'REMOVE_MEMBER') {
    members = members.filter(m => m.id !== event.memberId && m.name.toLowerCase() !== event.userName.toLowerCase());
    saveMembers();
    renderMembers();
    if (myUserId === event.memberId || currentUserName.toLowerCase() === event.userName.toLowerCase()) {
      alert("You have been removed from this room by the administrator.");
      reauth();
    }
  } else if (event.type === 'POLL_VOTE') {
    handleRemotePollVote(event.messageId, event.optionIndex);
  } else if (event.type === 'PRESENCE_PING') {
    handlePresencePing(event.member);
  } else if (event.type === 'EDIT_MEMBER') {
    handleRemoteEditMember(event.member);
  }
}

function handlePresencePing(newMember) {
  if (!newMember || !newMember.id || !newMember.name) return;
  let existing = members.find(m => m.id === newMember.id || m.name.toLowerCase() === newMember.name.toLowerCase());
  if (existing) {
    existing.id = newMember.id;
    existing.name = newMember.name;
    existing.role = newMember.role;
    existing.isOnline = true;
    existing.lastSeen = Date.now();
  } else {
    members.push({
      id: newMember.id,
      name: newMember.name,
      role: newMember.role,
      isOnline: true,
      lastSeen: Date.now()
    });
  }
  saveMembers();
  renderMembers();
}

function handleRemoteEditMember(updatedMember) {
  let existing = members.find(m => m.id === updatedMember.id);
  if (existing) {
    existing.name = updatedMember.name;
    saveMembers();
    renderMembers();
    if (myUserId === updatedMember.id) {
      currentUserName = updatedMember.name;
      localStorage.setItem('josephites_name', currentUserName);
      updateProfileUI();
    }
  }
}

// Sound Synthesis (Web Audio)
function playBeep(freq = 440, type = 'sine', duration = 0.08) {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = type;
    osc.frequency.setValueAtTime(freq, ctx.currentTime);
    gain.gain.setValueAtTime(0.1, ctx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + duration);
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.start();
    osc.stop(ctx.currentTime + duration);
  } catch (e) {}
}

function playWelcomeChord() {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const notes = [261.63, 329.63, 392.00, 523.25]; // C major chord
    notes.forEach((freq, idx) => {
      setTimeout(() => {
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.type = 'sine';
        osc.frequency.setValueAtTime(freq, ctx.currentTime);
        gain.gain.setValueAtTime(0.06, ctx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + 1.2);
        osc.connect(gain);
        gain.connect(ctx.destination);
        osc.start();
        osc.stop(ctx.currentTime + 1.2);
      }, idx * 90);
    });
  } catch (e) {}
}

// DOM Elements
const introOverlay = document.getElementById('introOverlay');
const introImg = document.getElementById('introImg');
const authModalBackdrop = document.getElementById('authModalBackdrop');
const inputUsername = document.getElementById('inputUsername');
const inputPasscode = document.getElementById('inputPasscode');
const authError = document.getElementById('authError');
const adminModalBackdrop = document.getElementById('adminModalBackdrop');
const adminConsoleBtn = document.getElementById('adminConsoleBtn');
const adminHeaderBtn = document.getElementById('adminHeaderBtn');
const adminAddChannelBtn = document.getElementById('adminAddChannelBtn');
const sidebar = document.getElementById('sidebar');
const sidebarOverlay = document.getElementById('sidebarOverlay');
const messagesContainer = document.getElementById('messagesContainer');
const messageInput = document.getElementById('messageInput');
const sendBtn = document.getElementById('sendBtn');
const micBtn = document.getElementById('micBtn');
const channelTitle = document.getElementById('channelTitle');
const channelTopic = document.getElementById('channelTopic');
const channelsList = document.getElementById('channelsList');
const membersList = document.getElementById('membersList');
const memberCount = document.getElementById('memberCount');
const recordingBar = document.getElementById('recordingBar');
const recordingTimer = document.getElementById('recordingTimer');
const profileName = document.getElementById('profileName');
const userAvatar = document.getElementById('userAvatar');
const eventBannerContainer = document.getElementById('eventBannerContainer');

// ========================================================
// DYNAMIC PARTICLE SYSTEM ON CANVAS
// ========================================================
function initIntroCanvas() {
  const canvas = document.getElementById('introCanvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  
  function resize() {
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
  }
  resize();
  window.addEventListener('resize', resize);

  const particles = [];
  const count = Math.min(45, Math.floor(window.innerWidth / 20));

  for (let i = 0; i < count; i++) {
    particles.push({
      x: Math.random() * canvas.width,
      y: Math.random() * canvas.height,
      radius: Math.random() * 2.2 + 0.8,
      speedX: (Math.random() - 0.5) * 0.4,
      speedY: -Math.random() * 0.5 - 0.2,
      alpha: Math.random() * 0.7 + 0.3,
      alphaSpeed: (Math.random() * 0.02 + 0.005) * (Math.random() > 0.5 ? 1 : -1),
      hue: Math.random() > 0.4 ? 45 : 230 // Gold & Indigo ambient sparks
    });
  }

  function render() {
    if (introOverlay.classList.contains('fade-out')) return;
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    particles.forEach(p => {
      p.x += p.speedX;
      p.y += p.speedY;
      p.alpha += p.alphaSpeed;

      if (p.alpha <= 0.2 || p.alpha >= 0.9) p.alphaSpeed *= -1;
      if (p.y < -10) { p.y = canvas.height + 10; p.x = Math.random() * canvas.width; }
      if (p.x < -10) p.x = canvas.width + 10;
      if (p.x > canvas.width + 10) p.x = -10;

      ctx.beginPath();
      ctx.arc(p.x, p.y, p.radius, 0, Math.PI * 2);
      ctx.fillStyle = `hsla(${p.hue}, 90%, 75%, ${p.alpha})`;
      ctx.shadowBlur = 12;
      ctx.shadowColor = `hsla(${p.hue}, 100%, 70%, 0.8)`;
      ctx.fill();
    });

    requestAnimationFrame(render);
  }
  render();

  // 3D Tilt Parallax on mouse / device
  window.addEventListener('mousemove', e => {
    if (!introImg || introOverlay.classList.contains('fade-out')) return;
    const moveX = (e.clientX - window.innerWidth / 2) * 0.02;
    const moveY = (e.clientY - window.innerHeight / 2) * 0.02;
    introImg.style.transform = `scale(1.1) translate(${moveX}px, ${moveY}px)`;
  });
}

// ========================================================
// INTRO & AUTH FLOW
// ========================================================
function dismissIntro() {
  playWelcomeChord();
  introOverlay.classList.add('fade-out');
  if (!currentUserName || !userRole) {
    setTimeout(showAuthModal, 300);
  }
}

function showAuthModal() {
  authModalBackdrop.classList.remove('hidden');
  inputUsername.value = currentUserName || '';
  inputPasscode.value = '';
  authError.innerText = '';
}

function submitAuth() {
  const name = inputUsername.value.trim();
  const passcode = inputPasscode.value.trim().toUpperCase();

  if (!name) {
    authError.innerText = "Please enter your name.";
    return;
  }

  if (passcode === "ADMINZERO") {
    userRole = "ADMIN";
  } else if (passcode === "STJOSEPH") {
    userRole = "NORMAL";
  } else {
    authError.innerText = "Invalid Passcode. Please check and try again.";
    playBeep(220, 'sawtooth', 0.2);
    return;
  }

  currentUserName = name;
  if (!myUserId) {
    myUserId = 'u_' + Math.random().toString(36).substr(2, 9);
    localStorage.setItem('josephites_user_id', myUserId);
  }

  localStorage.setItem('josephites_name', currentUserName);
  localStorage.setItem('josephites_role', userRole);

  authModalBackdrop.classList.add('hidden');
  playBeep(784, 'triangle', 0.15);

  handlePresencePing({
    id: myUserId,
    name: currentUserName,
    role: userRole,
    isOnline: true,
    lastSeen: Date.now()
  });

  applyUserRoleUI();
  updateProfileUI();
  renderMembers();
  renderCurrentChat();
  checkBlockedStatus();
  sendPresencePing();
  requestGlobalHistory();
}

function reauth() {
  localStorage.removeItem('josephites_role');
  showAuthModal();
}

function applyUserRoleUI() {
  const isAdmin = userRole === 'ADMIN';
  if (adminConsoleBtn) adminConsoleBtn.style.display = isAdmin ? 'inline-block' : 'none';
  if (adminHeaderBtn) adminHeaderBtn.style.display = isAdmin ? 'inline-block' : 'none';
  if (adminAddChannelBtn) adminAddChannelBtn.style.display = isAdmin ? 'inline' : 'none';
}

function checkBlockedStatus() {
  const isMuted = blockedUsers.includes((currentUserName || '').toLowerCase());
  if (isMuted) {
    messageInput.disabled = true;
    messageInput.placeholder = "🚫 You are muted by admin in this room.";
    sendBtn.disabled = true;
    micBtn.disabled = true;
  } else {
    messageInput.disabled = false;
    messageInput.placeholder = "Message Josephites or mention @josephite...";
    sendBtn.disabled = false;
    micBtn.disabled = false;
  }
}

// ========================================================
// MEMBER LIST RENDERING & ADMIN ACTIONS
// ========================================================
function renderMembers() {
  if (!membersList) return;
  const isAdmin = userRole === 'ADMIN';
  if (memberCount) memberCount.innerText = members.length;

  if (members.length === 0) {
    membersList.innerHTML = '<div style="padding: 8px 16px; font-size: 12px; color: var(--text-muted);">No members currently online</div>';
    return;
  }

  membersList.innerHTML = members.map(m => {
    const showAdminBadge = isAdmin && m.role === 'ADMIN';
    const isMe = m.id === myUserId || m.name === currentUserName;

    return `
      <div class="member-item-row" onclick="${isAdmin && !isMe ? `adminManageMember('${m.id}', '${m.name}', '${m.role}')` : ''}" style="${isAdmin && !isMe ? 'cursor: pointer;' : 'cursor: default;'}">
        <div class="member-info">
          <div class="avatar" style="width: 28px; height: 28px; font-size: 11px;">
            ${(m.name || 'J')[0].toUpperCase()}
            <div class="status-dot ${m.isOnline ? '' : 'offline'}"></div>
          </div>
          <div>
            <div style="font-size: 13px; font-weight: 600; color: var(--text-primary); display: flex; align-items: center;">
              <span>${m.name} ${isMe ? '<span style="font-size: 10px; color: var(--text-muted);">(You)</span>' : ''}</span>
              ${showAdminBadge ? '<span class="admin-stealth-badge">🛡️ ADMIN</span>' : ''}
            </div>
            <div style="font-size: 10px; color: ${m.isOnline ? 'var(--online)' : 'var(--offline)'};">
              ${m.isOnline ? 'Online' : 'Offline'}
            </div>
          </div>
        </div>
        ${isAdmin && !isMe ? `
          <div class="member-admin-actions">
            <button class="member-action-chip" onclick="event.stopPropagation(); adminManageMember('${m.id}', '${m.name}', '${m.role}')">Manage</button>
          </div>
        ` : ''}
      </div>
    `;
  }).join('');
}

function adminManageMember(memId, memName, memRole) {
  if (userRole !== 'ADMIN') return;
  const isCurrentlyBlocked = blockedUsers.includes(memName.toLowerCase());

  const choice = prompt(
    `⚙️ Manage Member: "${memName}"\n\n` +
    `1: Remove / Kick Member\n` +
    `2: ${isCurrentlyBlocked ? 'Unblock / Unmute' : 'Block / Mute from Chat'}\n` +
    `3: Change Display Name\n\n` +
    `Enter choice (1, 2, or 3):`, "1"
  );

  if (choice === "1") {
    if (confirm(`Are you sure you want to remove member "${memName}" from Josephites?`)) {
      members = members.filter(m => m.id !== memId && m.name.toLowerCase() !== memName.toLowerCase());
      saveMembers();
      renderMembers();
      broadcastToCloud("josephites/cloud/moderation", {
        type: 'REMOVE_MEMBER',
        memberId: memId,
        userName: memName
      });
      alert(`Member "${memName}" removed.`);
    }
  } else if (choice === "2") {
    adminBlockUserDirect(memName, !isCurrentlyBlocked);
  } else if (choice === "3") {
    const newName = prompt(`Enter new display name for "${memName}":`, memName);
    if (newName && newName.trim()) {
      const updated = { id: memId, name: newName.trim() };
      handleRemoteEditMember(updated);
      broadcastToCloud("josephites/cloud/member_edit", { type: 'EDIT_MEMBER', member: updated });
    }
  }
}

function adminBlockUserDirect(name, block) {
  if (block) {
    if (!blockedUsers.includes(name.toLowerCase())) blockedUsers.push(name.toLowerCase());
  } else {
    blockedUsers = blockedUsers.filter(u => u !== name.toLowerCase());
  }
  localStorage.setItem('josephites_blocked', JSON.stringify(blockedUsers));
  broadcastToCloud("josephites/cloud/moderation", { type: 'BLOCK_USER', userName: name, block: block });
  alert(`User "${name}" has been ${block ? 'blocked/muted' : 'unblocked'}.`);
  renderMembers();
}

// ========================================================
// ADMIN POWERS
// ========================================================
function toggleAdminModal() {
  if (userRole !== 'ADMIN') return;
  adminModalBackdrop.classList.toggle('hidden');
}

function adminCreateChannel() {
  const name = document.getElementById('newChanName').value.trim().toLowerCase().replace(/\s+/g, '-');
  const topic = document.getElementById('newChanTopic').value.trim();
  if (!name) return alert("Channel name required");

  const newChan = { id: name, name: name, topic: topic || "Josephites Discussion" };
  channels.push(newChan);
  saveChannels();
  renderChannels();
  broadcastToCloud("josephites/cloud/channel", { type: 'NEW_CHANNEL', channel: newChan });
  toggleAdminModal();
  selectChannel(name, name, topic);
  alert(`Channel #${name} created!`);
}

function adminPublishEvent() {
  const title = document.getElementById('eventTitle').value.trim();
  const desc = document.getElementById('eventDesc').value.trim();
  if (!title) return alert("Event title required");

  const eventData = { title: title, desc: desc, id: 'ev_' + Date.now() };
  displayEventBanner(eventData);
  broadcastToCloud("josephites/cloud/event", { type: 'PUBLISH_EVENT', eventData: eventData });
  toggleAdminModal();
  alert("Official event banner published!");
}

function displayEventBanner(eventData) {
  eventBannerContainer.innerHTML = `
    <div class="event-banner">
      <div class="event-icon">📢</div>
      <div class="event-content" style="flex: 1;">
        <h4>${eventData.title}</h4>
        <p>${eventData.desc}</p>
      </div>
      <button onclick="this.parentElement.remove()" style="background: none; border: none; color: var(--text-muted); cursor: pointer;">✕</button>
    </div>
  `;
}

function adminCreatePoll() {
  const question = document.getElementById('pollQuestion').value.trim();
  const rawOpts = document.getElementById('pollOptions').value.trim();
  if (!question || !rawOpts) return alert("Question and options required");

  const options = rawOpts.split(',').map(o => ({ text: o.trim(), votes: 0 }));
  const pollMsg = {
    id: 'poll_' + Date.now(),
    channelId: currentChannel,
    senderId: myUserId,
    senderName: currentUserName,
    content: `📊 **POLL:** ${question}`,
    timestamp: Date.now(),
    type: 'POLL',
    pollData: { question: question, options: options, voters: [] },
    isBot: false,
    reactions: []
  };

  messages.push(pollMsg);
  saveStoredMessages();
  appendMessage(pollMsg);
  broadcastToCloud("josephites/cloud/channel/" + currentChannel, { type: 'NEW_MESSAGE', message: pollMsg });
  toggleAdminModal();
}

function adminBlockUser(block) {
  const name = document.getElementById('blockUserName').value.trim();
  if (!name) return alert("Enter username");
  adminBlockUserDirect(name, block);
  toggleAdminModal();
}

// Voting on Polls
function votePoll(msgId, optIndex) {
  const msg = messages.find(m => m.id === msgId);
  if (!msg || !msg.pollData) return;

  msg.pollData.voters = msg.pollData.voters || [];
  if (msg.pollData.voters.includes(myUserId)) {
    return alert("You have already voted on this poll.");
  }

  msg.pollData.voters.push(myUserId);
  msg.pollData.options[optIndex].votes = (msg.pollData.options[optIndex].votes || 0) + 1;
  saveStoredMessages();
  renderCurrentChat();
  playBeep(700, 'sine', 0.08);

  broadcastToCloud("josephites/cloud/poll_vote", { type: 'POLL_VOTE', messageId: msgId, optionIndex: optIndex });
}

function handleRemotePollVote(msgId, optIndex) {
  const msg = messages.find(m => m.id === msgId);
  if (msg && msg.pollData && msg.pollData.options[optIndex]) {
    msg.pollData.options[optIndex].votes = (msg.pollData.options[optIndex].votes || 0) + 1;
    saveStoredMessages();
    renderCurrentChat();
  }
}

// Photo Upload Handler
function handleImageUpload(e) {
  const file = e.target.files[0];
  if (!file) return;

  const reader = new FileReader();
  reader.onload = function(evt) {
    const base64Data = evt.target.result;
    const msg = {
      id: 'img_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5),
      channelId: currentChannel,
      senderId: myUserId,
      senderName: currentUserName,
      content: "",
      imageUrl: base64Data,
      timestamp: Date.now(),
      type: 'IMAGE',
      isBot: false,
      reactions: []
    };

    messages.push(msg);
    saveStoredMessages();
    appendMessage(msg);
    playBeep(660, 'triangle', 0.08);

    if (currentChannel) {
      broadcastToCloud("josephites/cloud/channel/" + currentChannel, { type: 'NEW_MESSAGE', message: msg });
    }
  };
  reader.readAsDataURL(file);
}

// ========================================================
// CORE CHAT & UI
// ========================================================
function toggleSidebar() {
  sidebar.classList.toggle('open');
  sidebarOverlay.classList.toggle('active');
}

function updateProfileUI() {
  profileName.innerText = `${currentUserName || 'Guest'} (You)`;
  userAvatar.innerText = (currentUserName || 'J')[0].toUpperCase();
}

function initApp() {
  initIntroCanvas();
  updateProfileUI();
  applyUserRoleUI();
  renderChannels();
  renderMembers();
  renderCurrentChat();
  initMqtt();

  if (currentUserName && userRole) {
    setTimeout(() => {
      introOverlay.classList.add('fade-out');
    }, 2800);
  }
}

function renderChannels() {
  channelsList.innerHTML = channels.map(ch => `
    <div class="item-row ${ch.id === currentChannel ? 'active' : ''}" onclick="selectChannel('${ch.id}', '${ch.name}', '${ch.topic}')">
      <span>#</span>
      <span>${ch.name}</span>
    </div>
  `).join('');
}

function selectChannel(id, name, topic) {
  currentChannel = id;
  channelTitle.innerText = '#' + name;
  channelTopic.innerText = topic;
  renderChannels();
  renderMembers();
  renderCurrentChat();
  if (window.innerWidth <= 768 && sidebar.classList.contains('open')) {
    toggleSidebar();
  }
}

function renderCurrentChat() {
  messagesContainer.innerHTML = '';
  const filtered = messages.filter(m => m.channelId === currentChannel);
  filtered.forEach(m => appendMessage(m, false));
  scrollToBottom();
}

function appendMessage(msg, scroll = true) {
  const isSent = msg.senderId === myUserId || msg.senderName === currentUserName;
  const row = document.createElement('div');
  row.className = `message-row ${isSent ? 'sent' : 'received'}`;
  row.id = `msg-${msg.id}`;

  let contentHtml = '';
  if (msg.type === 'VOICE' && msg.voiceData) {
    contentHtml = `
      <div class="voice-player">
        <button class="voice-btn" onclick="playVoiceNote('${msg.id}', '${msg.voiceData}')">▶</button>
        <div class="voice-wave"><div class="voice-progress" id="vp-${msg.id}"></div></div>
        <span style="font-size: 11px;">${msg.voiceDuration || 3}s</span>
      </div>
    `;
  } else if (msg.type === 'IMAGE' && msg.imageUrl) {
    contentHtml = `
      <div>
        <img src="${msg.imageUrl}" alt="Uploaded photo" style="max-width: 100%; border-radius: 12px; margin-bottom: 4px; max-height: 260px; object-fit: cover;">
      </div>
    `;
  } else if (msg.type === 'POLL' && msg.pollData) {
    const totalVotes = msg.pollData.options.reduce((sum, opt) => sum + (opt.votes || 0), 0);
    contentHtml = `
      <div class="poll-card">
        <div class="poll-question">📊 ${msg.pollData.question}</div>
        <div>
          ${msg.pollData.options.map((opt, idx) => {
            const pct = totalVotes > 0 ? Math.round(((opt.votes || 0) / totalVotes) * 100) : 0;
            return `
              <button class="poll-option-btn" onclick="votePoll('${msg.id}', ${idx})">
                <span>${opt.text}</span>
                <span style="font-weight: 700; color: #a78bfa;">${opt.votes || 0} (${pct}%)</span>
              </button>
            `;
          }).join('')}
        </div>
        <div style="font-size: 11px; color: var(--text-muted); margin-top: 4px;">Total votes: ${totalVotes}</div>
      </div>
    `;
  } else {
    let formatted = (msg.content || '')
      .replace(/\*\*(.*?)\*\*/g, '<b>$1</b>')
      .replace(/`(.*?)`/g, '<code style="background: rgba(0,0,0,0.3); padding: 2px 4px; border-radius: 4px; color: #a78bfa;">$1</code>');
    contentHtml = `<div>${formatted}</div>`;
  }

  const timeStr = new Date(msg.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

  row.innerHTML = `
    ${!isSent ? `<div class="avatar">${(msg.senderName || 'J')[0].toUpperCase()}</div>` : ''}
    <div class="message-bubble">
      ${!isSent ? `<div class="sender-name">${msg.senderName} ${msg.isBot ? '<span class="bot-tag">AI BOT</span>' : ''}</div>` : ''}
      ${contentHtml}
      <div class="msg-time">${timeStr}</div>
      <div class="reactions-bar" id="rx-${msg.id}">
        ${(msg.reactions || []).map(r => `<span class="reaction-pill" onclick="reactToMessage('${msg.id}', '${r.emoji}')">${r.emoji} ${r.count}</span>`).join('')}
      </div>
    </div>
  `;

  messagesContainer.appendChild(row);
  if (scroll) scrollToBottom();
}

function scrollToBottom() {
  messagesContainer.scrollTop = messagesContainer.scrollHeight;
}

// Sending Messages
async function sendMessage() {
  if (blockedUsers.includes((currentUserName || '').toLowerCase())) {
    return alert("You are muted in this room.");
  }

  const text = messageInput.value.trim();
  if (!text) return;

  const msg = {
    id: 'm_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5),
    channelId: currentChannel,
    senderId: myUserId,
    senderName: currentUserName,
    content: text,
    timestamp: Date.now(),
    type: 'TEXT',
    voiceData: null,
    voiceDuration: 0,
    isBot: false,
    reactions: []
  };

  messageInput.value = '';
  messages.push(msg);
  saveStoredMessages();
  appendMessage(msg);
  playBeep(660, 'triangle', 0.08);

  if (currentChannel) {
    broadcastToCloud("josephites/cloud/channel/" + currentChannel, { type: 'NEW_MESSAGE', message: msg });
  }

  if (text.toLowerCase().includes('@josephite')) {
    generateAiResponse(text);
  }
}

// Embedded Cloud AI Assistant
async function generateAiResponse(prompt) {
  const typingBanner = document.createElement('div');
  typingBanner.id = 'ai-typing';
  typingBanner.style.cssText = 'font-size: 12px; color: var(--secondary); margin-left: 8px;';
  typingBanner.innerText = 'Josephite AI is typing...';
  messagesContainer.appendChild(typingBanner);
  scrollToBottom();

  setTimeout(() => {
    typingBanner.remove();
    let reply = "";
    const clean = prompt.toLowerCase().trim();

    if (clean.includes("help") || clean.includes("what can you do")) {
      reply = "🤖 **I am Josephite AI:**\n• 💬 Chat with fellow Josephites in real-time\n• 📱 Live sync & shared message history across all phones\n• 🎙️ Voice notes playback\n• 📷 Photo sharing & Community Polls\n• 📝 Study notes, summaries & project help";
    } else if (clean.includes("iphone") || clean.includes("install") || clean.includes("pwa")) {
      reply = "📱 **Install on iPhone / Android:**\n1. In Safari / Chrome, tap **Share / Menu**.\n2. Tap **Add to Home Screen**.\n3. Open Josephites in full screen!";
    } else if (clean.includes("summary") || clean.includes("summarize")) {
      reply = "📋 **Summary:** The Josephites community network is active with real-time multi-device messaging and voice notes.";
    } else {
      reply = `Hello ${currentUserName}! I'm Josephite AI. How can I help you today? 😊`;
    }

    const aiMsg = {
      id: 'ai_' + Date.now(),
      channelId: currentChannel,
      senderId: 'system_ai',
      senderName: 'Josephite AI',
      content: reply,
      timestamp: Date.now(),
      type: 'TEXT',
      isBot: true,
      reactions: [{ emoji: '✨', count: 1 }]
    };

    messages.push(aiMsg);
    saveStoredMessages();
    appendMessage(aiMsg);
    playBeep(880, 'sine', 0.12);
  }, 1000);
}

// Voice Note Recording
async function startVoiceRecording() {
  if (blockedUsers.includes((currentUserName || '').toLowerCase())) {
    return alert("You are muted in this room.");
  }
  try {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    mediaRecorder = new MediaRecorder(stream);
    audioChunks = [];

    mediaRecorder.ondataavailable = e => audioChunks.push(e.data);
    mediaRecorder.onstop = async () => {
      const audioBlob = new Blob(audioChunks, { type: 'audio/mp4' });
      const reader = new FileReader();
      reader.readAsDataURL(audioBlob);
      reader.onloadend = () => {
        sendVoiceMessage(reader.result, recordingSeconds);
      };
      stream.getTracks().forEach(track => track.stop());
    };

    mediaRecorder.start();
    isRecording = true;
    recordingSeconds = 0;
    recordingBar.classList.remove('hidden');
    recordingInterval = setInterval(() => {
      recordingSeconds++;
      recordingTimer.innerText = `Recording 0:0${recordingSeconds}`.slice(-4);
    }, 1000);
    playBeep(520, 'sine', 0.05);
  } catch (err) {
    alert("Microphone permission required for voice notes.");
  }
}

function stopVoiceRecording() {
  if (!isRecording) return;
  clearInterval(recordingInterval);
  recordingBar.classList.add('hidden');
  isRecording = false;
  mediaRecorder.stop();
}

function cancelVoiceRecording() {
  if (!isRecording) return;
  clearInterval(recordingInterval);
  recordingBar.classList.add('hidden');
  isRecording = false;
  if (mediaRecorder && mediaRecorder.state !== 'inactive') {
    mediaRecorder.ondataavailable = null;
    mediaRecorder.stop();
  }
}

function sendVoiceMessage(base64Audio, duration) {
  const msg = {
    id: 'v_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5),
    channelId: currentChannel,
    senderId: myUserId,
    senderName: currentUserName,
    content: "🎙️ Voice Note",
    timestamp: Date.now(),
    type: 'VOICE',
    voiceData: base64Audio,
    voiceDuration: Math.max(1, duration),
    isBot: false,
    reactions: []
  };

  messages.push(msg);
  saveStoredMessages();
  appendMessage(msg);
  playBeep(660, 'triangle', 0.08);

  if (currentChannel) {
    broadcastToCloud("josephites/cloud/channel/" + currentChannel, { type: 'NEW_MESSAGE', message: msg });
  }
}

function playVoiceNote(id, audioSrc) {
  const audio = new Audio(audioSrc);
  const progressEl = document.getElementById(`vp-${id}`);
  audio.ontimeupdate = () => {
    if (progressEl) {
      const pct = (audio.currentTime / audio.duration) * 100;
      progressEl.style.width = pct + '%';
    }
  };
  audio.onended = () => {
    if (progressEl) progressEl.style.width = '0%';
  };
  audio.play();
}

// Reactions
function reactToMessage(msgId, emoji) {
  addReactionToDOM(msgId, emoji);
  const msg = messages.find(m => m.id === msgId);
  if (msg) {
    msg.reactions = msg.reactions || [];
    const r = msg.reactions.find(rx => rx.emoji === emoji);
    if (r) r.count++;
    else msg.reactions.push({ emoji: emoji, count: 1 });
    saveStoredMessages();
  }
  if (currentChannel) {
    broadcastToCloud("josephites/cloud/channel/" + currentChannel, { type: 'REACTION', messageId: msgId, emoji: emoji });
  }
}

function addReactionToDOM(msgId, emoji) {
  const container = document.getElementById(`rx-${msgId}`);
  if (!container) return;
  const existing = Array.from(container.children).find(c => c.innerText.includes(emoji));
  if (existing) {
    const parts = existing.innerText.split(' ');
    existing.innerText = `${emoji} ${parseInt(parts[1] || 1) + 1}`;
  } else {
    const pill = document.createElement('span');
    pill.className = 'reaction-pill';
    pill.innerText = `${emoji} 1`;
    container.appendChild(pill);
  }
}

function showEmojiPicker() {
  const emojis = ['😀', '🔥', '🚀', '❤️', '👍', '🎓', '🎉', '✨', '💡', '🤖'];
  const picker = prompt("Choose an emoji to insert:\n" + emojis.join('  '), '🎓');
  if (picker) messageInput.value += picker;
}

messageInput.addEventListener('keydown', e => {
  if (e.key === 'Enter') sendMessage();
});

// PWA Registration
if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('./sw.js').catch(() => {});
  });
}

initApp();
