const http = require('http');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 8080;

const clients = new Map();
const queue = [];
let seq = 0;

const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const raw  = (ws, s) => { if (ws && ws.readyState === 1) ws.send(s); };

function getStats() {
  let seeking = 0, playing = 0;
  for (const c of clients.values()) {
    if (c.state === 'seek') seeking++;
    if (c.state === 'play') playing++;
  }
  return { online: clients.size, seeking, playing };
}

function broadcastLobby() {
  const m = JSON.stringify({ t: 'lobby', ...getStats() });
  for (const ws of clients.keys()) raw(ws, m);
}

function pair(a, b) {
  const ca = clients.get(a), cb = clients.get(b);
  if (!ca || !cb) return;
  ca.state = cb.state = 'play';
  ca.peer = b; cb.peer = a;
  const ia = queue.indexOf(a); if (ia >= 0) queue.splice(ia, 1);
  const ib = queue.indexOf(b); if (ib >= 0) queue.splice(ib, 1);
  // ПРОТОКОЛ КЛИЕНТА: t:"paired", host:bool, opponent:string
  send(a, { t: 'paired', host: true,  opponent: cb.name, plane: cb.plane, pilot: cb.pilot });
  send(b, { t: 'paired', host: false, opponent: ca.name, plane: ca.plane, pilot: ca.pilot });
  console.log(`[MATCH] ${ca.name} (HOST) <-> ${cb.name} (CLIENT)`);
}

function tryMatch(ws) {
  for (let i = 0; i < queue.length; i++) {
    const candidate = queue[i];
    if (candidate !== ws && clients.has(candidate) && clients.get(candidate).state === 'seek') {
      queue.splice(i, 1);
      pair(candidate, ws);
      return true;
    }
  }
  return false;
}

function drop(ws) {
  const c = clients.get(ws);
  if (!c) return;
  const qi = queue.indexOf(ws);
  if (qi >= 0) queue.splice(qi, 1);
  if (c.peer && clients.has(c.peer)) {
    send(c.peer, { t: 'bye', reason: 'disconnect' });
    const p = clients.get(c.peer);
    if (p) { p.peer = null; p.state = 'idle'; }
  }
  clients.delete(ws);
  try { ws.terminate(); } catch (_) {}
  broadcastLobby();
}

const server = http.createServer((req, res) => {
  res.setHeader('content-type', 'application/json; charset=utf-8');
  res.setHeader('access-control-allow-origin', '*');
  if (req.url === '/health') { res.writeHead(200); res.end('{"ok":true}'); return; }
  res.writeHead(200);
  res.end(JSON.stringify({ service: 'airduel-lobby', ...getStats() }, null, 2));
});

const wss = new WebSocketServer({ server });

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  clients.set(ws, { id: ++seq, name: 'PILOT', plane: 0, pilot: 0, state: 'idle', peer: null });
  send(ws, { t: 'lobby', ...getStats() });

  ws.on('message', data => {
    const s = data.toString();
    let o; try { o = JSON.parse(s); } catch (_) { return; }
    const c = clients.get(ws);
    if (!c) return;

    if (o.t === 'seek') {
      c.name  = String(o.n || 'PILOT').slice(0, 10);
      c.plane = Math.max(0, Math.min(6, o.pl | 0));
      c.pilot = Math.max(0, Math.min(14, o.pi | 0));
      c.state = 'seek';
      if (!tryMatch(ws)) {
        if (!queue.includes(ws)) queue.push(ws);
        send(ws, { t: 'waiting', pos: queue.indexOf(ws) + 1, ...getStats() });
      }
      broadcastLobby();
      return;
    }

    if (o.t === 'cancel') {
      const qi = queue.indexOf(ws);
      if (qi >= 0) queue.splice(qi, 1);
      c.state = 'idle';
      send(ws, { t: 'cancelled' });
      broadcastLobby();
      return;
    }

    // Всё остальное (hello, start, i, s, rematch, bye) — ретрансляция партнёру
    if (c.peer && clients.has(c.peer)) {
      raw(c.peer, s);
    }
  });

  ws.on('close', () => drop(ws));
  ws.on('error', () => drop(ws));
});

// Пинг мёртвых соединений
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { drop(ws); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) {}
  }
}, 15000);

setInterval(broadcastLobby, 3000);
server.listen(PORT, () => console.log(`[AIR DUEL LOBBY] :${PORT}`));