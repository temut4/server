// server/lobby.js — протокол синхронизирован с клиентом:
// клиент шлёт {t:"seek", n, pl, pi}
// сервер отвечает {t:"waiting"} / {t:"paired", host:bool, opponent:строка}
// после пары — прозрачная ретрансляция всех пакетов (hello, start, i, s, bye)
const http = require('http');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 8080;

const waiting = [];        // [{ws, info}] одиночки в очереди
const pairs = new Map();   // ws -> {peer: ws}

const send = (ws, o) => { if (ws && ws.readyState === 1) try { ws.send(JSON.stringify(o)); } catch (_) {} };
const raw  = (ws, s) => { if (ws && ws.readyState === 1) try { ws.send(s); } catch (_) {} };

function getStats() {
  let seeking = waiting.length, playing = 0;
  for (const p of pairs.values()) playing++;
  playing = playing / 2 | 0; // каждая пара учтена дважды
  return { online: waiting.length + playing * 2, seeking, playing };
}

function broadcastLobby() {
  const m = { t: 'lobby', ...getStats() };
  for (const w of waiting) send(w.ws, m);
  for (const [ws] of pairs) send(ws, m);
}

function pair(a, b) {
  pairs.set(a.ws, { peer: b.ws });
  pairs.set(b.ws, { peer: a.ws });
  // ПРОТОКОЛ КЛИЕНТА: t:"paired", host:bool, opponent:имя строкой
  send(a.ws, { t: 'paired', host: true,  opponent: b.info.n || 'PILOT', plane: b.info.pl, pilot: b.info.pi });
  send(b.ws, { t: 'paired', host: false, opponent: a.info.n || 'PILOT', plane: a.info.pl, pilot: a.info.pi });
  console.log(`[MATCH] ${a.info.n} (HOST) <-> ${b.info.n} (CLIENT)`);
}

function drop(ws) {
  const qi = waiting.findIndex(w => w.ws === ws);
  if (qi >= 0) waiting.splice(qi, 1);
  const p = pairs.get(ws);
  if (p) {
    pairs.delete(ws);
    send(p.peer, { t: 'bye', reason: 'disconnect' });
    pairs.delete(p.peer);
  }
  broadcastLobby();
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); res.end('{"ok":true}'); return; }
  res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
  res.end(JSON.stringify({ service: 'airduel-global-lobby', ...getStats() }));
});

const wss = new WebSocketServer({ server });
wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  let info = { n: 'PILOT', pl: 0, pi: 0 };

  ws.on('message', data => {
    const s = data.toString();
    let o; try { o = JSON.parse(s); } catch (_) { return; }

    if (o.t === 'seek') {
      info = { n: String(o.n || 'PILOT').slice(0, 10), pl: Math.max(0, Math.min(6, o.pl | 0)), pi: Math.max(0, Math.min(14, o.pi | 0)) };
      const other = waiting.shift();
      if (other) {
        pair(other, { ws, info });
      } else {
        waiting.push({ ws, info });
        send(ws, { t: 'waiting', pos: waiting.length, count: waiting.length });
      }
      broadcastLobby();
      return;
    }

    if (o.t === 'cancel') {
      const qi = waiting.findIndex(w => w.ws === ws);
      if (qi >= 0) waiting.splice(qi, 1);
      send(ws, { t: 'cancelled' });
      broadcastLobby();
      return