// Минималистичный лобби-сервер: принимает WS-соединения, спаривает по 2 и ретранслирует.
const { WebSocketServer } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); res.end('ok'); return; }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('airduel lobby · waiting:' + waiting.length + ' · pairs:' + pairs.size);
});

const wss = new WebSocketServer({ server });
const waiting = [];        // массив {ws, info} ждущих одиночных игроков
const pairs = new Map();   // ws -> {peer: ws, info}

function broadcastWaiting() {
  const msg = JSON.stringify({ t: 'waiting', count: waiting.length, pairs: pairs.size });
  for (const w of waiting) try { w.ws.send(msg); } catch (_) {}
  for (const p of pairs.values()) try { p.peer.send(msg); } catch (_) {}
}

function makePair(a, b) {
  const pair = { a, b };
  pairs.set(a.ws, { peer: b.ws, info: b.info });
  pairs.set(b.ws, { peer: a.ws, info: a.info });
  // сообщаем обоим, что они спарены
  a.ws.send(JSON.stringify({ t: 'matched', role: 'host', opponent: b.info }));
  b.ws.send(JSON.stringify({ t: 'matched', role: 'guest', opponent: a.info }));
  broadcastWaiting();
}

wss.on('connection', (ws) => {
  let myInfo = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (_) { return; }

    // регистрация в очереди
    if (msg.t === 'seek') {
      myInfo = { n: (msg.n || 'PILOT').slice(0, 10), pl: msg.pl | 0, pi: msg.pi | 0 };
      // есть ли свободный?
      const other = waiting.shift();
      if (other) {
        makePair(other, { ws, info: myInfo });
      } else {
        waiting.push({ ws, info: myInfo });
        ws.send(JSON.stringify({ t: 'waiting', pos: waiting.length, count: waiting.length }));
        broadcastWaiting();
      }
      return;
    }

    // отмена ожидания
    if (msg.t === 'cancel') {
      const idx = waiting.findIndex(w => w.ws === ws);
      if (idx >= 0) waiting.splice(idx, 1);
      pairs.delete(ws);
      broadcastWaiting();
      return;
    }

    // игровые пакеты: ретранслируем партнёру
    const pair = pairs.get(ws);
    if (pair) {
      try { pair.peer.send(raw.toString()); } catch (_) {}
    }
  });

  ws.on('close', () => {
    const idx = waiting.findIndex(w => w.ws === ws);
    if (idx >= 0) waiting.splice(idx, 1);
    const pair = pairs.get(ws);
    if (pair) {
      pairs.delete(ws);
      // уведомить партнёра
      try { pair.peer.send(JSON.stringify({ t: 'bye' })); } catch (_) {}
      // разорвать вторую сторону
      try { pair.peer.close(); } catch (_) {}
      pairs.delete(pair.peer);
    }
    broadcastWaiting();
  });

  ws.on('error', () => {});
});

server.listen(PORT, () => console.log('lobby on :' + PORT));