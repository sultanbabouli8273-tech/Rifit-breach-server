// Rift Breach multiplayer server. No dependencies: run with   node server.js
// It serves index.html and relays WebSocket messages between players in the same room.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = parseInt(process.env.PORT, 10) || 8080;
const MAX_PAYLOAD = 16 * 1024;
const MAX_PER_ROOM = 16;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const rooms = new Map(); // room -> Set<client>

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
      if (err) { res.writeHead(404); res.end('index.html not found'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
  } else if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
  } else {
    res.writeHead(404); res.end('not found');
  }
});

function frame(text) {
  const payload = Buffer.from(text);
  const len = payload.length;
  let header;
  if (len < 126) header = Buffer.from([0x81, len]);
  else if (len < 65536) { header = Buffer.alloc(4); header[0] = 0x81; header[1] = 126; header.writeUInt16BE(len, 2); }
  else { header = Buffer.alloc(10); header[0] = 0x81; header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([header, payload]);
}

function control(opcode, payload) {
  payload = payload || Buffer.alloc(0);
  return Buffer.concat([Buffer.from([0x80 | opcode, payload.length]), payload]);
}

function broadcast(client, text) {
  const set = rooms.get(client.room);
  if (!set) return;
  const data = frame(text);
  for (const c of set) if (c !== client && !c.socket.destroyed) c.socket.write(data);
}

function leave(client) {
  if (client.left) return;
  client.left = true;
  if (client.room && rooms.has(client.room)) {
    const set = rooms.get(client.room);
    set.delete(client);
    if (client.pid) broadcast(client, JSON.stringify({ t: 'bye', id: client.pid }));
    if (set.size === 0) rooms.delete(client.room);
  }
}

function handleText(client, text) {
  let msg;
  try { msg = JSON.parse(text); } catch (e) { return; }
  if (!msg || typeof msg !== 'object') return;
  if (msg.t === 'join') {
    const room = String(msg.room || 'lobby').slice(0, 40);
    if (!rooms.has(room)) rooms.set(room, new Set());
    const set = rooms.get(room);
    if (set.size >= MAX_PER_ROOM) { client.socket.end(control(0x8)); return; }
    client.room = room;
    set.add(client);
    console.log('[join]', room, '(' + set.size + ' players)');
    return;
  }
  if (!client.room) return;
  if (typeof msg.id === 'string') client.pid = msg.id.slice(0, 20);
  broadcast(client, text);
}

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\nConnection: Upgrade\r\n' +
    'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
  );
  socket.setNoDelay(true);
  const client = { socket, room: null, pid: null, left: false };
  let buf = Buffer.alloc(0);
  let fragments = [];

  socket.on('data', chunk => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const fin = (buf[0] & 0x80) !== 0;
      const opcode = buf[0] & 0x0f;
      const masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f;
      let offset = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); offset = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); offset = 10; }
      if (len > MAX_PAYLOAD) { socket.destroy(); return; }
      const maskLen = masked ? 4 : 0;
      if (buf.length < offset + maskLen + len) return;
      let payload = buf.subarray(offset + maskLen, offset + maskLen + len);
      if (masked) {
        const mask = buf.subarray(offset, offset + 4);
        const out = Buffer.alloc(len);
        for (let i = 0; i < len; i++) out[i] = payload[i] ^ mask[i & 3];
        payload = out;
      }
      buf = buf.subarray(offset + maskLen + len);

      if (opcode === 0x8) { socket.end(control(0x8)); return; }
      if (opcode === 0x9) { socket.write(control(0xA, payload)); continue; }
      if (opcode === 0xA) continue;
      if (opcode === 0x1 || opcode === 0x0) {
        fragments.push(payload);
        if (fin) {
          const text = Buffer.concat(fragments).toString('utf8');
          fragments = [];
          handleText(client, text);
        } else if (fragments.reduce((n, f) => n + f.length, 0) > MAX_PAYLOAD) {
          socket.destroy(); return;
        }
      }
    }
  });
  socket.on('close', () => leave(client));
  socket.on('error', () => leave(client));
});

server.listen(PORT, () => {
  console.log('Rift Breach server running on port ' + PORT);
  console.log('Open  http://localhost:' + PORT + '  (other devices: http://<your-LAN-IP>:' + PORT + ')');
});
