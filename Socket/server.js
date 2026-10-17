const WebSocket = require('ws');
const http = require('http');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('WebSocket server online\n');
});

const wss = new WebSocket.Server({ server, path: '/ws' });

const rooms = new Map();

wss.on('connection', (ws) => {
  let jobid = null;

  ws.on('message', (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }

    if (data.jobid) {
      if (jobid && jobid !== data.jobid) {
        const oldRoom = rooms.get(jobid);
        if (oldRoom) oldRoom.delete(ws);
      }
      jobid = data.jobid;
      if (!rooms.has(jobid)) rooms.set(jobid, new Set());
      rooms.get(jobid).add(ws);
    }

    console.log("Mensagem recebida:", data);

    if (jobid && rooms.has(jobid)) {
      const room = rooms.get(jobid);
      const payload = JSON.stringify(data);
      room.forEach((client) => {
        if (client !== ws && client.readyState === WebSocket.OPEN) {
          client.send(payload);
        }
      });
    }
  });

  ws.on('close', () => {
    if (jobid && rooms.has(jobid)) {
      rooms.get(jobid).delete(ws);
      if (rooms.get(jobid).size === 0) rooms.delete(jobid);
    }
  });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`WebSocket server on port ${PORT}`);
});
