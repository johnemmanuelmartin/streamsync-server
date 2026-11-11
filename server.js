const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;

// Simple HTTP server for health checks
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Content-Type', 'application/json');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200);
    const roomCount = rooms.size;
    let peerCount = 0;
    rooms.forEach(set => peerCount += set.size);
    res.end(JSON.stringify({
      status: 'online',
      service: 'StreamSync Signaling Server',
      activeRooms: roomCount,
      activePeers: peerCount,
      timestamp: new Date().toISOString()
    }));
  } else {
    res.writeHead(404);
    res.end(JSON.stringify({ error: 'Not found' }));
  }
});

// WebSocket server attached to HTTP server
const wss = new WebSocketServer({ server });

// Map: roomId => Set of { ws, peerId, userName }
const rooms = new Map();

function broadcastToRoom(roomId, senderWs, payload) {
  const room = rooms.get(roomId);
  if (!room) return;

  const data = JSON.stringify(payload);
  for (const client of room) {
    if (client.ws !== senderWs && client.ws.readyState === WebSocket.OPEN) {
      client.ws.send(data);
    }
  }
}

wss.on('connection', (ws) => {
  let currentRoomId = null;
  let currentPeerId = null;
  let currentUserName = 'Anonymous';

  ws.on('message', (messageRaw) => {
    try {
      const msg = JSON.parse(messageRaw);
      const { type, roomId, peerId, userName, data } = msg;

      switch (type) {
        case 'join-room': {
          currentRoomId = roomId;
          currentPeerId = peerId;
          currentUserName = userName || 'Friend';

          if (!rooms.has(roomId)) {
            rooms.set(roomId, new Set());
          }

          const room = rooms.get(roomId);

          // Get existing peers in the room before adding the new one
          const existingPeers = Array.from(room).map(p => ({
            peerId: p.peerId,
            userName: p.userName
          }));

          // Add this client
          room.add({ ws, peerId, userName: currentUserName });
          console.log(`[JOIN] ${currentUserName} (${peerId}) joined room: ${roomId}. Total peers in room: ${room.size}`);

          // Acknowledge back to joining peer with existing peers list
          ws.send(JSON.stringify({
            type: 'room-joined',
            roomId,
            peerId,
            existingPeers
          }));

          // Notify existing peers that a new user joined
          broadcastToRoom(roomId, ws, {
            type: 'peer-joined',
            peerId,
            userName: currentUserName
          });
          break;
        }

        case 'signal': {
          // WebRTC offer, answer, or ice-candidate relay
          const { targetPeerId, signalData } = data;
          const room = rooms.get(currentRoomId);
          if (!room) return;

          for (const client of room) {
            if (client.peerId === targetPeerId && client.ws.readyState === WebSocket.OPEN) {
              client.ws.send(JSON.stringify({
                type: 'signal',
                fromPeerId: currentPeerId,
                fromUserName: currentUserName,
                signalData
              }));
              break;
            }
          }
          break;
        }

        case 'broadcast': {
          // General room-wide broadcast (fallback for chat / sync if DataChannel isn't ready)
          if (currentRoomId) {
            broadcastToRoom(currentRoomId, ws, {
              type: 'broadcast',
              fromPeerId: currentPeerId,
              fromUserName: currentUserName,
              data: data
            });
          }
          break;
        }

        default:
          console.warn('Unknown message type:', type);
      }
    } catch (err) {
      console.error('Error handling WebSocket message:', err);
    }
  });

  const cleanUp = () => {
    if (currentRoomId && rooms.has(currentRoomId)) {
      const room = rooms.get(currentRoomId);
      for (const client of room) {
        if (client.ws === ws) {
          room.delete(client);
          console.log(`[LEAVE] ${client.userName} (${client.peerId}) left room: ${currentRoomId}. Remaining: ${room.size}`);
          break;
        }
      }

      if (room.size === 0) {
        rooms.delete(currentRoomId);
        console.log(`[ROOM CLOSED] Room ${currentRoomId} is now empty and removed.`);
      } else {
        broadcastToRoom(currentRoomId, ws, {
          type: 'peer-left',
          peerId: currentPeerId,
          userName: currentUserName
        });
      }
    }
  };

  ws.on('close', cleanUp);
  ws.on('error', (err) => {
    console.error('Socket error:', err);
    cleanUp();
  });
});

server.listen(PORT, () => {
  console.log(`StreamSync Signaling Server running on http://localhost:${PORT}`);
  console.log(`WebSocket endpoint: ws://localhost:${PORT}`);
});
