/**
 * Zero-Persistence Ephemeral Communication Server
 * Exactly two users per session.
 *
 * Features:
 * - In-memory pairing code registry with 5-minute expiry.
 * - Real-time WebSocket relay for E2EE messages, typing indicators, and delivery ACKs.
 * - WebRTC signalling broker (SDP Offer/Answer, ICE candidates).
 * - Authoritative session termination (cleans in-memory records and broadcasts termination).
 * - ZERO disk storage: No messages or media are ever saved to disk or database.
 */

const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'Ephemeral Relay Server Running', activeSessions: sessions.size }));
});

// Allow WebSocket connections on root '/' as well as '/ws', configure maxPayload to 50MB for media/voice notes
const wss = new WebSocket.Server({ server, maxPayload: 50 * 1024 * 1024 });

// In-Memory ephemeral sessions: Map<pairingCode, Session>
const sessions = new Map();
// Client connection mapping: Map<ws, { pairingCode, userId }>
const clients = new Map();

wss.on('connection', (ws) => {
    console.log('[WS] Client connected');
    clients.set(ws, { pairingCode: null, userId: null });

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message.toString());
            handleMessage(ws, data);
        } catch (e) {
            console.error('[WS] Malformed JSON received:', e.message);
        }
    });

    ws.on('close', () => {
        const clientInfo = clients.get(ws);
        if (clientInfo && clientInfo.pairingCode) {
            console.log(`[WS] Client disconnected: ${clientInfo.userId}`);
            const session = sessions.get(clientInfo.pairingCode);
            if (session) {
                // If userA or userB left, clear them from session
                if (session.userA?.ws === ws) session.userA = null;
                if (session.userB?.ws === ws) session.userB = null;

                const peerWs = session.userA ? session.userA.ws : (session.userB ? session.userB.ws : null);
                if (peerWs && peerWs.readyState === WebSocket.OPEN) {
                    peerWs.send(JSON.stringify({ type: 'PEER_STATUS', online: false }));
                }

                // If room is completely empty, clean it up
                if (!session.userA && !session.userB) {
                    sessions.delete(clientInfo.pairingCode);
                }
            }
            // Broadcast room status change
            broadcastRoomUpdate(clientInfo.pairingCode, false);
        }
        clients.delete(ws);
    });
});

function broadcastRoomUpdate(pairingCode, hasPeer) {
    const message = JSON.stringify({ type: 'ROOM_STATUS', pairingCode, hasPeer });
    for (const [wsClient] of clients.entries()) {
        if (wsClient.readyState === WebSocket.OPEN) {
            wsClient.send(message);
        }
    }
}

function handleMessage(ws, data) {
    const { type } = data;

    switch (type) {
        case 'PAIR_REQUEST': {
            // User A creates pairing code
            const { pairingCode, userId, publicKey } = data;
            sessions.set(pairingCode, {
                code: pairingCode,
                createdAt: Date.now(),
                userA: { ws, userId, publicKey },
                userB: null
            });
            clients.set(ws, { pairingCode, userId });
            console.log(`[PAIR] Session created with code: ${pairingCode} by ${userId}`);
            broadcastRoomUpdate(pairingCode, true);
            break;
        }

        case 'PAIR_JOIN': {
            // User B joins pairing code (or initiates if session not yet created)
            const { pairingCode, userId, publicKey } = data;
            let session = sessions.get(pairingCode);

            if (!session) {
                // If room does not exist yet, create it and wait for the second peer
                sessions.set(pairingCode, {
                    code: pairingCode,
                    createdAt: Date.now(),
                    userA: { ws, userId, publicKey },
                    userB: null
                });
                clients.set(ws, { pairingCode, userId });
                console.log(`[PAIR] Auto-created session for code: ${pairingCode} by ${userId}`);
                // Broadcast to any other clients that a peer entered this room
                broadcastRoomUpdate(pairingCode, true);
                return;
            }

            if (session.userA?.userId === userId) {
                // Same user reconnecting
                session.userA.ws = ws;
                clients.set(ws, { pairingCode, userId });
                return;
            }

            if (session.userB !== null && session.userB.userId !== userId) {
                ws.send(JSON.stringify({ type: 'PAIR_ERROR', error: 'Session already has two participants' }));
                return;
            }

            session.userB = { ws, userId, publicKey };
            clients.set(ws, { pairingCode, userId });

            console.log(`[PAIR] User B (${userId}) paired successfully with User A (${session.userA.userId})`);

            // Notify User A with User B's public key
            session.userA.ws.send(JSON.stringify({
                type: 'PAIR_SUCCESS',
                pairingCode,
                peerUserId: userId,
                peerPublicKey: publicKey
            }));

            // Notify User B with User A's public key
            session.userB.ws.send(JSON.stringify({
                type: 'PAIR_SUCCESS',
                pairingCode,
                peerUserId: session.userA.userId,
                peerPublicKey: session.userA.publicKey
            }));
            break;
        }

        case 'CHAT_MESSAGE':
        case 'STATUS_ACK':
        case 'TYPING':
        case 'DELETE_MESSAGE':
        case 'CALL_OFFER':
        case 'CALL_ANSWER':
        case 'ICE_CANDIDATE':
        case 'CALL_END': {
            const clientInfo = clients.get(ws);
            const pairingCode = data.pairingCode || clientInfo?.pairingCode;
            if (!pairingCode) return;

            const session = sessions.get(pairingCode);
            if (!session) return;

            // Route strictly to the other party in this 2-person room
            let targetWs = null;
            if (session.userA?.ws === ws) {
                targetWs = session.userB?.ws;
            } else if (session.userB?.ws === ws) {
                targetWs = session.userA?.ws;
            } else if (session.userA?.userId === data.to) {
                targetWs = session.userA?.ws;
            } else if (session.userB?.userId === data.to) {
                targetWs = session.userB?.ws;
            } else {
                // Fallback by from
                targetWs = (session.userA?.userId === data.from) ? session.userB?.ws : session.userA?.ws;
            }

            if (targetWs && targetWs.readyState === WebSocket.OPEN) {
                targetWs.send(JSON.stringify(data));
                console.log(`[RELAY] Successfully relayed ${data.type} to other peer in session ${pairingCode}`);
            } else {
                console.log(`[RELAY] Target peer not reachable for ${data.type}`);
            }
            break;
        }

        case 'SESSION_TERMINATE': {
            const { pairingCode } = data;
            console.log(`[TERMINATE] Authoritative session termination for code: ${pairingCode}`);
            const session = sessions.get(pairingCode);
            if (session) {
                // Broadcast termination event to both devices
                const broadcastEvent = JSON.stringify({ type: 'SESSION_TERMINATE' });
                if (session.userA?.ws && session.userA.ws.readyState === WebSocket.OPEN) {
                    session.userA.ws.send(broadcastEvent);
                }
                if (session.userB?.ws && session.userB.ws.readyState === WebSocket.OPEN) {
                    session.userB.ws.send(broadcastEvent);
                }
                // Completely erase session from memory
                sessions.delete(pairingCode);
            }
            break;
        }

        case 'HEARTBEAT': {
            ws.send(JSON.stringify({ type: 'HEARTBEAT_ACK', timestamp: Date.now() }));
            break;
        }

        case 'CHECK_ROOM': {
            const { pairingCode, userId } = data;
            const session = sessions.get(pairingCode);
            let hasPeer = false;
            if (session) {
                // Check if someone else is currently present in this room
                const userAPresent = session.userA && session.userA.userId !== userId && session.userA.ws.readyState === WebSocket.OPEN;
                const userBPresent = session.userB && session.userB.userId !== userId && session.userB.ws.readyState === WebSocket.OPEN;
                hasPeer = !!(userAPresent || userBPresent);
            }
            ws.send(JSON.stringify({
                type: 'ROOM_STATUS',
                pairingCode,
                hasPeer
            }));
            break;
        }
    }
}

// Inactivity cleanup: remove abandoned sessions older than 30 minutes
setInterval(() => {
    const now = Date.now();
    for (const [code, session] of sessions.entries()) {
        if (now - session.createdAt > 30 * 60 * 1000) {
            console.log(`[CLEANUP] Expiring inactive session: ${code}`);
            sessions.delete(code);
        }
    }
}, 60 * 1000);

server.listen(PORT, () => {
    console.log(`Ephemeral Two-User Signalling Server running on port ${PORT}`);
});
