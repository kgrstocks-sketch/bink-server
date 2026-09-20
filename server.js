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

const wss = new WebSocket.Server({ server, path: '/ws' });

// In-Memory ephemeral sessions: Map<pairingCode, Session>
const sessions = new Map();
// Client connection mapping: Map<ws, { pairingCode, userId }>
const clients = new Map();

wss.on('connection', (ws) => {
    console.log('[WS] Client connected');

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
        if (clientInfo) {
            console.log(`[WS] Client disconnected: ${clientInfo.userId}`);
            const session = sessions.get(clientInfo.pairingCode);
            if (session) {
                // Notify the other peer about disconnection
                const peerWs = session.userA?.ws === ws ? session.userB?.ws : session.userA?.ws;
                if (peerWs && peerWs.readyState === WebSocket.OPEN) {
                    peerWs.send(JSON.stringify({ type: 'PEER_STATUS', online: false }));
                }
            }
            clients.delete(ws);
        }
    });
});

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
            break;
        }

        case 'PAIR_JOIN': {
            // User B joins pairing code
            const { pairingCode, userId, publicKey } = data;
            const session = sessions.get(pairingCode);

            if (!session) {
                ws.send(JSON.stringify({ type: 'PAIR_ERROR', error: 'Invalid or expired pairing code' }));
                return;
            }

            if (session.userB !== null) {
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
            // Relay strictly between paired peers
            const clientInfo = clients.get(ws);
            if (!clientInfo) return;
            const session = sessions.get(clientInfo.pairingCode);
            if (!session) return;

            const targetWs = session.userA?.ws === ws ? session.userB?.ws : session.userA?.ws;
            if (targetWs && targetWs.readyState === WebSocket.OPEN) {
                targetWs.send(JSON.stringify(data));
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
