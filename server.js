require('dotenv').config();
const express = require('express');
const path = require('path');
const fs = require('fs');
const pino = require('pino');
const { Pool } = require('pg');

// ─── Configuration ───────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
const API_KEY = process.env.API_KEY;
const DATABASE_URL = process.env.DATABASE_URL;
const MAX_RECONNECT_ATTEMPTS = 3; // caps the 515 retry loop — prevents zombie loops

if (!API_KEY) {
  console.error('❌ API_KEY environment variable is required');
  process.exit(1);
}
if (!DATABASE_URL) {
  console.error('❌ DATABASE_URL environment variable is required');
  process.exit(1);
}

const pool = new Pool({ connectionString: DATABASE_URL });
const sessionDir = process.env.WHATSAPP_SESSION_DIR || path.join(process.cwd(), 'whatsapp-sessions');
const logger = pino({ level: process.env.LOG_LEVEL || 'info' });

// ─── Global WhatsApp State ──────────────────────────────────────
let globalSocket = null;
let isInitializing = false;
let cachedWaVersion = null;
let globalConnectionTimeout = null;
let reconnectTimer = null;
let reconnectAttempts = 0; // NEW — tracks consecutive 515 retries

// ─── Database Helpers ───────────────────────────────────────────
async function getSettings() {
  const res = await pool.query('SELECT * FROM "whatsapp_settings" ORDER BY id ASC LIMIT 1');
  if (res.rows.length === 0) {
    const created = await pool.query(`
      INSERT INTO "whatsapp_settings" (owner_phone, status, simulate_failures, simulate_session_error)
      VALUES ('9928203203', 'disconnected', false, false)
      RETURNING *
    `);
    return created.rows[0];
  }
  return res.rows[0];
}

async function updateSettings(id, data) {
  const setClauses = [];
  const values = [];
  let idx = 1;

  if (data.status !== undefined) { setClauses.push(`status = $${idx++}`); values.push(data.status); }
  if (data.qr_code !== undefined) { setClauses.push(`qr_code = $${idx++}`); values.push(data.qr_code); }

  if (setClauses.length === 0) return;
  values.push(id);

  const query = `UPDATE "whatsapp_settings" SET ${setClauses.join(', ')} WHERE id = $${idx}`;
  await pool.query(query, values);
}

async function logAuditEvent(billId, billNumber, event, details) {
  await pool.query(`
    INSERT INTO "whatsapp_audit_logs" (bill_id, bill_number, event, details)
    VALUES ($1, $2, $3, $4)
  `, [billId, billNumber, event, details]);
}

// ─── Session Cleanup Helper (SINGLE place for all cleanup) ─────
// FIXED: only deletes the CONTENTS of sessionDir, never the folder itself.
// sessionDir is now a Railway Volume mount point — rmdir'ing it directly
// throws EBUSY because the OS refuses to remove an active mount.
function clearSessionFiles() {
  if (fs.existsSync(sessionDir)) {
    for (const entry of fs.readdirSync(sessionDir)) {
      fs.rmSync(path.join(sessionDir, entry), { recursive: true, force: true });
    }
  } else {
    fs.mkdirSync(sessionDir, { recursive: true });
  }
  console.log('🧹 Session files cleared.');
}

// ─── Baileys WhatsApp Daemon ────────────────────────────────────
async function initWhatsappSocket() {
  if (isInitializing) {
    console.log('⏳ WhatsApp initialization already in progress, skipping...');
    return;
  }

  isInitializing = true;
  console.log('🚀 Starting WhatsApp connection...');

  try {
    const baileys = await import('@whiskeysockets/baileys');
    const makeWASocket = baileys.default;
    const { DisconnectReason, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = baileys;

    // Ensure session directory exists
    if (!fs.existsSync(sessionDir)) {
      fs.mkdirSync(sessionDir, { recursive: true });
    }

    const settings = await getSettings();

    // ── Dead-man's switch ────────────────────────────────────────────────────
    // If a stray timer fired but the DB was explicitly set to disconnected,
    // abort immediately so we don't overwrite the user's manual disconnect.
    if (settings.status === 'disconnected') {
      console.log('🛑 DB is set to disconnected. Aborting daemon startup to prevent zombie loops.');
      isInitializing = false;
      reconnectAttempts = 0;
      return;
    }
    // ─────────────────────────────────────────────────────────────────────────

    await updateSettings(settings.id, { status: 'connecting', qr_code: null });

    const { state, saveCreds } = await useMultiFileAuthState(sessionDir);

    // Use cached version to avoid rate limiting
    let versionToUse = cachedWaVersion;
    if (!versionToUse) {
      const { version, isLatest } = await fetchLatestBaileysVersion();
      console.log(`fetched WA v${version.join('.')}, isLatest: ${isLatest}`);
      cachedWaVersion = version;
      versionToUse = version;
    } else {
      console.log(`using cached WA v${versionToUse.join('.')}`);
    }

    const sock = makeWASocket({
      version: versionToUse,
      auth: state,
      browser: Browsers.ubuntu('Chrome'),
      printQRInTerminal: true,
      logger: logger,
      fireInitQueries: false,
      syncFullHistory: false,   // Prevent full history download (stops OOM crashes)
      markOnlineOnConnect: false,
    });

    globalSocket = sock;

    // Start a 60-second watchdog timer. If the user doesn't scan the QR code
    // within 60 seconds, this will gracefully kill the socket and reset the DB to disconnected.
    globalConnectionTimeout = setTimeout(async () => {
      if (globalSocket === sock) {
        console.log('⏱️ WhatsApp connection timed out after 60 seconds. Terminating socket...');
        try { globalSocket.end(undefined); } catch (e) { /* ignore */ }
        globalSocket = null;
        reconnectAttempts = 0;
        clearSessionFiles();
        await updateSettings(settings.id, { status: 'disconnected', qr_code: null });
      }
    }, 60000);

    // Save credentials on update
    sock.ev.on('creds.update', saveCreds);

    // Completely ignore history sync events to prevent memory spikes
    sock.ev.on('messaging-history.set', () => {
      console.log('📚 History sync event received — ignored to save memory.');
    });

    // Handle connection updates
    sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        console.log('📱 New QR code generated!');
        await updateSettings(settings.id, { status: 'connecting', qr_code: qr });
      }

      if (connection === 'open') {
        clearTimeout(globalConnectionTimeout);
        reconnectAttempts = 0; // reset on successful connection
        console.log('✅ WhatsApp connected successfully!');
        await updateSettings(settings.id, { status: 'connected', qr_code: null });
      }

      if (connection === 'close') {
        clearTimeout(globalConnectionTimeout);
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        console.log(`❌ Disconnected (code: ${statusCode}).`);

        // Prevent auto-reconnect if this socket was manually killed or replaced
        if (globalSocket !== sock) {
          console.log('🛑 Socket was manually killed. Stopping reconnect loop.');
          reconnectAttempts = 0;
          await updateSettings(settings.id, { status: 'disconnected', qr_code: null });
          return;
        }

        if (statusCode === 515) {
          // Code 515 = pairing restart. This is EXPECTED and REQUIRED after a
          // fresh QR scan — WhatsApp always closes once and needs one reconnect
          // to finish the handshake. But if the session is broken, this can
          // repeat forever, so we cap it.
          reconnectAttempts++;

          if (reconnectAttempts > MAX_RECONNECT_ATTEMPTS) {
            console.log(`🛑 Max reconnect attempts (${MAX_RECONNECT_ATTEMPTS}) reached. Session appears broken — resetting.`);
            globalSocket = null;
            isInitializing = false;
            reconnectAttempts = 0;
            clearSessionFiles(); // wipe the broken session so next connect starts clean
            await updateSettings(settings.id, { status: 'disconnected', qr_code: null });
          } else {
            console.log(`🔄 Reconnecting in 5 seconds (pairing restart, attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})...`);
            isInitializing = false;
            reconnectTimer = setTimeout(() => initWhatsappSocket(), 5000);
          }
        } else if (statusCode === DisconnectReason.loggedOut) {
          // Logged out (401) — reset state cleanly
          console.log('🛑 Session logged out. Resetting state.');
          globalSocket = null;
          isInitializing = false;
          reconnectAttempts = 0;
          await updateSettings(settings.id, { status: 'disconnected', qr_code: null });
        } else {
          // Any other disconnect (network error, OOM restart, etc.)
          // Do NOT auto-reconnect — reset DB to disconnected and let user reconnect manually.
          console.log(`⚠️ Unexpected disconnect (code: ${statusCode}). Resetting to disconnected.`);
          globalSocket = null;
          isInitializing = false;
          reconnectAttempts = 0;
          clearSessionFiles();
          await updateSettings(settings.id, { status: 'disconnected', qr_code: null });
        }
      }
    });

    isInitializing = false;
    console.log('🚀 WhatsApp daemon initialized!');

  } catch (error) {
    isInitializing = false;
    console.error('❌ WhatsApp initialization failed:', error);
    throw error;
  }
}

async function disconnectWhatsapp() {
  // Clear any pending timers so they don't fire and restart the daemon
  clearTimeout(globalConnectionTimeout);
  clearTimeout(reconnectTimer);
  reconnectAttempts = 0;

  // Close the socket without calling logout() to avoid triggering 401 event
  if (globalSocket) {
    try {
      globalSocket.end(undefined);
    } catch (e) {
      console.error('Error closing socket:', e);
    }
    globalSocket = null;
  }

  isInitializing = false;

  const settings = await getSettings();
  await updateSettings(settings.id, { status: 'disconnected', qr_code: null });

  // Single cleanup point
  clearSessionFiles();
  console.log('✅ WhatsApp disconnected and cleaned up.');
}

async function sendWhatsappMessage(phone, text, pdfPath, pdfFilename, pdfBase64) {
  if (!globalSocket) {
    throw new Error('WhatsApp is not connected');
  }

  // Format phone number for WhatsApp (add country code if missing)
  let jid = phone.replace(/[^0-9]/g, '');
  if (jid.length === 10) {
    jid = '91' + jid; // India country code
  }
  jid = jid + '@s.whatsapp.net';

  // Send PDF if provided
  if ((pdfPath || pdfBase64) && pdfFilename) {
    let pdfBuffer;

    if (pdfBase64) {
      pdfBuffer = Buffer.from(pdfBase64, 'base64');
    } else if (pdfPath?.startsWith('http')) {
      const response = await fetch(pdfPath);
      pdfBuffer = Buffer.from(await response.arrayBuffer());
    } else if (pdfPath) {
      // Try to read from local filesystem (only works if bot and files are on same machine)
      const fullPath = path.resolve(pdfPath);
      if (fs.existsSync(fullPath)) {
        pdfBuffer = fs.readFileSync(fullPath);
      }
    }

    if (pdfBuffer) {
      await globalSocket.sendMessage(jid, {
        document: pdfBuffer,
        mimetype: 'application/pdf',
        fileName: pdfFilename,
        caption: text // Add text as caption to the PDF
      });
      console.log(`✅ Message sent to ${phone} (with PDF)`);
      return; // Exit early since text was sent as caption
    }
  }

  // Fallback: Send text only if no PDF was provided or buffer failed
  await globalSocket.sendMessage(jid, { text });
  console.log(`✅ Message sent to ${phone} (text only)`);
}

// ─── Express App ────────────────────────────────────────────────
const app = express();
app.use(express.json({ limit: '10mb' }));

// API Key authentication middleware
function authMiddleware(req, res, next) {
  const key = req.headers['x-api-key'];
  if (key !== API_KEY) {
    return res.status(401).json({ error: 'Unauthorized: Invalid API key' });
  }
  next();
}

// Health check (no auth required — Railway uses this)
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    whatsapp: globalSocket ? 'connected' : 'disconnected',
    uptime: process.uptime(),
  });
});

// All other routes require API key
app.use(authMiddleware);

// GET /status — Return current WhatsApp connection status
app.get('/status', async (req, res) => {
  try {
    const settings = await getSettings();
    res.json({
      id: settings.id,
      ownerPhone: settings.owner_phone,
      status: settings.status,
      qrCode: settings.qr_code,
      simulateFailures: settings.simulate_failures,
      simulateSessionError: settings.simulate_session_error,
    });
  } catch (error) {
    console.error('Error fetching status:', error);
    res.status(500).json({ error: 'Failed to fetch status' });
  }
});

// POST /connect — Start WhatsApp connection and QR generation
// FIXED: no longer wipes session files on every connect click.
// initWhatsappSocket() -> useMultiFileAuthState() will reuse a valid
// existing session automatically. Only a broken/expired session gets
// cleared, and only from inside the connection.update handler itself.
app.post('/connect', async (req, res) => {
  try {
    if (globalSocket) {
      try { globalSocket.end(undefined); } catch (e) { /* ignore */ }
      globalSocket = null;
    }
    isInitializing = false;
    reconnectAttempts = 0;

    initWhatsappSocket().catch((err) => {
      console.error('Daemon startup error:', err);
    });

    res.json({ status: 'connecting', message: 'WhatsApp daemon started. Poll /status for QR code.' });
  } catch (error) {
    console.error('Error starting connection:', error);
    res.status(500).json({ error: 'Failed to start WhatsApp connection' });
  }
});

// POST /disconnect — Disconnect WhatsApp
app.post('/disconnect', async (req, res) => {
  try {
    await disconnectWhatsapp();
    res.json({ status: 'disconnected', message: 'WhatsApp disconnected successfully.' });
  } catch (error) {
    console.error('Error disconnecting:', error);
    res.status(500).json({ error: 'Failed to disconnect WhatsApp' });
  }
});

// POST /send — Send a WhatsApp message
app.post('/send', async (req, res) => {
  try {
    const { phone, text, pdfUrl, pdfFilename, pdfBase64 } = req.body;

    if (!phone || !text) {
      return res.status(400).json({ error: 'Missing required fields: phone, text' });
    }

    await sendWhatsappMessage(phone, text, pdfUrl, pdfFilename, pdfBase64);

    res.json({ success: true, message: `Message sent to ${phone}` });
  } catch (error) {
    console.error('Error sending message:', error);
    res.status(500).json({ error: error.message || 'Failed to send message' });
  }
});

// ─── Start Server ───────────────────────────────────────────────
app.listen(PORT, async () => {
  console.log(`\n🤖 WhatsApp Bot microservice running on port ${PORT}`);
  console.log(`   Health:     http://localhost:${PORT}/health`);
  console.log(`   Status:     http://localhost:${PORT}/status`);
  console.log(`   Connect:    POST http://localhost:${PORT}/connect`);
  console.log(`   Disconnect: POST http://localhost:${PORT}/disconnect`);
  console.log(`   Send:       POST http://localhost:${PORT}/send`);

  // Auto-start bot on boot ONLY if DB says it should already be connected
  try {
    const settings = await getSettings();

    if (settings.status === 'connected') {
      console.log('🔄 Active session detected (DB=connected). Auto-starting WhatsApp daemon...');
      initWhatsappSocket().catch(e => console.error('Daemon startup error:', e));
    } else {
      console.log('ℹ️ No active session. Waiting for manual /connect trigger.');
    }
  } catch (err) {
    console.error('Startup DB check failed:', err);
  }
});