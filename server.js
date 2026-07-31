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

// ─── Database Helpers ───────────────────────────────────────────
async function getSettings() {
  const res = await pool.query('SELECT * FROM "whatsapp_settings" LIMIT 1');
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

// ─── Baileys WhatsApp Daemon ────────────────────────────────────
async function initWhatsappSocket() {
  if (isInitializing) {
    console.log('⏳ WhatsApp initialization already in progress...');
    return;
  }

  console.log('🚀 Starting initWhatsappSocket flow...');
  isInitializing = true;

  try {
    console.log('📦 Importing baileys...');
    const baileys = await import('@whiskeysockets/baileys');
    const makeWASocket = baileys.default;
    const { DisconnectReason, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers } = baileys;

    console.log('📁 Checking session directory...');
    if (!fs.existsSync(sessionDir)) {
      fs.mkdirSync(sessionDir, { recursive: true });
    }

    console.log('📡 Fetching settings from DB...');
    const settings = await getSettings();

    console.log('📝 Updating DB status...');
    await updateSettings(settings.id, { status: 'connecting', qr_code: null });

    console.log('🔐 Initializing auth state...');
    const { state, saveCreds } = await useMultiFileAuthState(sessionDir);
    
    console.log('🌐 Fetching latest WhatsApp version...');
    let versionToUse = cachedWaVersion;
    if (!versionToUse) {
      const { version, isLatest } = await fetchLatestBaileysVersion();
      console.log(`fetched WA v${version.join('.')}, isLatest: ${isLatest}`);
      cachedWaVersion = version;
      versionToUse = version;
    } else {
      console.log(`using cached WA v${versionToUse.join('.')}`);
    }

    console.log('🔌 Creating WA socket...');
    const sock = makeWASocket({
      version: versionToUse,
      auth: state,
      browser: Browsers.ubuntu('Chrome'),
      printQRInTerminal: true,
      logger: logger,
      shouldSyncHistoryMessage: () => false,
      fireInitQueries: true,
      markOnlineOnConnect: false,
    });

    globalSocket = sock;

    // Save credentials on update
    sock.ev.on('creds.update', saveCreds);

    // Handle connection updates
    sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr } = update;

      console.log(`📡 Connection update: ${connection || 'pending'}`);

      if (qr) {
        console.log('📱 New QR code generated!');
        await updateSettings(settings.id, { status: 'connecting', qr_code: qr });
      }

      if (connection === 'open') {
        console.log('✅ WhatsApp connected successfully!');
        await updateSettings(settings.id, { status: 'connected', qr_code: null });
      }

      if (connection === 'close') {
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        console.log(`❌ Disconnected (code: ${statusCode}).`);

        if (shouldReconnect) {
          console.log('🔄 Reconnecting in 5 seconds to finalize pairing...');
          isInitializing = false;
          setTimeout(() => initWhatsappSocket(), 5000);
        } else {
          console.log('🛑 Session explicitly logged out or cancelled. Cleaning up...');
          globalSocket = null;
          isInitializing = false;
          
          await updateSettings(settings.id, { status: 'disconnected', qr_code: null });

          if (fs.existsSync(sessionDir)) {
            fs.rmSync(sessionDir, { recursive: true, force: true });
            fs.mkdirSync(sessionDir, { recursive: true });
          }
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
  if (globalSocket) {
    try {
      await globalSocket.logout();
    } catch (e) {
      console.error('Error during logout:', e);
    }
    globalSocket = null;
  }

  isInitializing = false;

  const settings = await getSettings();
  await updateSettings(settings.id, { status: 'disconnected', qr_code: null });

  // Clear session files
  if (fs.existsSync(sessionDir)) {
    fs.rmSync(sessionDir, { recursive: true, force: true });
    fs.mkdirSync(sessionDir, { recursive: true });
  }
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
app.post('/connect', async (req, res) => {
  try {
    const settings = await getSettings();
    await updateSettings(settings.id, { status: 'connecting', qr_code: null });

    // Clear stale session files to prevent 401 from old credentials
    if (fs.existsSync(sessionDir)) {
      fs.rmSync(sessionDir, { recursive: true, force: true });
      fs.mkdirSync(sessionDir, { recursive: true });
      console.log('🧹 Cleared stale session files before fresh connect.');
    }

    // Fire daemon in background (don't await — it runs forever)
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
app.listen(PORT, () => {
  console.log(`\n🤖 WhatsApp Bot microservice running on port ${PORT}`);
  console.log(`   Health:     http://localhost:${PORT}/health`);
  console.log(`   Status:     http://localhost:${PORT}/status`);
  console.log(`   Connect:    POST http://localhost:${PORT}/connect`);
  console.log(`   Disconnect: POST http://localhost:${PORT}/disconnect`);
  console.log(`   Send:       POST http://localhost:${PORT}/send\n`);
});
