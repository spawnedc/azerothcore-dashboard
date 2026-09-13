/**
 * Server Agent — standalone process that manages worldserver(s) and authserver.
 *
 * Run independently of the dashboard backend so game servers survive a
 * dashboard restart:
 *
 *   node backend/serverAgent.js
 *
 * The dashboard backend connects to this agent via HTTP (REST) and an
 * SSE stream (/events) for real-time log forwarding.
 *
 * Multiple worldservers are supported via worldservers.json — see
 * worldservers.json.example.  When that file is absent the agent falls
 * back to the single WORLDSERVER_PATH defined in .env.
 *
 * Spawned servers are detached into their own process group (see
 * startServer()), so a signal delivered to this agent's process group (e.g.
 * a terminal/session teardown) won't reach them. That's independent of this
 * process's own lifetime, though: if the agent process itself exits, these
 * pipes close and the child gets SIGPIPE on its next log write. A pid file
 * per server (see the PID files section below) lets a fresh agent instance
 * recognize an already-running server on startup — whether it's one that
 * outlived a previous agent process, or one started entirely outside the
 * dashboard — but full console/log access to an adopted process is only
 * available again once it's stopped and restarted through the agent.
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const express = require('express');
const log     = require('./logger')('server-agent');
const http    = require('http');
const { spawn } = require('child_process');
const path    = require('path');
const fs      = require('fs');
const wsConfig = require('./worldservers');

const PORT   = parseInt(process.env.AGENT_PORT, 10) || 3002;
const SECRET = process.env.AGENT_SECRET || 'changeme';

// ── Process management ────────────────────────────────────────────────────────

function sanitizeOutput(str) {
  return str.toString()
    .replace(/\x1B\[[0-9;]*[ABCDEFGJKST]/g, '')
    .replace(/\x1B\[[0-9;]*[hl]/g, '')
    .replace(/\x1B\][^\x07]*\x07/g, '')
    .replace(/\x1B[^[\]m]/g, '');
}

// ── PID files (adoption of already-running instances) ──────────────────────────
//
// The agent's own restarts (crash, or its /restart API), or a server started
// entirely outside the dashboard (e.g. via a plain shell alias), would
// otherwise be invisible to a fresh agent instance — it only knows about
// processes it spawned itself, in memory. A PID file per server lets a new
// agent instance recognize "this is already running" instead of assuming
// it's down (and possibly trying to double-start it).

const RUN_DIR = path.join(__dirname, 'run');
fs.mkdirSync(RUN_DIR, { recursive: true });

function pidFilePath(serverName) {
  return path.join(RUN_DIR, `${serverName}.pid`);
}

function writePidFile(serverName, pid) {
  try { fs.writeFileSync(pidFilePath(serverName), String(pid)); } catch {}
}

function removePidFile(serverName) {
  try { fs.unlinkSync(pidFilePath(serverName)); } catch {}
}

/** True if `pid` is alive (process.kill with signal 0 doesn't actually signal it). */
function isAlive(pid) {
  try { process.kill(pid, 0); return true; }
  catch { return false; }
}

function expectedExePath(serverName) {
  if (serverName === 'authserver') return process.env.AUTHSERVER_PATH;
  return wsConfig.getById(serverName)?.path;
}

/**
 * Check whether `pid` is actually running the executable expected for
 * `serverName`. Resolves /proc/<pid>/exe (the kernel's own record of what's
 * actually running) rather than comparing against /proc/<pid>/cmdline's
 * argv[0] — the plain `start`/`wow` shell aliases launch these binaries with
 * a relative argv0 (`./authserver`), which wouldn't string-match the agent's
 * absolute configured path even though it's the same binary.
 */
function pidMatchesExe(pid, serverName) {
  const expected = expectedExePath(serverName);
  if (!expected) return false;
  try {
    const actual = fs.readlinkSync(`/proc/${pid}/exe`);
    return actual === fs.realpathSync(expected);
  } catch {
    return false;
  }
}

/** On agent startup, recognize a server that's already running from a leftover pid file. */
function tryAdopt(serverName) {
  let pid;
  try { pid = parseInt(fs.readFileSync(pidFilePath(serverName), 'utf8'), 10); } catch { return; }
  if (!pid || !isAlive(pid) || !pidMatchesExe(pid, serverName)) {
    removePidFile(serverName);
    return;
  }
  processes[serverName] = { pid, adopted: true };
  processLogs[serverName] = [
    `[Server Agent] Adopted already-running ${serverName} (pid ${pid}) — console/log ` +
    `streaming is unavailable until it's restarted through the agent.\n`,
  ];
  log.info(`Adopted already-running ${serverName} (pid ${pid})`);
}

// Dynamic maps — one entry per worldserver + authserver
const processes   = { authserver: null };
const processLogs = { authserver: [] };
const autoRestart = { authserver: false };
const stopping    = { authserver: false };
const startTimes  = { authserver: null };

// Initialise slots for every configured worldserver
for (const ws of wsConfig.load()) {
  processes[ws.id]   = null;
  processLogs[ws.id] = [];
  autoRestart[ws.id] = false;
  stopping[ws.id]    = false;
  startTimes[ws.id]  = null;
}

// Adopt anything already running before the HTTP API starts accepting requests.
for (const name of ['authserver', ...wsConfig.getIds()]) {
  tryAdopt(name);
}

const MAX_LOG_LINES = 2000;

// SSE clients connected from the dashboard backend
const sseClients = new Set();

function broadcast(event) {
  const payload = `data: ${JSON.stringify(event)}\n\n`;
  for (const send of sseClients) {
    try { send(payload); } catch {}
  }
}

function emitLog(serverName, raw) {
  const line = sanitizeOutput(raw.toString());
  processLogs[serverName].push(line);
  if (processLogs[serverName].length > MAX_LOG_LINES) {
    processLogs[serverName].shift();
  }
  broadcast({ type: 'console-line', server: serverName, line });
}

function startServer(serverName) {
  if (processes[serverName]) {
    return { success: false, error: 'Server is already running' };
  }

  let exePath, workDir;

  if (serverName === 'authserver') {
    exePath = process.env.AUTHSERVER_PATH;
    workDir = process.env.AUTHSERVER_DIR || null;
  } else {
    const ws = wsConfig.getById(serverName);
    if (!ws) return { success: false, error: `Unknown server: ${serverName}` };
    exePath = ws.path;
    workDir = ws.dir || null;
  }

  if (!exePath) {
    return { success: false, error: `${serverName} path not configured` };
  }

  try {
    stopping[serverName] = false;
    const cwd  = workDir || path.dirname(exePath);
    // detached: true puts the child in its own process group, so a signal
    // aimed at the agent's session/process group (e.g. a tmux session
    // teardown) doesn't reach it. This does NOT protect against the agent's
    // own Node process exiting normally — these stdio pipes are still owned
    // by this process, so the child would still get SIGPIPE on its next log
    // write once we're gone. That case is an accepted, documented gap (see
    // the module doc comment at the top of this file).
    const proc = spawn(exePath, [], {
      cwd,
      stdio: ['pipe', 'pipe', 'pipe'],
      windowsHide: false,
      detached: true,
    });
    proc.unref();

    processes[serverName]   = proc;
    startTimes[serverName]  = Date.now();
    processLogs[serverName] = [`[Server Agent] Starting ${serverName} from ${exePath}\n`];
    writePidFile(serverName, proc.pid);

    proc.stdout.on('data', (d) => emitLog(serverName, d));
    proc.stderr.on('data', (d) => emitLog(serverName, d));

    proc.on('close', (code) => {
      emitLog(serverName, `\n[Server Agent] Process exited with code ${code}\n`);
      processes[serverName]  = null;
      startTimes[serverName] = null;
      removePidFile(serverName);
      broadcast({ type: 'server-status', server: serverName, running: false });

      if (autoRestart[serverName] && !stopping[serverName]) {
        emitLog(serverName, `[Server Agent] Auto-restart enabled — restarting in 5 seconds…\n`);
        setTimeout(() => startServer(serverName), 5000);
      }
      stopping[serverName] = false;
    });

    proc.on('error', (err) => {
      emitLog(serverName, `\n[Server Agent] Failed to start: ${err.message}\n`);
      processes[serverName]  = null;
      startTimes[serverName] = null;
      removePidFile(serverName);
      broadcast({ type: 'server-status', server: serverName, running: false });
      stopping[serverName] = false;
    });

    broadcast({ type: 'server-status', server: serverName, running: true });
    return { success: true };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function stopServer(serverName, mode = 'exit', delay = 0) {
  const proc = processes[serverName];
  if (!proc) return { success: false, error: 'Server is not running' };

  stopping[serverName] = true;

  // An adopted process (recognized via pid file, not spawned by us) has no
  // stdin/stdout pipes to talk to — fall back to a plain signal, and update
  // our own bookkeeping directly since we won't get a 'close' event for it.
  if (proc.adopted) {
    try { process.kill(proc.pid, 'SIGTERM'); } catch {}
    processes[serverName]  = null;
    startTimes[serverName] = null;
    stopping[serverName]   = false;
    removePidFile(serverName);
    broadcast({ type: 'server-status', server: serverName, running: false });
    return { success: true };
  }

  if (wsConfig.isWorldserver(serverName)) {
    try {
      if (mode === 'shutdown') {
        proc.stdin.write(`server shutdown ${delay}\n`);
      } else {
        proc.stdin.write('server exit\n');
      }
    } catch { proc.kill(); }
  } else {
    proc.kill();
  }

  return { success: true };
}

function setAutoRestart(serverName, enabled) {
  if (!(serverName in autoRestart)) return { success: false, error: 'Invalid server name' };
  autoRestart[serverName] = !!enabled;
  return { success: true };
}

function sendCommand(command, serverName) {
  // Default to first configured worldserver for backward compatibility
  const target = serverName || wsConfig.getIds()[0];
  if (!target) return { success: false, error: 'No worldserver configured' };
  const proc = processes[target];
  if (!proc) return { success: false, error: `${target} is not running` };
  if (proc.adopted) {
    return {
      success: false,
      error: 'Cannot send console commands to a server started outside the agent — stop and restart it from the dashboard to enable this.',
    };
  }
  try {
    proc.stdin.write(command + '\n');
    return { success: true };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

function getStatus(serverName) {
  // Adopted processes don't get a 'close' event when they exit (we didn't
  // spawn them), so check liveness lazily whenever status is actually read.
  const proc = processes[serverName];
  if (proc?.adopted && !isAlive(proc.pid)) {
    processes[serverName] = null;
    removePidFile(serverName);
  }

  return {
    running:     processes[serverName] !== null,
    autoRestart: autoRestart[serverName],
    pid:         processes[serverName]?.pid || null,
    startTime:   startTimes[serverName],
  };
}

// ── HTTP API ──────────────────────────────────────────────────────────────────

const app = express();
app.use(express.json());

// Token auth on all routes
app.use((req, res, next) => {
  if (req.headers['x-agent-token'] !== SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
});

const VALID = wsConfig.getValidServers();

// GET /status
app.get('/status', (req, res) => {
  const result = { authserver: getStatus('authserver'), uptime: process.uptime() };
  for (const id of wsConfig.getIds()) {
    result[id] = getStatus(id);
  }
  res.json(result);
});

// GET /:name/logs
app.get('/:name/logs', (req, res) => {
  const { name } = req.params;
  if (!VALID.includes(name)) return res.status(400).json({ error: 'Invalid server name' });
  res.json({ logs: processLogs[name] });
});

// POST /:name/start
app.post('/:name/start', (req, res) => {
  const { name } = req.params;
  if (!VALID.includes(name)) return res.status(400).json({ error: 'Invalid server name' });
  res.json(startServer(name));
});

// POST /:name/stop
app.post('/:name/stop', (req, res) => {
  const { name } = req.params;
  if (!VALID.includes(name)) return res.status(400).json({ error: 'Invalid server name' });
  const { mode = 'exit', delay = 0 } = req.body || {};
  res.json(stopServer(name, mode, parseInt(delay, 10) || 0));
});

// POST /:name/autorestart
app.post('/:name/autorestart', (req, res) => {
  const { name } = req.params;
  if (!VALID.includes(name)) return res.status(400).json({ error: 'Invalid server name' });
  res.json(setAutoRestart(name, req.body.enabled));
});

// POST /command
app.post('/command', (req, res) => {
  const { command, server } = req.body;
  if (!command) return res.status(400).json({ error: 'command is required' });
  res.json(sendCommand(command, server));
});

// POST /restart — gracefully restart the agent process via runAgent.js
app.post('/restart', (req, res) => {
  res.json({ ok: true });
  setTimeout(() => process.exit(42), 500);
});

// GET /events — SSE stream for the dashboard backend
app.get('/events', (req, res) => {
  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.flushHeaders();

  // Send a heartbeat so the client can detect a stale connection
  const heartbeat = setInterval(() => {
    try { res.write(': heartbeat\n\n'); } catch {}
  }, 15000);

  const send = (payload) => res.write(payload);
  sseClients.add(send);

  // Send current status immediately so the client is in sync
  const initEvent = { type: 'init', authserver: getStatus('authserver') };
  for (const id of wsConfig.getIds()) {
    initEvent[id] = getStatus(id);
  }
  res.write(`data: ${JSON.stringify(initEvent)}\n\n`);

  req.on('close', () => {
    clearInterval(heartbeat);
    sseClients.delete(send);
  });
});

// ── Start ─────────────────────────────────────────────────────────────────────

const server = http.createServer(app);
server.listen(PORT, '0.0.0.0', () => {
  log.info(`Server Agent listening on port ${PORT}`);
});
