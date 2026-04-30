import { createServer, type IncomingMessage, type ServerResponse, type Server } from 'node:http';
import { logEmitter, logger } from '../logger.js';
import { SyncOrchestrator } from '../pipeline/orchestrator.js';
import { normalizeConfig } from '../config.js';
import type { SyncConfig } from '../types.js';

interface SSEClient {
  id: number;
  res: ServerResponse;
}

let clients: SSEClient[] = [];
let nextClientId = 0;
let orchestrator: SyncOrchestrator | null = null;
let statsTimer: ReturnType<typeof setInterval> | null = null;

function broadcast(type: string, data: unknown): void {
  const payload = JSON.stringify({ type, data, ts: Date.now() });
  for (const client of clients) {
    try {
      client.res.write(`data: ${payload}\n\n`);
    } catch {
      // Client disconnected, will be removed on next cleanup
    }
  }
}

function removeClient(id: number): void {
  clients = clients.filter(c => c.id !== id);
}

function setupSSE(req: IncomingMessage, res: ServerResponse): void {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });

  const client: SSEClient = { id: nextClientId++, res };
  clients.push(client);

  // Send initial connected event
  res.write(`data: ${JSON.stringify({ type: 'connected', ts: Date.now() })}\n\n`);

  req.on('close', () => {
    removeClient(client.id);
  });
}

function sendJSON(res: ServerResponse, status: number, data: unknown): void {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', (chunk: Buffer) => { body += chunk.toString(); });
    req.on('end', () => resolve(body));
    req.on('error', reject);
  });
}

function startStatsPolling(): void {
  if (statsTimer) return;
  statsTimer = setInterval(() => {
    if (orchestrator) {
      broadcast('stats', orchestrator.stats);
    }
  }, 1000);
}

function stopStatsPolling(): void {
  if (statsTimer) {
    clearInterval(statsTimer);
    statsTimer = null;
  }
}

// Subscribe logger events to SSE broadcast
logEmitter.on('log', (entry: { level: string; message: string; timestamp: string }) => {
  broadcast('log', entry);
});

function getHtml(): string {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MySQL Sync - Web UI</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #0d1117; color: #c9d1d9; min-height: 100vh;
  }
  .container { max-width: 1100px; margin: 0 auto; padding: 20px; }
  h1 { font-size: 1.5rem; color: #58a6ff; margin-bottom: 20px; }
  h2 { font-size: 1rem; color: #8b949e; margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.5px; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  .card {
    background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 16px;
    margin-bottom: 16px;
  }
  .form-row { display: flex; gap: 10px; margin-bottom: 10px; flex-wrap: wrap; }
  .form-group { display: flex; flex-direction: column; gap: 4px; flex: 1; min-width: 120px; }
  label { font-size: 0.8rem; color: #8b949e; }
  input, select {
    background: #0d1117; border: 1px solid #30363d; border-radius: 4px;
    color: #c9d1d9; padding: 6px 10px; font-size: 0.85rem; outline: none;
  }
  input:focus, select:focus { border-color: #58a6ff; }
  .btn-row { display: flex; gap: 10px; margin-top: 10px; }
  button {
    padding: 8px 20px; border: none; border-radius: 4px; font-size: 0.9rem;
    cursor: pointer; font-weight: 500;
  }
  .btn-start { background: #238636; color: #fff; }
  .btn-start:hover { background: #2ea043; }
  .btn-start:disabled { background: #21262d; color: #484f58; cursor: not-allowed; }
  .btn-stop { background: #da3633; color: #fff; }
  .btn-stop:hover { background: #f85149; }
  .btn-stop:disabled { background: #21262d; color: #484f58; cursor: not-allowed; }
  .status {
    display: inline-block; padding: 4px 12px; border-radius: 12px;
    font-size: 0.8rem; font-weight: 600; margin-left: 10px;
  }
  .status-idle { background: #21262d; color: #8b949e; }
  .status-running { background: #1b3a1b; color: #3fb950; }
  .status-error { background: #3a1b1b; color: #f85149; }
  .stat-value { font-size: 1.4rem; font-weight: 700; color: #58a6ff; }
  .stat-label { font-size: 0.75rem; color: #8b949e; }
  .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(100px, 1fr)); gap: 12px; }
  .stat-item { text-align: center; }
  #log-console {
    background: #0d1117; border: 1px solid #30363d; border-radius: 4px;
    height: 300px; overflow-y: auto; padding: 10px; font-family: 'Cascadia Code', 'Fira Code', monospace;
    font-size: 0.8rem; line-height: 1.5;
  }
  .log-entry { margin-bottom: 2px; white-space: pre-wrap; word-break: break-all; }
  .log-debug { color: #8b949e; }
  .log-info { color: #c9d1d9; }
  .log-warn { color: #d2991d; }
  .log-error { color: #f85149; }
  @media (max-width: 768px) { .grid { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<div class="container">
  <h1>MySQL Sync <span id="status-badge" class="status status-idle">idle</span></h1>

  <div class="grid">
    <!-- Source DB -->
    <div class="card">
      <h2>Source Database</h2>
      <div class="form-row">
        <div class="form-group" style="flex:3"><label>Host</label><input id="src-host" value="127.0.0.1"></div>
        <div class="form-group" style="flex:1"><label>Port</label><input id="src-port" type="number" value="3306"></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label>User</label><input id="src-user" value="root"></div>
        <div class="form-group"><label>Password</label><input id="src-password" type="password"></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label>Database (optional)</label><input id="src-database" placeholder="Leave empty for all"></div>
      </div>
    </div>

    <!-- Target DB -->
    <div class="card">
      <h2>Target Database</h2>
      <div class="form-row">
        <div class="form-group" style="flex:3"><label>Host</label><input id="tgt-host" value="127.0.0.1"></div>
        <div class="form-group" style="flex:1"><label>Port</label><input id="tgt-port" type="number" value="3307"></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label>User</label><input id="tgt-user" value="root"></div>
        <div class="form-group"><label>Password</label><input id="tgt-password" type="password"></div>
      </div>
    </div>
  </div>

  <!-- Sync Options -->
  <div class="card">
    <h2>Sync Options</h2>
    <div class="form-row">
      <div class="form-group" style="flex:3">
        <label>Databases (comma-separated, leave empty for all)</label>
        <input id="databases" placeholder="e.g. db1,db2">
      </div>
      <div class="form-group" style="flex:2">
        <label>Mode</label>
        <select id="sync-mode">
          <option value="full+incremental">Full + Incremental</option>
          <option value="full">Full Only</option>
          <option value="incremental">Incremental Only</option>
        </select>
      </div>
    </div>
    <div class="form-row">
      <div class="form-group" style="flex:1"><label>Full Sync Workers</label><input id="fs-workers" type="number" value="4" min="1" max="16"></div>
      <div class="form-group" style="flex:1"><label>Batch Size</label><input id="batch-size" type="number" value="1000" min="100"></div>
      <div class="form-group" style="flex:1"><label>Server ID</label><input id="server-id" type="number" value="7777" min="1"></div>
      <div class="form-group" style="flex:1"><label>Position File</label><input id="position-file" value="./binlog-position.json"></div>
    </div>
  </div>

  <!-- Controls -->
  <div class="card">
    <div class="btn-row">
      <button class="btn-start" id="btn-start" onclick="startSync()">Start Sync</button>
      <button class="btn-stop" id="btn-stop" onclick="stopSync()" disabled>Stop</button>
    </div>
  </div>

  <!-- Full Sync Progress -->
  <div class="card">
    <h2>Full Sync Progress</h2>
    <div class="stat-grid">
      <div class="stat-item"><div class="stat-value" id="fs-tables">0/0</div><div class="stat-label">Tables</div></div>
      <div class="stat-item"><div class="stat-value" id="fs-rows-read">0</div><div class="stat-label">Rows Read</div></div>
      <div class="stat-item"><div class="stat-value" id="fs-rows-written">0</div><div class="stat-label">Rows Written</div></div>
      <div class="stat-item"><div class="stat-value" id="fs-elapsed">0s</div><div class="stat-label">Elapsed</div></div>
    </div>
  </div>

  <!-- Incremental Sync Stats -->
  <div class="card">
    <h2>Incremental Sync</h2>
    <div class="stat-grid">
      <div class="stat-item"><div class="stat-value" id="inc-inserts">0</div><div class="stat-label" style="color:#3fb950">Inserts</div></div>
      <div class="stat-item"><div class="stat-value" id="inc-updates">0</div><div class="stat-label" style="color:#d2991d">Updates</div></div>
      <div class="stat-item"><div class="stat-value" id="inc-deletes">0</div><div class="stat-label" style="color:#f85149">Deletes</div></div>
      <div class="stat-item"><div class="stat-value" id="inc-errors">0</div><div class="stat-label" style="color:#f85149">Errors</div></div>
      <div class="stat-item"><div class="stat-value" id="inc-elapsed">0s</div><div class="stat-label">Elapsed</div></div>
    </div>
  </div>

  <!-- Log Console -->
  <div class="card">
    <h2>Logs</h2>
    <div id="log-console"></div>
  </div>
</div>

<script>
  const logConsole = document.getElementById('log-console');
  const MAX_LOG_LINES = 500;
  let logLines = 0;

  function appendLog(level, message) {
    const div = document.createElement('div');
    div.className = 'log-entry log-' + level.toLowerCase();
    div.textContent = message;
    logConsole.appendChild(div);
    logLines++;
    // Trim old entries
    while (logLines > MAX_LOG_LINES && logConsole.firstChild) {
      logConsole.removeChild(logConsole.firstChild);
      logLines--;
    }
    logConsole.scrollTop = logConsole.scrollHeight;
  }

  function setStatus(status, className) {
    const badge = document.getElementById('status-badge');
    badge.textContent = status;
    badge.className = 'status ' + className;
  }

  function setButtonState(running) {
    document.getElementById('btn-start').disabled = running;
    document.getElementById('btn-stop').disabled = !running;
  }

  // SSE connection
  const es = new EventSource('/api/events');
  es.onmessage = function(e) {
    try {
      const msg = JSON.parse(e.data);
      if (msg.type === 'log') {
        appendLog(msg.data.level, msg.data.message);
      } else if (msg.type === 'stats') {
        updateStats(msg.data);
      } else if (msg.type === 'lifecycle') {
        if (msg.data === 'started') { setStatus('running', 'status-running'); setButtonState(true); }
        else if (msg.data === 'stopped') { setStatus('idle', 'status-idle'); setButtonState(false); }
        else if (msg.data === 'error') { setStatus('error', 'status-error'); setButtonState(false); }
      }
    } catch {}
  };
  es.onerror = function() { appendLog('warn', '[UI] SSE connection lost, reconnecting...'); };

  function updateStats(stats) {
    if (stats.fullSync) {
      document.getElementById('fs-tables').textContent = stats.fullSync.tablesCompleted + '/' + stats.fullSync.tablesTotal;
      document.getElementById('fs-rows-read').textContent = stats.fullSync.rowsRead.toLocaleString();
      document.getElementById('fs-rows-written').textContent = stats.fullSync.rowsWritten.toLocaleString();
      if (stats.fullSync.startTime) {
        var sec = Math.floor((Date.now() - stats.fullSync.startTime) / 1000);
        document.getElementById('fs-elapsed').textContent = sec + 's';
      }
    }
    if (stats.incrementalSync) {
      document.getElementById('inc-inserts').textContent = stats.incrementalSync.inserts.toLocaleString();
      document.getElementById('inc-updates').textContent = stats.incrementalSync.updates.toLocaleString();
      document.getElementById('inc-deletes').textContent = stats.incrementalSync.deletes.toLocaleString();
      document.getElementById('inc-errors').textContent = stats.incrementalSync.errors.toLocaleString();
      if (stats.incrementalSync.startTime) {
        var sec2 = Math.floor((Date.now() - stats.incrementalSync.startTime) / 1000);
        document.getElementById('inc-elapsed').textContent = sec2 + 's';
      }
    }
  }

  function buildConfig() {
    return {
      source: {
        host: document.getElementById('src-host').value,
        port: parseInt(document.getElementById('src-port').value) || 3306,
        user: document.getElementById('src-user').value,
        password: document.getElementById('src-password').value,
        database: document.getElementById('src-database').value || undefined,
      },
      target: {
        host: document.getElementById('tgt-host').value,
        port: parseInt(document.getElementById('tgt-port').value) || 3306,
        user: document.getElementById('tgt-user').value,
        password: document.getElementById('tgt-password').value,
      },
      mode: document.getElementById('sync-mode').value,
      fullSync: {
        enabled: document.getElementById('sync-mode').value !== 'incremental',
        workerCount: parseInt(document.getElementById('fs-workers').value) || 4,
        batchSize: parseInt(document.getElementById('batch-size').value) || 1000,
        batchQueueSize: 10,
      },
      incrementalSync: {
        enabled: document.getElementById('sync-mode').value !== 'full',
        workerCount: 2,
        positionFile: document.getElementById('position-file').value || './binlog-position.json',
        serverId: parseInt(document.getElementById('server-id').value) || 7777,
      },
      syncObjects: {
        databases: document.getElementById('databases').value
          ? document.getElementById('databases').value.split(',').map(function(s) { return s.trim(); })
          : undefined,
      },
    };
  }

  async function startSync() {
    var config = buildConfig();
    appendLog('info', '[UI] Starting sync with config: ' + JSON.stringify({
      source: config.source.host + ':' + config.source.port,
      target: config.target.host + ':' + config.target.port,
      mode: config.mode,
    }));
    setStatus('connecting...', 'status-running');
    try {
      var resp = await fetch('/api/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config),
      });
      if (!resp.ok) {
        var err = await resp.json();
        throw new Error(err.error || 'Unknown error');
      }
      appendLog('info', '[UI] Sync started successfully');
    } catch (e) {
      appendLog('error', '[UI] Failed to start: ' + e.message);
      setStatus('error', 'status-error');
    }
  }

  async function stopSync() {
    appendLog('info', '[UI] Stopping sync...');
    try {
      await fetch('/api/stop', { method: 'POST' });
    } catch (e) {
      appendLog('error', '[UI] Stop request failed: ' + e.message);
    }
  }
</script>
</body>
</html>`;
}

export function startUIServer(port: number = 3000): Server {
  const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    const url = req.url || '/';
    const method = req.method || 'GET';

    try {
      if (method === 'GET' && url === '/') {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(getHtml());
      } else if (method === 'GET' && url === '/api/events') {
        setupSSE(req, res);
      } else if (method === 'POST' && url === '/api/start') {
        const body = await readBody(req);
        const rawConfig = JSON.parse(body);
        const config: SyncConfig = normalizeConfig(rawConfig);

        // Stop any existing orchestrator
        if (orchestrator) {
          await orchestrator.stop().catch(() => {});
          stopStatsPolling();
        }

        orchestrator = new SyncOrchestrator(config);
        startStatsPolling();

        // Start in background
        orchestrator.start()
          .then(() => {
            broadcast('lifecycle', 'stopped');
            stopStatsPolling();
          })
          .catch((err: Error) => {
            logger.error('Orchestrator error:', err);
            broadcast('lifecycle', 'error');
            stopStatsPolling();
          });

        broadcast('lifecycle', 'started');
        sendJSON(res, 200, { ok: true });
      } else if (method === 'POST' && url === '/api/stop') {
        if (orchestrator) {
          await orchestrator.stop().catch(() => {});
          orchestrator = null;
        }
        stopStatsPolling();
        broadcast('lifecycle', 'stopped');
        sendJSON(res, 200, { ok: true });
      } else {
        sendJSON(res, 404, { error: 'Not found' });
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      logger.error('HTTP error:', message);
      sendJSON(res, 500, { error: message });
    }
  });

  server.on('error', (err: NodeJS.ErrnoException) => {
    if (err.code === 'EADDRINUSE') {
      logger.error(`Port ${port} is already in use.`);
      process.exit(1);
    } else {
      logger.error('Server error:', err.message);
    }
  });

  server.listen(port, () => {
    logger.info(`Web UI server started at http://localhost:${port}`);
  });

  return server;
}
