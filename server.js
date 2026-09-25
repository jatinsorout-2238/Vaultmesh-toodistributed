const express = require('express');
const http = require('http');
const path = require('path');
const { DatabaseSync } = require('node:sqlite');
const { WebSocketServer, WebSocket } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

app.use(express.json());
app.use(express.static(path.join(__dirname)));

// =========================================================================
// SQLITE DATABASE INITIALIZATION
// =========================================================================
const dataDir = process.env.DATA_DIR || __dirname;
const dbPath = path.join(dataDir, 'vaultmesh.db');
const db = new DatabaseSync(dbPath);

db.exec(`
  CREATE TABLE IF NOT EXISTS cluster_meta (
    key TEXT PRIMARY KEY,
    value TEXT
  );

  CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    hostname TEXT,
    short_name TEXT,
    status TEXT,
    disk_usage_pct INTEGER,
    capacity_used_tb REAL,
    capacity_total_tb REAL,
    zone TEXT,
    rack TEXT,
    last_heartbeat REAL,
    latency_ms REAL,
    iops REAL,
    temp INTEGER,
    nvme_health INTEGER
  );

  CREATE TABLE IF NOT EXISTS policies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_name TEXT UNIQUE,
    mode TEXT,
    ec_k INTEGER,
    ec_m INTEGER,
    replication_factor INTEGER,
    durability_target TEXT,
    consistency_mode TEXT,
    status TEXT,
    created_at TEXT
  );

  CREATE TABLE IF NOT EXISTS rebalance_jobs (
    id TEXT PRIMARY KEY,
    source_node TEXT,
    target_node TEXT,
    pg TEXT,
    total_tb REAL,
    moved_tb REAL,
    progress INTEGER,
    throttle INTEGER,
    rate INTEGER,
    status TEXT,
    eta TEXT,
    created_at TEXT
  );

  CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT,
    severity TEXT,
    badge TEXT,
    title TEXT,
    detail TEXT,
    message TEXT,
    timestamp REAL,
    related_resource TEXT,
    deep_link TEXT
  );

  CREATE TABLE IF NOT EXISTS audit_log (
    id TEXT PRIMARY KEY,
    timestamp TEXT,
    raw_timestamp REAL,
    actor TEXT,
    action TEXT,
    resource TEXT,
    result TEXT,
    signature TEXT,
    client_ip TEXT,
    diff_json TEXT
  );

  CREATE TABLE IF NOT EXISTS notifications (
    id TEXT PRIMARY KEY,
    event_id INTEGER,
    severity TEXT,
    title TEXT,
    detail TEXT,
    timestamp REAL,
    read_status INTEGER,
    deep_link TEXT
  );
`);

// =========================================================================
// SEED INITIAL DATABASE STATE IF EMPTY
// =========================================================================
function seedDatabase() {
  const nodeCount = db.prepare('SELECT COUNT(*) as count FROM nodes').get().count;
  if (nodeCount === 0) {
    console.log('[DB] Seeding initial cluster nodes...');
    const insertNode = db.prepare(`
      INSERT INTO nodes (id, hostname, short_name, status, disk_usage_pct, capacity_used_tb, capacity_total_tb, zone, rack, last_heartbeat, latency_ms, iops, temp, nvme_health)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    for (let i = 0; i < 16; i++) {
      const idStr = i < 10 ? `0${i}` : `${i}`;
      const az = i < 8 ? 'us-east-2a' : i < 12 ? 'us-east-2b' : 'us-east-2c';
      const rack = `rack-0${Math.floor(i / 4) + 1}`;
      const fillPct = Math.floor(60 + (i * 7) % 25);
      const usedTb = +(fillPct * 0.16).toFixed(1);
      const status = (i === 3 || i === 11) ? 'rebalancing' : 'healthy';

      insertNode.run(
        `node-us-east-${idStr}`,
        `node-us-east-${idStr}.internal`,
        `OSD.${idStr}`,
        status,
        fillPct,
        usedTb,
        16.0,
        az,
        rack,
        Date.now(),
        0.12,
        +(16.5 + (i % 4) * 0.8).toFixed(1),
        36 + (i % 5),
        97 + (i % 3)
      );
    }
  }

  const policyCount = db.prepare('SELECT COUNT(*) as count FROM policies').get().count;
  if (policyCount === 0) {
    console.log('[DB] Seeding initial durability policies...');
    const insertPolicy = db.prepare(`
      INSERT INTO policies (bucket_name, mode, ec_k, ec_m, replication_factor, durability_target, consistency_mode, status, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    insertPolicy.run('telemetry-time-series', 'ec', 8, 4, null, '99.999999999% (11 9s)', 'Strict Raft Linearizable', 'ACTIVE', new Date().toISOString());
    insertPolicy.run('video-raw-ingest', 'ec', 6, 3, null, '99.99999%', 'Strict Raft Linearizable', 'ACTIVE', new Date().toISOString());
    insertPolicy.run('financial-audit-logs', 'replica', null, null, 3, '99.999999999% (11 9s)', 'Strict Raft Linearizable', 'ACTIVE', new Date().toISOString());
    insertPolicy.run('ai-model-checkpoints', 'ec', 8, 4, null, '99.999999999% (11 9s)', 'Strict Raft Linearizable', 'ACTIVE', new Date().toISOString());
  }

  const jobCount = db.prepare('SELECT COUNT(*) as count FROM rebalance_jobs').get().count;
  if (jobCount === 0) {
    console.log('[DB] Seeding rebalancing jobs...');
    const insertJob = db.prepare(`
      INSERT INTO rebalance_jobs (id, source_node, target_node, pg, total_tb, moved_tb, progress, throttle, rate, status, eta, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    insertJob.run('REBAL-9041', 'node-va-04 (VA)', 'node-va-12 (VA)', 'pg.4412_ec63', 4.2, 3.1, 74, 10, 124, 'active', '01h 04m', new Date().toISOString());
    insertJob.run('REBAL-9042', 'node-eu-02 (FRA)', 'node-eu-08 (FRA)', 'pg.1098_rep3', 2.8, 1.45, 52, 10, 98, 'active', '02h 18m', new Date().toISOString());
    insertJob.run('REBAL-9043', 'node-va-01 (VA)', 'node-va-11 (VA)', 'pg.8812_ec84', 3.6, 3.2, 89, 10, 110, 'active', '00h 28m', new Date().toISOString());
    insertJob.run('REBAL-9044', 'node-eu-05 (FRA)', 'node-eu-07 (FRA)', 'pg.3391_ec63', 1.8, 0.55, 31, 10, 88, 'active', '03h 40m', new Date().toISOString());
  }

  const notifCount = db.prepare('SELECT COUNT(*) as count FROM notifications').get().count;
  if (notifCount === 0) {
    console.log('[DB] Seeding notifications...');
    const insertNotif = db.prepare(`
      INSERT INTO notifications (id, event_id, severity, title, detail, timestamp, read_status, deep_link)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    insertNotif.run('notif-101', 1, 'critical', 'OSD.07 Heartbeat Timeout', 'Spinal link dropped on Rack-02 (us-east-2a). Daemon isolated from Raft quorum.', Date.now() - 2 * 60 * 1000, 0, 'topology');
    insertNotif.run('notif-102', 2, 'warning', 'Foreground I/O Governor Throttled', 'Rebalance throughput capped to 10% (450 MB/s) to protect client P99 SLA.', Date.now() - 14 * 60 * 1000, 0, 'rebalancing');
    insertNotif.run('notif-103', 3, 'resolved', 'Bit-rot Corrupted Block Repaired', 'Reed-Solomon RS(8+4) reconstructed 4KB chunk on Node-11 in 0.32s.', Date.now() - 38 * 60 * 1000, 0, 'bitrot');
    insertNotif.run('notif-104', 4, 'info', 'Raft Term 94 Quorum Verified', 'Consensus verified across Frankfurt 3/5 voter quorum (0.4ms fsync latency).', Date.now() - 75 * 60 * 1000, 1, 'metadata');
    insertNotif.run('notif-105', 5, 'info', 'CRUSH Map Epoch #4891208 Active', 'Equal weight distribution converged across European and Virginia fault zones.', Date.now() - 190 * 60 * 1000, 1, 'policies');
  }

  const auditCount = db.prepare('SELECT COUNT(*) as count FROM audit_log').get().count;
  if (auditCount === 0) {
    console.log('[DB] Seeding audit log...');
    const insertAudit = db.prepare(`
      INSERT INTO audit_log (id, timestamp, raw_timestamp, actor, action, resource, result, signature, client_ip, diff_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    insertAudit.run('audit-9481', '2026-09-26 03:14:02 UTC', Date.now() - 5 * 60 * 1000, 'op://cluster-admin', 'Policy Change', 's3://financial-audit-logs', 'Success', '0x8f19e4a01bc89a7', '10.240.0.14 (TLS-v1.3)', JSON.stringify({ before: { policy: '2x Multi-Copy', durability: '99.999%' }, after: { policy: '3x Multi-Copy', durability: '99.999999999%' } }));
    insertAudit.run('audit-9480', '2026-09-26 03:08:44 UTC', Date.now() - 10 * 60 * 1000, 'svc://rebalance-daemon', 'Rebalance Triggered', 'pg.4412_ec63', 'Success', '0x4ca7710bfe98012', 'internal://cluster.local', JSON.stringify({ source: 'node-va-04', target: 'node-va-12', shards: 4280, volumeTB: 4.2 }));
    insertAudit.run('audit-9479', '2026-09-26 02:55:12 UTC', Date.now() - 25 * 60 * 1000, 'op://cluster-admin', 'Config Change', 'cgroups.v2.io_governor', 'Success', '0x38fa091bde449aa', '10.240.0.14 (TLS-v1.3)', JSON.stringify({ parameter: 'bandwidth_limit', old: '25%', new: '10%' }));
    insertAudit.run('audit-9478', '2026-09-26 02:40:19 UTC', Date.now() - 40 * 60 * 1000, 'svc://scrub-governor', 'Repair Action', 'obj_94a7e21c:0x4F80', 'Success', '0x9920bf84210aae1', 'internal://scrubber-daemon', JSON.stringify({ incident: 'Bit-Rot Drift', repairedHash: '9f0187ba42e88a01cd2078b66e1335198ad', latency: '0.32s' }));
    insertAudit.run('audit-9477', '2026-09-26 02:18:50 UTC', Date.now() - 62 * 60 * 1000, 'sys://raft-consensus', 'Auth & Quorum', 'raft.term94.lease', 'Success', '0x7e88910bc472091', 'meta-eu-01.frankfurt', JSON.stringify({ term: 94, electedLeader: 'meta-eu-01', voters: 5 }));
  }

  const eventCount = db.prepare('SELECT COUNT(*) as count FROM events').get().count;
  if (eventCount === 0) {
    console.log('[DB] Seeding live events...');
    const insertEvent = db.prepare(`
      INSERT INTO events (type, severity, badge, title, detail, message, timestamp, related_resource, deep_link)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    insertEvent.run('heal', 'resolved', 'DEEP_SCRUB', 'Deep Scrub Verified', 'PG pg.14.12 verified 1,420 object BLAKE3 checksums (0 errors)', 'PG pg.14.12 verified 1,420 object BLAKE3 checksums (0 errors)', Date.now() - 10000, 'pg.14.12', 'bitrot');
    insertEvent.run('peer', 'info', 'RAFT_LEASE', 'Term Lease Renewed', 'Term 94 lease renewed across Frankfurt 3/5 voter quorum (0.4ms)', 'Term 94 lease renewed across Frankfurt 3/5 voter quorum (0.4ms)', Date.now() - 25000, 'meta-eu-01', 'metadata');
    insertEvent.run('warn', 'warning', 'IO_THROTTLE', 'Background Scrub Throttled', 'Background scrub throttled on OSD.04 to protect S3 ingress P99', 'Background scrub throttled on OSD.04 to protect S3 ingress P99', Date.now() - 45000, 'node-us-east-04', 'rebalancing');
    insertEvent.run('heal', 'info', 'REBALANCE', 'Payload Migrated', 'Chunk c_8812a payload migrated from node-va-01 to node-va-11', 'Chunk c_8812a payload migrated from node-va-01 to node-va-11', Date.now() - 80000, 'node-va-11', 'rebalancing');
    insertEvent.run('peer', 'resolved', 'PEER_SYNC', 'Convergence Achieved', 'Placement group pg.9011 convergence achieved in 12ms', 'Placement group pg.9011 convergence achieved in 12ms', Date.now() - 120000, 'pg.9011', 'metadata');
  }

  // Set meta defaults
  const setMeta = db.prepare('INSERT OR REPLACE INTO cluster_meta (key, value) VALUES (?, ?)');
  setMeta.run('epoch', '4891208');
  setMeta.run('health', 'HEALTH_OK');
  setMeta.run('sla', '99.999%');
  setMeta.run('throttlePct', '10');
  setMeta.run('rebalanceRunning', 'true');
}

seedDatabase();

// =========================================================================
// WEBSOCKET BROADCASTING
// =========================================================================
function broadcast(data) {
  const payload = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

// =========================================================================
// REST API ENDPOINTS
// =========================================================================

// 1. Cluster Status & KPIs
app.get('/api/cluster/status', (req) => {
  const nodes = db.prepare('SELECT * FROM nodes').all();
  const totalNodes = 128;
  const onlineNodes = nodes.filter(n => n.status !== 'down').length * 8; // 16 monitored daemons represent 128-node fabric
  const downNodes = nodes.filter(n => n.status === 'down').length;
  const rebalancingNodes = nodes.filter(n => n.status === 'rebalancing').length;
  
  const epochVal = parseInt(db.prepare('SELECT value FROM cluster_meta WHERE key = ?').get('epoch')?.value || '4891208', 10);
  const healthVal = downNodes > 0 ? (downNodes > 2 ? 'HEALTH_CRITICAL' : 'HEALTH_WARN') : 'HEALTH_OK';
  const throttleVal = parseInt(db.prepare('SELECT value FROM cluster_meta WHERE key = ?').get('throttlePct')?.value || '10', 10);
  const rebalRunning = db.prepare('SELECT value FROM cluster_meta WHERE key = ?').get('rebalanceRunning')?.value === 'true';

  req.res.json({
    health: healthVal,
    epoch: epochVal,
    totalNodes: 128,
    onlineNodes: Math.min(128, Math.max(0, 128 - downNodes * 8)),
    downNodes,
    rebalancingNodes,
    sla: '99.999%',
    qps: {
      total: 116300 + Math.round((Math.random() - 0.5) * 2000),
      read: 84200 + Math.round((Math.random() - 0.5) * 1500),
      write: 32100 + Math.round((Math.random() - 0.5) * 800)
    },
    latency: {
      p50: +(0.82 + (Math.random() - 0.5) * 0.04).toFixed(2),
      p95: +(2.14 + (Math.random() - 0.5) * 0.08).toFixed(2),
      p99: +(4.18 + (Math.random() - 0.5) * 0.12).toFixed(2)
    },
    activeConnections: 24850 + Math.round((Math.random() - 0.5) * 200),
    queueDepth: 142 + Math.round((Math.random() - 0.5) * 20),
    maxQueueDepth: 2048,
    totalObjects: '4.82B',
    totalCapacityTB: 12288,
    usedCapacityTB: 8601,
    throttlePct: throttleVal,
    rebalanceRunning: rebalRunning
  });
});

// 2. Nodes List & Detail
function findNode(idParam) {
  if (!idParam) return null;
  let node = db.prepare('SELECT * FROM nodes WHERE id = ? OR short_name = ? OR hostname = ?').get(idParam, idParam, idParam);
  if (!node) {
    const numMatch = idParam.match(/\d+/);
    if (numMatch) {
      const numSuffix = numMatch[0].length === 1 ? `0${numMatch[0]}` : numMatch[0];
      node = db.prepare('SELECT * FROM nodes WHERE id LIKE ? OR short_name LIKE ?').get(`%${numSuffix}%`, `%${numSuffix}%`);
    }
  }
  return node;
}

app.get('/api/nodes', (req, res) => {
  const nodes = db.prepare('SELECT * FROM nodes ORDER BY id ASC').all();
  res.json(nodes);
});

app.get('/api/nodes/:id', (req, res) => {
  const node = findNode(req.params.id);
  if (!node) return res.status(404).json({ error: 'Node not found' });
  res.json(node);
});

app.post('/api/nodes/:id/action', (req, res) => {
  const { action } = req.body;
  const node = findNode(req.params.id);
  if (!node) return res.status(404).json({ error: 'Node not found' });

  const now = Date.now();
  const timeStr = new Date().toISOString().replace('T', ' ').substring(0, 19) + ' UTC';

  if (action === 'scrub') {
    const eventId = db.prepare(`
      INSERT INTO events (type, severity, badge, title, detail, message, timestamp, related_resource, deep_link)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run('scrub', 'info', 'DEEP_SCRUB', `Deep Scrub Scheduled: ${node.id}`, `Cryptographic BLAKE3 scan scheduled on ${node.short_name}`, `Deep scrub scheduled for ${node.id}`, now, node.id, 'bitrot').lastInsertRowid;

    const notifId = `notif-${Date.now().toString().slice(-4)}`;
    db.prepare(`
      INSERT INTO notifications (id, event_id, severity, title, detail, timestamp, read_status, deep_link)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(notifId, eventId, 'info', `Deep Scrub on ${node.short_name}`, `BLAKE3 checksum validation queued for ${node.id}`, now, 0, 'bitrot');

    const auditId = `audit-${Date.now().toString().slice(-4)}`;
    db.prepare(`
      INSERT INTO audit_log (id, timestamp, raw_timestamp, actor, action, resource, result, signature, client_ip, diff_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(auditId, timeStr, now, 'op://cluster-admin', 'Repair Action', node.id, 'Success', '0x9920bf84210aae1', '10.240.0.14', JSON.stringify({ scanType: 'BLAKE3 Merkle Tree Scan', node: node.id }));

    broadcast({ type: 'CLUSTER_EVENT', action: 'NODE_ACTION', node: node.id, subAction: 'scrub' });
    return res.json({ success: true, message: `Deep scrub initiated on ${node.id}` });
  }

  if (action === 'drain') {
    db.prepare('UPDATE nodes SET status = ? WHERE id = ?').run('rebalancing', node.id);
    broadcast({ type: 'NODE_UPDATE', node: { ...node, status: 'rebalancing' } });
    return res.json({ success: true, message: `Node ${node.id} set to draining / rebalancing` });
  }

  if (action === 'isolate') {
    const newStatus = node.status === 'down' ? 'healthy' : 'down';
    db.prepare('UPDATE nodes SET status = ? WHERE id = ?').run(newStatus, node.id);
    broadcast({ type: 'NODE_UPDATE', node: { ...node, status: newStatus } });
    return res.json({ success: true, message: `Node ${node.id} toggled to ${newStatus}` });
  }

  res.status(400).json({ error: 'Unknown action' });
});

// 3. Durability Policies
app.get('/api/policies', (req, res) => {
  const policies = db.prepare('SELECT * FROM policies ORDER BY id DESC').all();
  res.json(policies);
});

app.post('/api/policies', (req, res) => {
  const bucketName = req.body.bucketName || req.body.bucket_name;
  const mode = req.body.mode || (req.body.replication_factor || req.body.rep ? 'replica' : 'ec');
  const k = parseInt(req.body.k || req.body.ec_k || 8, 10);
  const m = parseInt(req.body.m || req.body.ec_m || 4, 10);
  const rep = parseInt(req.body.rep || req.body.replication_factor || 3, 10);

  if (!bucketName) return res.status(400).json({ error: 'Bucket name required' });

  const durability = req.body.durability_target || (mode === 'ec' ? '99.999999999% (11 9s)' : (rep >= 3 ? '99.999999999%' : '99.999%'));
  const consistency = req.body.consistency_mode || 'Strict Raft Linearizable';
  const now = Date.now();
  const timeStr = new Date().toISOString().replace('T', ' ').substring(0, 19) + ' UTC';

  try {
    const result = db.prepare(`
      INSERT OR REPLACE INTO policies (bucket_name, mode, ec_k, ec_m, replication_factor, durability_target, consistency_mode, status, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      bucketName,
      mode,
      mode === 'ec' ? k : null,
      mode === 'ec' ? m : null,
      mode === 'replica' ? rep : null,
      durability,
      consistency,
      'ACTIVE',
      new Date().toISOString()
    );

    const auditId = `audit-${Date.now().toString().slice(-4)}`;
    db.prepare(`
      INSERT INTO audit_log (id, timestamp, raw_timestamp, actor, action, resource, result, signature, client_ip, diff_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      auditId,
      timeStr,
      now,
      'op://cluster-admin',
      'Policy Change',
      `s3://${bucketName}`,
      'Success',
      '0x' + Math.random().toString(16).slice(2, 12),
      '10.240.0.14',
      JSON.stringify({
        bucket: `s3://${bucketName}`,
        mode: mode === 'ec' ? `Erasure Coding RS ${k}+${m}` : `${rep}x Multi-Copy`,
        durability
      })
    );

    const notifId = `notif-${Date.now().toString().slice(-4)}`;
    db.prepare(`
      INSERT INTO notifications (id, event_id, severity, title, detail, timestamp, read_status, deep_link)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      notifId,
      result.lastInsertRowid,
      'info',
      `Policy Deployed: s3://${bucketName}`,
      `Configured ${mode === 'ec' ? `RS ${k}+${m}` : `${rep}x Multi-Copy`} durability target.`,
      now,
      0,
      'policies'
    );

    broadcast({ type: 'POLICY_CREATED', bucket: bucketName });
    res.json({ success: true, id: result.lastInsertRowid });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 4. Rebalancing Engine
app.get('/api/rebalance/jobs', (req, res) => {
  const jobs = db.prepare('SELECT * FROM rebalance_jobs ORDER BY id ASC').all();
  res.json({ success: true, jobs });
});

app.post('/api/rebalance/trigger', (req, res) => {
  const newJobId = `REBAL-${Math.floor(9050 + Math.random() * 500)}`;
  const now = Date.now();
  const timeStr = new Date().toISOString().replace('T', ' ').substring(0, 19) + ' UTC';

  db.prepare(`
    INSERT INTO rebalance_jobs (id, source_node, target_node, pg, total_tb, moved_tb, progress, throttle, rate, status, eta, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(newJobId, 'node-va-04 (VA)', 'node-va-12 (VA)', 'pg.4412_ec63', 5.8, 0.2, 3, 10, 120, 'active', '02h 15m', new Date().toISOString());

  const auditId = `audit-${Date.now().toString().slice(-4)}`;
  db.prepare(`
    INSERT INTO audit_log (id, timestamp, raw_timestamp, actor, action, resource, result, signature, client_ip, diff_json)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(auditId, timeStr, now, 'svc://rebalance-daemon', 'Rebalance Triggered', 'pg.4412_ec63', 'Success', '0x' + Math.random().toString(16).slice(2, 12), 'internal://cluster.local', JSON.stringify({ jobId: newJobId, volumeTB: 5.8 }));

  broadcast({ type: 'REBALANCE_TRIGGERED', jobId: newJobId, job_id: newJobId });
  res.json({ success: true, jobId: newJobId, job_id: newJobId, estimatedVolumeTB: 5.8, eta: '02h 15m' });
});

app.post('/api/rebalance/throttle', (req, res) => {
  const { throttlePct } = req.body;
  const pct = parseInt(throttlePct, 10) || 10;
  db.prepare('INSERT OR REPLACE INTO cluster_meta (key, value) VALUES (?, ?)').run('throttlePct', pct.toString());

  broadcast({ type: 'THROTTLE_CHANGE', throttlePct: pct });
  res.json({ success: true, throttlePct: pct });
});

app.post('/api/rebalance/toggle', (req, res) => {
  const current = db.prepare('SELECT value FROM cluster_meta WHERE key = ?').get('rebalanceRunning')?.value === 'true';
  const newState = !current;
  db.prepare('INSERT OR REPLACE INTO cluster_meta (key, value) VALUES (?, ?)').run('rebalanceRunning', newState.toString());

  broadcast({ type: 'REBALANCE_TOGGLE', running: newState });
  res.json({ success: true, running: newState });
});

// 5. Chaos Simulator & Recovery
app.post('/api/chaos/simulate', (req, res) => {
  const { scenario } = req.body;
  const now = Date.now();
  const timeStr = new Date().toISOString().replace('T', ' ').substring(0, 19) + ' UTC';

  if (scenario === 'kill') {
    const healthyNodes = db.prepare("SELECT * FROM nodes WHERE status != 'down'").all();
    if (healthyNodes.length === 0) return res.status(400).json({ error: 'No healthy nodes available' });
    const target = healthyNodes[Math.floor(Math.random() * healthyNodes.length)];

    db.prepare("UPDATE nodes SET status = 'down' WHERE id = ?").run(target.id);

    const eventId = db.prepare(`
      INSERT INTO events (type, severity, badge, title, detail, message, timestamp, related_resource, deep_link)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run('node', 'critical', 'NODE_DEAD', `Node Failure: ${target.id}`, `Heartbeat dropped on ${target.short_name}. Raft fence engaged.`, `Node killed: ${target.id}`, now, target.id, 'failure-sim').lastInsertRowid;

    const notifId = `notif-${Date.now().toString().slice(-4)}`;
    db.prepare(`
      INSERT INTO notifications (id, event_id, severity, title, detail, timestamp, read_status, deep_link)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(notifId, eventId, 'critical', `Storage Node Down: ${target.id}`, `Spine heartbeat failure detected on ${target.short_name}.`, now, 0, 'failure-sim');

    broadcast({ type: 'CHAOS_EVENT', scenario: 'kill', targetNode: target.id });
    return res.json({ success: true, scenario: 'kill', targetNode: target.id });
  }

  if (scenario === 'restore') {
    db.prepare("UPDATE nodes SET status = 'healthy'").run();
    broadcast({ type: 'CHAOS_EVENT', scenario: 'restore' });
    return res.json({ success: true, scenario: 'restore', message: 'All nodes restored to healthy' });
  }

  if (scenario === 'partition') {
    broadcast({ type: 'CHAOS_EVENT', scenario: 'partition' });
    return res.json({ success: true, scenario: 'partition', message: 'WAN partition simulated' });
  }

  if (scenario === 'bitrot') {
    broadcast({ type: 'CHAOS_EVENT', scenario: 'bitrot' });
    return res.json({ success: true, scenario: 'bitrot', message: 'Bit-rot forensics drill triggered' });
  }

  res.status(400).json({ error: 'Unknown scenario' });
});

// 6. Live Events
app.get('/api/events', (req, res) => {
  const { type, severity, limit = 50 } = req.query;
  let sql = 'SELECT * FROM events';
  const params = [];
  const clauses = [];

  if (type && type !== 'all') {
    clauses.push('type = ?');
    params.push(type);
  }
  if (severity && severity !== 'all') {
    clauses.push('severity = ?');
    params.push(severity);
  }

  if (clauses.length > 0) {
    sql += ' WHERE ' + clauses.join(' AND ');
  }
  sql += ' ORDER BY id DESC LIMIT ?';
  params.push(parseInt(limit, 10));

  const events = db.prepare(sql).all(...params);
  res.json(events);
});

// 7. Audit Log
app.get('/api/audit-log', (req, res) => {
  const { search, action, timeRange, result, page = 1, pageSize = 10 } = req.query;
  let sql = 'SELECT * FROM audit_log';
  const params = [];
  const clauses = [];

  if (search) {
    clauses.push('(actor LIKE ? OR action LIKE ? OR resource LIKE ? OR id LIKE ?)');
    const q = `%${search}%`;
    params.push(q, q, q, q);
  }
  if (action && action !== 'all') {
    clauses.push('action = ?');
    params.push(action);
  }
  if (result && result !== 'all') {
    clauses.push('result = ?');
    params.push(result);
  }

  if (clauses.length > 0) {
    sql += ' WHERE ' + clauses.join(' AND ');
  }

  const countSql = sql.replace('SELECT *', 'SELECT COUNT(*) as total');
  const total = db.prepare(countSql).get(...params).total;

  sql += ' ORDER BY raw_timestamp DESC LIMIT ? OFFSET ?';
  const limit = parseInt(pageSize, 10);
  const offset = (parseInt(page, 10) - 1) * limit;
  params.push(limit, offset);

  const rows = db.prepare(sql).all(...params).map(r => ({
    ...r,
    diff: r.diff_json ? JSON.parse(r.diff_json) : null
  }));

  res.json({
    total,
    page: parseInt(page, 10),
    pageSize: limit,
    totalPages: Math.ceil(total / limit),
    data: rows
  });
});

// 8. Notifications
app.get('/api/notifications', (req, res) => {
  const notifs = db.prepare('SELECT * FROM notifications ORDER BY timestamp DESC').all().map(n => ({
    ...n,
    read: n.read_status === 1
  }));
  res.json(notifs);
});

app.patch('/api/notifications/:id/read', (req, res) => {
  db.prepare('UPDATE notifications SET read_status = 1 WHERE id = ?').run(req.params.id);
  broadcast({ type: 'NOTIFICATION_READ', id: req.params.id });
  res.json({ success: true });
});

app.post('/api/notifications/mark-all-read', (req, res) => {
  db.prepare('UPDATE notifications SET read_status = 1').run();
  broadcast({ type: 'ALL_NOTIFICATIONS_READ' });
  res.json({ success: true });
});

// 9. Global Search
app.get('/api/search', (req, res) => {
  const q = (req.query.q || '').toLowerCase().trim();
  if (!q) return res.json([]);

  const nodes = db.prepare("SELECT id, short_name, status, zone, rack, disk_usage_pct FROM nodes WHERE LOWER(id) LIKE ? OR LOWER(short_name) LIKE ?").all(`%${q}%`, `%${q}%`).map(n => ({
    category: 'Nodes',
    icon: 'dns',
    title: `${n.id} (${n.short_name})`,
    subtitle: `${n.zone} · ${n.rack} · ${n.disk_usage_pct}% fill · ${n.status.toUpperCase()}`,
    actionType: 'node',
    nodeId: n.id,
    targetView: 'topology'
  }));

  const policies = db.prepare("SELECT bucket_name, mode, ec_k, ec_m, replication_factor FROM policies WHERE LOWER(bucket_name) LIKE ?").all(`%${q}%`).map(p => ({
    category: 'Placement Groups',
    icon: 'hub',
    title: `s3://${p.bucket_name}`,
    subtitle: p.mode === 'ec' ? `Erasure Coding RS ${p.ec_k}+${p.ec_m}` : `${p.replication_factor}x Multi-Copy`,
    actionType: 'view',
    targetView: 'policies'
  }));

  res.json([...nodes, ...policies]);
});

// =========================================================================
// BACKGROUND SIMULATION & HEARTBEAT ENGINE
// =========================================================================
let tickCount = 0;
setInterval(() => {
  tickCount++;

  // Advance epoch
  const epochRow = db.prepare('SELECT value FROM cluster_meta WHERE key = ?').get('epoch');
  const currentEpoch = parseInt(epochRow?.value || '4891208', 10) + 1;
  db.prepare('INSERT OR REPLACE INTO cluster_meta (key, value) VALUES (?, ?)').run('epoch', currentEpoch.toString());

  // Tick rebalancing jobs progress
  const isRebalRunning = db.prepare('SELECT value FROM cluster_meta WHERE key = ?').get('rebalanceRunning')?.value === 'true';
  if (isRebalRunning) {
    const jobs = db.prepare("SELECT * FROM rebalance_jobs WHERE status = 'active'").all();
    jobs.forEach(job => {
      let newProgress = job.progress + 1;
      let newMoved = +(job.total_tb * (newProgress / 100)).toFixed(2);
      if (newProgress >= 100) {
        newProgress = 10;
        newMoved = +(job.total_tb * 0.1).toFixed(2);
      }
      db.prepare('UPDATE rebalance_jobs SET progress = ?, moved_tb = ? WHERE id = ?').run(newProgress, newMoved, job.id);
    });
  }

  // Update heartbeats on nodes
  db.prepare("UPDATE nodes SET last_heartbeat = ? WHERE status != 'down'").run(Date.now());

  // Broadcast Telemetry Tick
  broadcast({
    type: 'TELEMETRY_TICK',
    epoch: currentEpoch,
    qpsTotal: 116300 + Math.round((Math.random() - 0.5) * 3000),
    latencyP50: +(0.82 + (Math.random() - 0.5) * 0.04).toFixed(2),
    latencyP99: +(4.18 + (Math.random() - 0.5) * 0.15).toFixed(2),
    activeConnections: 24850 + Math.round((Math.random() - 0.5) * 200),
    queueDepth: 142 + Math.round((Math.random() - 0.5) * 30)
  });
}, 1500);

// SPA Fallback for /dashboard, direct links, and all sub-routes
app.use((req, res, next) => {
  if (req.path.startsWith('/api') || req.path.startsWith('/ws')) {
    return next();
  }
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Start server on port 3000
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`[VaultMesh Server] Backend & WebSocket running on http://localhost:${PORT}`);
});
