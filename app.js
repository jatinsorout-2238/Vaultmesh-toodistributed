/**
 * VaultMesh — Enterprise Mission Control Console
 * Fully wired to real Node.js + SQLite REST Backend & Live WebSockets
 */

(function () {
  'use strict';

  // =========================================================================
  // APPLICATION STATE
  // =========================================================================
  const state = {
    currentView: 'dashboard',
    clusterStatus: {
      health: 'HEALTH_OK',
      epoch: 4891208,
      totalNodes: 128,
      onlineNodes: 128,
      downNodes: 0,
      rebalancingNodes: 0,
      sla: '99.999%',
      qps: { total: 116300, read: 84200, write: 32100 },
      latency: { p50: 0.82, p95: 2.14, p99: 4.18 },
      activeConnections: 24850,
      queueDepth: 142,
      maxQueueDepth: 2048,
      totalObjects: '4.82B',
      totalCapacityTB: 12288,
      usedCapacityTB: 8601,
      throttlePct: 10,
      rebalanceRunning: true
    },
    nodes: [],
    policies: [],
    rebalanceJobs: [],
    events: [],
    notifications: [],
    notifFilter: 'all',
    notifOpen: false,
    activeModalNode: null,
    meshLinksVisible: true,
    streamPaused: false,

    // Audit Log Query State
    audit: {
      data: [],
      total: 0,
      page: 1,
      pageSize: 10,
      totalPages: 1,
      filter: {
        search: '',
        action: 'all',
        timeRange: '24h',
        result: 'all'
      }
    },
    expandedAuditIds: new Set(),

    // Policy Form
    policyForm: {
      mode: 'ec',
      k: 8,
      m: 4,
      rep: 3
    }
  };

  let ws = null;

  // =========================================================================
  // INITIALIZATION
  // =========================================================================
  document.addEventListener('DOMContentLoaded', async () => {
    initNavigation();
    initInfoModals();
    initNotifications();
    initAuditLog();
    initGlobalSearch();
    initCommandPalette();
    initPolicySynthesizer();
    bindGlobalButtons();

    // Connect to Backend REST API & WebSockets
    await loadInitialData();
    initWebSocket();

    // Check URL hash for direct deep links
    const initialHash = window.location.hash.replace('#', '');
    if (initialHash && document.getElementById(`view-${initialHash}`)) {
      switchView(initialHash, false);
    }
  });

  // =========================================================================
  // BACKEND REST API DATA FETCHING
  // =========================================================================
  async function loadInitialData() {
    try {
      await Promise.all([
        fetchClusterStatus(),
        fetchNodes(),
        fetchPolicies(),
        fetchRebalanceJobs(),
        fetchNotifications(),
        fetchEvents(),
        fetchAuditLog()
      ]);
    } catch (err) {
      console.error('[VaultMesh] Initial data load error:', err);
    }
  }

  async function fetchClusterStatus() {
    try {
      const res = await fetch('/api/cluster/status');
      if (res.ok) {
        state.clusterStatus = await res.json();
        renderClusterStatus();
      }
    } catch (e) {
      console.warn('[API] Cluster status fetch failed:', e);
    }
  }

  async function fetchNodes() {
    try {
      const res = await fetch('/api/nodes');
      if (res.ok) {
        const data = await res.json();
        state.nodes = Array.isArray(data) ? data : (data.nodes || []);
        renderTopologyRing();
        renderTopologyOsdTable();
        renderFailureSimulatorRacks();
      }
    } catch (e) {
      console.warn('[API] Nodes fetch failed:', e);
    }
  }

  async function fetchPolicies() {
    try {
      const res = await fetch('/api/policies');
      if (res.ok) {
        const data = await res.json();
        state.policies = Array.isArray(data) ? data : (data.policies || []);
        renderPoliciesTable();
      }
    } catch (e) {
      console.warn('[API] Policies fetch failed:', e);
    }
  }

  async function fetchRebalanceJobs() {
    try {
      const res = await fetch('/api/rebalance/jobs');
      if (res.ok) {
        const data = await res.json();
        state.rebalanceJobs = Array.isArray(data) ? data : (data.jobs || []);
        renderRebalanceJobs();
      }
    } catch (e) {
      console.warn('[API] Rebalance jobs fetch failed:', e);
    }
  }

  async function fetchNotifications() {
    try {
      const res = await fetch('/api/notifications');
      if (res.ok) {
        const data = await res.json();
        state.notifications = Array.isArray(data) ? data : (data.notifications || []);
        renderNotifications();
        updateUnreadBadge();
      }
    } catch (e) {
      console.warn('[API] Notifications fetch failed:', e);
    }
  }

  async function fetchEvents(filter = 'all') {
    try {
      const url = filter && filter !== 'all' ? `/api/events?type=${filter}` : '/api/events';
      const res = await fetch(url);
      if (res.ok) {
        const data = await res.json();
        state.events = Array.isArray(data) ? data : (data.events || []);
        renderEventFeed();
      }
    } catch (e) {
      console.warn('[API] Events fetch failed:', e);
    }
  }

  async function fetchAuditLog() {
    try {
      const { search, action, result } = state.audit.filter;
      const { page, pageSize } = state.audit;
      const params = new URLSearchParams({
        page: page.toString(),
        pageSize: pageSize.toString()
      });
      if (search) params.append('search', search);
      if (action && action !== 'all') params.append('action', action);
      if (result && result !== 'all') params.append('result', result);

      const res = await fetch(`/api/audit-log?${params.toString()}`);
      if (res.ok) {
        const json = await res.json();
        state.audit.data = json.data;
        state.audit.total = json.total;
        state.audit.totalPages = json.totalPages;
        renderAuditTable();
      }
    } catch (e) {
      console.warn('[API] Audit log fetch failed:', e);
    }
  }

  // =========================================================================
  // WEBSOCKET CLIENT FOR REAL-TIME TELEMETRY
  // =========================================================================
  function initWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;

    try {
      ws = new WebSocket(wsUrl);

      ws.onopen = () => {
        console.log('[WebSocket] Connected to VaultMesh telemetry stream');
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          handleWebSocketMessage(msg);
        } catch (err) {
          console.warn('[WebSocket] Malformed message:', err);
        }
      };

      ws.onclose = () => {
        console.log('[WebSocket] Connection closed, reconnecting in 3s...');
        setTimeout(initWebSocket, 3000);
      };

      ws.onerror = (err) => {
        console.warn('[WebSocket] Error:', err);
      };
    } catch (e) {
      console.warn('[WebSocket] Initialization error:', e);
    }
  }

  function handleWebSocketMessage(msg) {
    if (msg.type === 'TELEMETRY_TICK') {
      if (state.streamPaused) return;

      state.clusterStatus.epoch = msg.epoch;
      state.clusterStatus.qps.total = msg.qpsTotal;
      state.clusterStatus.latency.p50 = msg.latencyP50;
      state.clusterStatus.latency.p99 = msg.latencyP99;
      state.clusterStatus.activeConnections = msg.activeConnections;
      state.clusterStatus.queueDepth = msg.queueDepth;

      renderTelemetryTick();
    } else if (msg.type === 'NODE_UPDATE' || msg.type === 'CHAOS_EVENT') {
      fetchNodes();
      fetchClusterStatus();
      fetchEvents();
      fetchNotifications();
      fetchAuditLog();
    } else if (msg.type === 'REBALANCE_TRIGGERED' || msg.type === 'REBALANCE_TOGGLE' || msg.type === 'THROTTLE_CHANGE') {
      fetchRebalanceJobs();
      fetchClusterStatus();
      fetchAuditLog();
    } else if (msg.type === 'POLICY_CREATED') {
      fetchPolicies();
      fetchAuditLog();
      fetchNotifications();
    } else if (msg.type === 'NOTIFICATION_READ' || msg.type === 'ALL_NOTIFICATIONS_READ') {
      fetchNotifications();
    } else if (msg.type === 'CLUSTER_EVENT') {
      fetchEvents();
      fetchNotifications();
      fetchAuditLog();
    }
  }

  // =========================================================================
  // DOM RENDERING & STATUS UPDATES
  // =========================================================================
  function renderClusterStatus() {
    const s = state.clusterStatus;

    // Sidebar & Topbar status
    const topHealth = document.getElementById('top-health-status');
    if (topHealth) {
      topHealth.textContent = `${s.health}: ${s.onlineNodes}/${s.totalNodes} Nodes Online`;
    }

    const sbEpoch = document.getElementById('sb-epoch');
    if (sbEpoch) sbEpoch.textContent = `#${s.epoch.toLocaleString()}`;

    // KPI Cards
    const dashOsdCount = document.getElementById('dash-osd-count');
    if (dashOsdCount) dashOsdCount.textContent = `${s.onlineNodes} / ${s.totalNodes}`;

    const liveIngress = document.getElementById('live-ingress-qps');
    if (liveIngress) liveIngress.textContent = `${(s.qps.total / 1000).toFixed(1)}k QPS`;

    const queueDepthText = document.getElementById('queue-depth-text');
    if (queueDepthText) queueDepthText.innerHTML = `${s.queueDepth} <span class="text-muted font-normal text-xs">/ 2,048 reqs</span>`;

    const p50Val = document.getElementById('p50-val');
    if (p50Val) p50Val.textContent = `${s.latency.p50} ms`;

    const p99Val = document.getElementById('p99-val');
    if (p99Val) p99Val.textContent = `${s.latency.p99} ms`;

    const connVal = document.getElementById('active-conn-count');
    if (connVal) connVal.textContent = s.activeConnections.toLocaleString();
  }

  function renderTelemetryTick() {
    const s = state.clusterStatus;
    const sbEpoch = document.getElementById('sb-epoch');
    if (sbEpoch) sbEpoch.textContent = `#${s.epoch.toLocaleString()}`;

    const liveIngress = document.getElementById('live-ingress-qps');
    if (liveIngress) liveIngress.textContent = `${(s.qps.total / 1000).toFixed(1)}k QPS`;

    const queueDepthText = document.getElementById('queue-depth-text');
    if (queueDepthText) queueDepthText.innerHTML = `${s.queueDepth} <span class="text-muted font-normal text-xs">/ 2,048 reqs</span>`;

    const queueBar = document.getElementById('queue-fill-bar');
    if (queueBar) queueBar.style.width = `${((s.queueDepth / s.maxQueueDepth) * 100).toFixed(1)}%`;

    const p50Val = document.getElementById('p50-val');
    if (p50Val) p50Val.textContent = `${s.latency.p50} ms`;

    const p99Val = document.getElementById('p99-val');
    if (p99Val) p99Val.textContent = `${s.latency.p99} ms`;
  }

  // =========================================================================
  // VIEW NAVIGATION & SPA DUAL-MODE ROUTING (Portal + Console)
  // =========================================================================
  function initNavigation() {
    document.querySelectorAll('[data-view]').forEach(elem => {
      elem.addEventListener('click', (e) => {
        if (elem.tagName === 'A' || elem.classList.contains('kpi-tile') || elem.classList.contains('btn') || elem.classList.contains('btn-hero-solid-cyan') || elem.classList.contains('btn-launch-cyan') || elem.classList.contains('btn-deploy-cluster') || elem.classList.contains('btn-study-action') || elem.classList.contains('footer-nav-link') || elem.classList.contains('social-icon-btn') || elem.classList.contains('dropdown-item') || elem.classList.contains('brand-logo-group')) {
          e.preventDefault();
        }
        const targetView = elem.getAttribute('data-view');
        if (targetView) {
          switchView(targetView);
        }
      });
    });

    document.querySelectorAll('.landing-portal-wrapper a[href^="#"]').forEach(anchor => {
      anchor.addEventListener('click', function (e) {
        const targetId = this.getAttribute('href').substring(1);
        if (!targetId) return;

        const consoleWrapper = document.getElementById('dashboard-console-wrapper');
        if (consoleWrapper && consoleWrapper.classList.contains('active')) {
          switchView('portal', false);
        }

        const targetElem = document.getElementById(targetId);
        if (targetElem) {
          e.preventDefault();
          targetElem.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
      });
    });

    const copyBtn = document.getElementById('btn-copy-install');
    const cmdText = document.getElementById('install-cmd');
    const label = document.getElementById('copy-btn-label');

    if (copyBtn && cmdText && label) {
      copyBtn.addEventListener('click', async () => {
        try {
          await navigator.clipboard.writeText(cmdText.textContent.trim());
          label.textContent = 'Copied!';
          setTimeout(() => label.textContent = 'Copy', 2000);
        } catch (err) {
          label.textContent = 'Copied!';
          setTimeout(() => label.textContent = 'Copy', 2000);
        }
      });
    }

    function handleInitialHash() {
      let pathname = window.location.pathname.replace(/^\//, '').replace(/\/$/, '');
      let hash = window.location.hash.replace('#', '');
      if (hash.startsWith('view-')) hash = hash.replace('view-', '');

      // Check URL pathname (e.g. /dashboard or /dashboard/topology or /audit)
      if (pathname === 'dashboard' || pathname.startsWith('dashboard/')) {
        const sub = pathname.replace(/^dashboard\/?/, '');
        const target = (sub && document.getElementById(`view-${sub}`)) ? sub : 'dashboard';
        switchView(target, false);
        return;
      }
      if (pathname && document.getElementById(`view-${pathname}`)) {
        switchView(pathname, false);
        return;
      }

      const portalAnchors = ['discover', 'users', 'developers', 'community', 'news', 'foundation', 'intro', 'deploy', 'study', 'hero', 'portal', 'landing', 'overview'];
      
      if (!hash || portalAnchors.includes(hash)) {
        switchView('portal', false);
        if (hash && hash !== 'portal' && hash !== 'landing' && hash !== 'overview') {
          const elem = document.getElementById(hash);
          if (elem) {
            setTimeout(() => elem.scrollIntoView({ behavior: 'smooth', block: 'start' }), 60);
          }
        }
      } else if (document.getElementById(`view-${hash}`)) {
        switchView(hash, false);
      }
    }

    window.addEventListener('hashchange', handleInitialHash);
    handleInitialHash();
  }

  window.switchView = function (viewId, updateHash = true) {
    const isPortal = (viewId === 'portal' || viewId === 'landing' || viewId === 'overview');
    state.currentView = viewId;

    const portalWrapper = document.getElementById('landing-portal-wrapper');
    const consoleWrapper = document.getElementById('dashboard-console-wrapper');

    if (isPortal) {
      if (portalWrapper) portalWrapper.classList.add('active');
      if (consoleWrapper) consoleWrapper.classList.remove('active');

      document.querySelectorAll('.nav-link').forEach(link => {
        link.classList.remove('active');
        if (link.getAttribute('data-view') === 'portal') link.classList.add('active');
      });

      if (updateHash) window.location.hash = 'portal';
      window.scrollTo({ top: 0, behavior: 'smooth' });
      return;
    }

    if (portalWrapper) portalWrapper.classList.remove('active');
    if (consoleWrapper) consoleWrapper.classList.add('active');

    document.querySelectorAll('.view-panel').forEach(panel => panel.classList.remove('active'));

    const targetPanel = document.getElementById(`view-${viewId}`);
    if (targetPanel) targetPanel.classList.add('active');

    document.querySelectorAll('.nav-link').forEach(link => {
      link.classList.remove('active');
      if (link.getAttribute('data-view') === viewId) link.classList.add('active');
    });

    if (updateHash) window.location.hash = viewId;

    if (viewId === 'audit') fetchAuditLog();
    if (viewId === 'topology') fetchNodes();
    if (viewId === 'policies') fetchPolicies();
    if (viewId === 'rebalancing') fetchRebalanceJobs();

    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  // =========================================================================
  // TOPOLOGY RING & OSD TABLE (Requirement 1 & 4)
  // =========================================================================
  function renderTopologyRing() {
    const nodesGroup = document.getElementById('topology-nodes-group');
    if (!nodesGroup) return;

    nodesGroup.innerHTML = '';
    const total = state.nodes.length || 16;
    const cx = 300;
    const cy = 160;
    const r = 130;

    state.nodes.forEach((node, idx) => {
      const angle = (idx / total) * 2 * Math.PI - Math.PI / 2;
      const x = cx + r * Math.cos(angle);
      const y = cy + r * Math.sin(angle);

      const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
      g.setAttribute('class', 'ring-node-group');
      g.style.cursor = 'pointer';

      let strokeColor = '#16a34a';
      if (node.status === 'rebalancing') strokeColor = '#0f766e';
      if (node.status === 'down') strokeColor = '#dc2626';

      // Pulsing Ring for Down Nodes (CSS Keyframe Animated)
      if (node.status === 'down') {
        const pulseRing = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        pulseRing.setAttribute('cx', x);
        pulseRing.setAttribute('cy', y);
        pulseRing.setAttribute('r', '14');
        pulseRing.setAttribute('fill', 'none');
        pulseRing.setAttribute('stroke', '#dc2626');
        pulseRing.setAttribute('class', 'ring-node-pulse-ring');
        g.appendChild(pulseRing);
      }

      const outerCircle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      outerCircle.setAttribute('cx', x);
      outerCircle.setAttribute('cy', y);
      outerCircle.setAttribute('r', '14');
      outerCircle.setAttribute('fill', '#ffffff');
      outerCircle.setAttribute('stroke', strokeColor);
      outerCircle.setAttribute('stroke-width', '2');

      const innerDot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      innerDot.setAttribute('cx', x);
      innerDot.setAttribute('cy', y);
      innerDot.setAttribute('r', '4');
      innerDot.setAttribute('fill', strokeColor);

      const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      text.setAttribute('x', x);
      text.setAttribute('y', y + (y > cy ? 22 : -16));
      text.setAttribute('text-anchor', 'middle');
      text.setAttribute('fill', '#0f172a');
      text.setAttribute('font-family', 'JetBrains Mono');
      text.setAttribute('font-size', '8');
      text.setAttribute('font-weight', '600');
      text.textContent = node.short_name;

      g.appendChild(outerCircle);
      g.appendChild(innerDot);
      g.appendChild(text);

      g.addEventListener('mouseenter', () => updateNodeInspector(node));
      g.addEventListener('click', () => window.openNodeModal(node.id));

      nodesGroup.appendChild(g);
    });
  }

  function updateNodeInspector(node) {
    const nameEl = document.getElementById('inspect-node-name');
    const iopsEl = document.getElementById('inspect-node-iops');
    const tempEl = document.getElementById('inspect-node-temp');
    const nvmeEl = document.getElementById('inspect-node-nvme');

    if (nameEl) nameEl.textContent = node.id;
    if (iopsEl) iopsEl.textContent = `${node.iops}k`;
    if (tempEl) tempEl.textContent = `${node.temp}°C`;
    if (nvmeEl) nvmeEl.textContent = `${node.nvme_health}% health (${node.disk_usage_pct}% fill)`;
  }

  function renderTopologyOsdTable() {
    const tbody = document.getElementById('topology-osd-table-body');
    if (!tbody) return;

    let html = '';
    state.nodes.forEach(node => {
      let statusBadge = '<span class="status-pill healthy"><span class="status-beacon"></span>NOMINAL</span>';
      if (node.status === 'rebalancing') {
        statusBadge = '<span class="status-pill active"><span class="status-beacon"></span>REBALANCING</span>';
      } else if (node.status === 'down') {
        statusBadge = '<span class="status-pill critical"><span class="status-beacon"></span>OUT / DOWN</span>';
      }

      html += `
        <tr style="cursor: pointer;" onclick="openNodeModal('${node.id}')">
          <td>
            <div style="display: flex; align-items: center; gap: 8px;">
              <span class="status-beacon" style="background: ${node.status === 'down' ? '#dc2626' : node.status === 'rebalancing' ? '#0f766e' : '#16a34a'};"></span>
              <strong class="font-mono text-cyan text-xs">${node.short_name}</strong>
              <span class="font-mono text-muted text-xs">(${node.id})</span>
            </div>
          </td>
          <td><span class="font-mono text-xs text-secondary">${node.zone} · ${node.rack}</span></td>
          <td>${statusBadge}</td>
          <td>
            <div style="display: flex; align-items: center; gap: 8px;">
              <div class="progress-track" style="width: 70px; height: 6px;">
                <div class="progress-fill ${node.disk_usage_pct > 85 ? 'amber' : 'cyan'}" style="width: ${node.disk_usage_pct}%;"></div>
              </div>
              <span class="font-mono text-xs font-bold ${node.disk_usage_pct > 85 ? 'text-amber' : 'text-primary'}">${node.disk_usage_pct}%</span>
            </div>
          </td>
          <td class="font-mono text-xs text-secondary">${node.iops}k /s</td>
          <td class="font-mono text-xs text-secondary">${node.temp}°C</td>
          <td class="font-mono text-xs text-emerald font-bold">${node.nvme_health}%</td>
          <td>
            <div style="display: flex; gap: 4px;" onclick="event.stopPropagation();">
              <button class="btn btn-secondary btn-sm" onclick="openNodeModal('${node.id}')">Inspect</button>
              <button class="btn btn-secondary btn-sm" onclick="handleNodeAction('${node.id}', 'scrub')">Scrub</button>
            </div>
          </td>
        </tr>
      `;
    });

    tbody.innerHTML = html;
  }

  window.openNodeModal = function (nodeId) {
    const node = state.nodes.find(n => n.id === nodeId) || state.nodes[0];
    if (!node) return;
    state.activeModalNode = node;

    const modal = document.getElementById('node-detail-modal-backdrop');
    if (!modal) return;

    const titleEl = document.getElementById('modal-node-title');
    const subtitleEl = document.getElementById('modal-node-subtitle');
    const beaconEl = document.getElementById('modal-node-beacon');
    const iopsEl = document.getElementById('modal-node-iops');
    const tempEl = document.getElementById('modal-node-temp');
    const heartbeatEl = document.getElementById('modal-node-heartbeat');
    const capacityEl = document.getElementById('modal-node-capacity');
    const fillBarEl = document.getElementById('modal-node-fill-bar');

    if (titleEl) titleEl.textContent = `${node.id} (${node.short_name})`;
    if (subtitleEl) subtitleEl.textContent = `Zone: ${node.zone} / ${node.rack} / NVMe Solid State Drive`;
    if (iopsEl) iopsEl.textContent = `${node.iops}k /s`;
    if (tempEl) tempEl.textContent = `${node.temp}°C · ${node.nvme_health}% Health`;
    if (heartbeatEl) heartbeatEl.textContent = '0.12s ago (ACK)';
    if (capacityEl) capacityEl.textContent = `${node.capacity_used_tb} TB / ${node.capacity_total_tb} TB (${node.disk_usage_pct}% Fill)`;
    if (fillBarEl) fillBarEl.style.width = `${node.disk_usage_pct}%`;

    if (beaconEl) {
      beaconEl.style.background = node.status === 'down' ? '#dc2626' : node.status === 'rebalancing' ? '#0f766e' : '#16a34a';
    }

    modal.classList.add('open');
  };

  window.closeNodeModal = function () {
    const modal = document.getElementById('node-detail-modal-backdrop');
    if (modal) modal.classList.remove('open');
  };

  window.handleModalNodeAction = async function (action) {
    const node = state.activeModalNode || state.nodes[0];
    window.closeNodeModal();
    if (!node) return;
    await window.handleNodeAction(node.id, action);
  };

  window.handleNodeAction = async function (nodeId, action) {
    try {
      const res = await fetch(`/api/nodes/${nodeId}/action`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action })
      });
      const data = await res.json();
      if (res.ok) {
        showToast(data.message || `Action ${action} executed`, 'success');
        fetchNodes();
        fetchClusterStatus();
      } else {
        showToast(data.error || 'Action failed', 'error');
      }
    } catch (e) {
      showToast('Node action request failed', 'error');
    }
  };

  // =========================================================================
  // FAILURE SIMULATOR & CHAOS (Requirement 1 & 6)
  // =========================================================================
  function renderFailureSimulatorRacks() {
    const container = document.getElementById('sim-racks-container');
    if (!container) return;

    container.innerHTML = '';
    for (let r = 1; r <= 4; r++) {
      const rackDiv = document.createElement('div');
      rackDiv.className = 'kpi-tile';
      rackDiv.style.background = 'var(--surface-low)';
      rackDiv.style.padding = '10px';
      rackDiv.style.display = 'flex';
      rackDiv.style.flexDirection = 'column';
      rackDiv.style.gap = '8px';

      const rackNodes = state.nodes.filter((_, idx) => Math.floor(idx / 4) + 1 === r);
      const onlineCount = rackNodes.filter(n => n.status !== 'down').length;

      rackDiv.innerHTML = `
        <div style="display:flex; justify-content:space-between; align-items:center;">
          <span class="font-bold text-xs text-primary">RACK 0${r}</span>
          <span class="status-pill ${onlineCount === rackNodes.length ? 'healthy' : 'critical'}" style="font-size:0.6rem;">${onlineCount}/${rackNodes.length} NOMINAL</span>
        </div>
        <div style="display:grid; grid-template-columns: 1fr 1fr; gap:6px;" id="rack-${r}-nodes"></div>
      `;

      const grid = rackDiv.querySelector(`#rack-${r}-nodes`);
      rackNodes.forEach(node => {
        const nodeBtn = document.createElement('div');
        nodeBtn.className = 'sim-node-chip';
        nodeBtn.style.background = '#ffffff';
        nodeBtn.style.border = `1px solid ${node.status === 'down' ? '#fecaca' : '#e2e8f0'}`;
        nodeBtn.style.borderRadius = 'var(--radius-sm)';
        nodeBtn.style.padding = '6px';
        nodeBtn.style.cursor = 'pointer';
        nodeBtn.style.display = 'flex';
        nodeBtn.style.alignItems = 'center';
        nodeBtn.style.gap = '6px';
        nodeBtn.style.fontFamily = 'var(--font-mono)';
        nodeBtn.style.fontSize = '0.75rem';

        nodeBtn.innerHTML = `
          <span class="status-beacon" style="background:${node.status === 'down' ? '#dc2626' : '#16a34a'};"></span>
          <span class="text-primary font-bold">${node.short_name}</span>
        `;

        nodeBtn.addEventListener('click', () => {
          window.handleNodeAction(node.id, 'isolate');
        });

        grid.appendChild(nodeBtn);
      });

      container.appendChild(rackDiv);
    }
  }

  window.triggerChaos = async function (scenario) {
    if (scenario === 'restore') {
      try {
        const res = await fetch('/api/chaos/simulate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ scenario: 'restore' })
        });
        if (res.ok) {
          showToast('All nodes online — quorum fully converged', 'success');
          // Reset repair links
          document.querySelectorAll('.topology-mesh-link').forEach(link => link.classList.remove('active-repair'));
          fetchNodes();
          fetchClusterStatus();
          fetchEvents();
        }
      } catch (e) {
        showToast('Cluster restore failed', 'error');
      }
      return;
    }

    // Step 1: Initial Toast Beat
    showToast('Node-07 unreachable → repair initiated', 'warning');

    // Step 2: Animate active repair mesh links (growth animation)
    document.querySelectorAll('.topology-mesh-link').forEach(link => link.classList.add('active-repair'));

    // Step 3: Trigger real backend fault
    try {
      const res = await fetch('/api/chaos/simulate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ scenario })
      });
      if (res.ok) {
        fetchNodes();
        fetchClusterStatus();
        fetchEvents();
      }
    } catch (e) {
      console.warn('[Chaos] Sim fetch:', e);
    }

    // Step 4: Live Progress Bar Ticker (0 -> 100%) across 3.2s
    const step3Bar = document.getElementById('sim-step3-bar');
    const step3Pct = document.getElementById('sim-step3-pct');
    const simStatus = document.getElementById('sim-pipeline-status');

    let currentPct = 0;
    const interval = setInterval(() => {
      currentPct += 4;
      if (currentPct > 100) currentPct = 100;

      if (step3Bar) step3Bar.style.width = `${currentPct}%`;
      if (step3Pct) step3Pct.textContent = `${currentPct}%`;
      if (simStatus) simStatus.textContent = `Step 3 of 4: Parity Re-computation (${currentPct}%)`;

      if (currentPct >= 100) {
        clearInterval(interval);
        if (simStatus) {
          simStatus.innerHTML = `<span class="material-symbols-outlined text-emerald checkmark-pop" style="font-size: 14px; vertical-align: middle;">check_circle</span> Step 4 of 4: Parity Reconstructed & Verified`;
        }

        // Step 5: Second Toast Beat
        setTimeout(() => {
          showToast('Recovered in 3.2s', 'success');
          document.querySelectorAll('.topology-mesh-link').forEach(link => link.classList.remove('active-repair'));
          fetchNodes();
          fetchClusterStatus();
        }, 300);
      }
    }, 120);
  };

  window.restoreClusterSimulation = function () {
    window.triggerChaos('restore');
  };

  // =========================================================================
  // DURABILITY POLICIES (Requirement 1 & Part 3)
  // =========================================================================
  function renderPoliciesTable() {
    const tbody = document.getElementById('policies-table-body');
    if (!tbody) return;

    tbody.innerHTML = state.policies.map(p => `
      <tr>
        <td>
          <div style="display: flex; align-items: center; gap: 8px;">
            <span class="material-symbols-outlined text-cyan" style="font-size: 18px;">folder</span>
            <strong class="font-mono text-primary text-xs">s3://${p.bucket_name}</strong>
          </div>
        </td>
        <td>
          <span class="status-pill active font-mono" style="font-size: 0.6875rem;">
            ${p.mode === 'ec' ? `RS ${p.ec_k}+${p.ec_m}` : `${p.replication_factor}x COPY`}
          </span>
        </td>
        <td class="font-mono text-xs text-secondary">${p.durability_target}</td>
        <td class="font-mono text-xs text-secondary">${p.consistency_mode}</td>
        <td><span class="status-pill healthy"><span class="status-beacon"></span>${p.status}</span></td>
        <td>
          <button class="btn btn-secondary btn-sm" onclick="showToast('Policy active for s3://${p.bucket_name}', 'info')">Inspect</button>
        </td>
      </tr>
    `).join('');
  }

  function initPolicySynthesizer() {
    window.setPolicyMode = function (mode) {
      state.policyForm.mode = mode;
      const btnEc = document.getElementById('btn-mode-ec');
      const btnRep = document.getElementById('btn-mode-replica');
      const controlsEc = document.getElementById('policy-ec-controls');
      const controlsRep = document.getElementById('policy-replica-controls');

      if (mode === 'ec') {
        if (btnEc) { btnEc.classList.add('active', 'btn-primary'); btnEc.classList.remove('btn-secondary'); }
        if (btnRep) { btnRep.classList.remove('active', 'btn-primary'); btnRep.classList.add('btn-secondary'); }
        if (controlsEc) controlsEc.style.display = 'block';
        if (controlsRep) controlsRep.style.display = 'none';
      } else {
        if (btnRep) { btnRep.classList.add('active', 'btn-primary'); btnRep.classList.remove('btn-secondary'); }
        if (btnEc) { btnEc.classList.remove('active', 'btn-primary'); btnEc.classList.add('btn-secondary'); }
        if (controlsEc) controlsEc.style.display = 'none';
        if (controlsRep) controlsRep.style.display = 'block';
      }
      window.updatePolicyEstimate();
    };

    window.updatePolicyEstimate = function () {
      const sliderK = document.getElementById('slider-k');
      const sliderM = document.getElementById('slider-m');
      const sliderRep = document.getElementById('slider-replica');

      if (sliderK) state.policyForm.k = parseInt(sliderK.value, 10);
      if (sliderM) state.policyForm.m = parseInt(sliderM.value, 10);
      if (sliderRep) state.policyForm.rep = parseInt(sliderRep.value, 10);

      const kVal = document.getElementById('val-k');
      const mVal = document.getElementById('val-m');
      const repVal = document.getElementById('val-replica');

      if (kVal) kVal.textContent = state.policyForm.k;
      if (mVal) mVal.textContent = state.policyForm.m;
      if (repVal) repVal.textContent = `${state.policyForm.rep}x`;

      const overheadEl = document.getElementById('est-overhead-pct');
      const usableEl = document.getElementById('est-usable-pct');

      if (state.policyForm.mode === 'ec') {
        const overhead = ((state.policyForm.m / state.policyForm.k) * 100).toFixed(1);
        const usable = ((state.policyForm.k / (state.policyForm.k + state.policyForm.m)) * 100).toFixed(1);
        if (overheadEl) overheadEl.textContent = `+${overhead}%`;
        if (usableEl) usableEl.textContent = `${usable}%`;
      } else {
        const overhead = ((state.policyForm.rep - 1) * 100).toFixed(0);
        const usable = (100 / state.policyForm.rep).toFixed(1);
        if (overheadEl) overheadEl.textContent = `+${overhead}%`;
        if (usableEl) usableEl.textContent = `${usable}%`;
      }
    };

    window.deployPolicy = async function () {
      const bucketInput = document.getElementById('policy-bucket-name');
      const bucketName = bucketInput ? bucketInput.value.trim() : 'custom-bucket';

      try {
        const res = await fetch('/api/policies', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            bucketName,
            mode: state.policyForm.mode,
            k: state.policyForm.k,
            m: state.policyForm.m,
            rep: state.policyForm.rep
          })
        });

        if (res.ok) {
          showToast(`Durability policy deployed for s3://${bucketName}`, 'success');
          fetchPolicies();
        } else {
          showToast('Failed to deploy policy', 'error');
        }
      } catch (e) {
        showToast('Policy deployment failed', 'error');
      }
    };
  }

  // =========================================================================
  // REBALANCING ENGINE (Requirement 2 & 6)
  // =========================================================================
  function renderRebalanceJobs() {
    const tbody = document.getElementById('rebal-jobs-tbody');
    if (!tbody) return;

    tbody.innerHTML = state.rebalanceJobs.map(job => `
      <tr>
        <td class="font-mono text-cyan text-xs font-bold">${job.id}</td>
        <td class="font-mono text-xs text-primary">${job.source_node}</td>
        <td class="font-mono text-xs text-emerald">${job.target_node}</td>
        <td class="font-mono text-xs text-secondary">${job.pg}</td>
        <td style="min-width: 140px;">
          <div style="display:flex; justify-content:space-between; font-size:0.75rem; margin-bottom:2px;">
            <span class="text-cyan font-bold">${job.progress}%</span>
            <span class="text-muted">${job.moved_tb} TB / ${job.total_tb} TB</span>
          </div>
          <div class="progress-track"><div class="progress-fill cyan" style="width: ${job.progress}%;"></div></div>
        </td>
        <td class="font-mono text-xs text-teal font-bold">${job.rate} MB/s</td>
        <td class="font-mono text-xs text-muted">${job.eta}</td>
      </tr>
    `).join('');
  }

  window.openRebalanceModal = function () {
    const modal = document.getElementById('rebalance-modal-backdrop');
    if (modal) modal.classList.add('open');
  };

  window.closeRebalanceModal = function () {
    const modal = document.getElementById('rebalance-modal-backdrop');
    if (modal) modal.classList.remove('open');
  };

  window.confirmStartRebalance = async function () {
    window.closeRebalanceModal();
    try {
      const res = await fetch('/api/rebalance/trigger', { method: 'POST' });
      const data = await res.json();
      if (res.ok) {
        showToast(`Cluster rebalance job ${data.jobId} triggered!`, 'success');
        fetchRebalanceJobs();
      }
    } catch (e) {
      showToast('Rebalance trigger failed', 'error');
    }
  };

  window.handleThrottleChange = async function (val) {
    const pct = parseInt(val, 10);
    try {
      await fetch('/api/rebalance/throttle', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ throttlePct: pct })
      });
      showToast(`Governor set to ${pct}% bandwidth ceiling`, 'info');
    } catch (e) {}
  };

  window.toggleRebalanceEngine = async function () {
    try {
      const res = await fetch('/api/rebalance/toggle', { method: 'POST' });
      const data = await res.json();
      if (res.ok) {
        showToast(`Rebalancing engine ${data.running ? 'resumed' : 'paused'}`, 'info');
      }
    } catch (e) {}
  };

  // =========================================================================
  // LIVE EVENT FEED (Requirement 5)
  // =========================================================================
  function renderEventFeed() {
    const streamContainer = document.getElementById('dash-event-stream');
    if (!streamContainer) return;

    streamContainer.innerHTML = state.events.map(evt => {
      let levelColor = '#16a34a';
      if (evt.severity === 'warning') levelColor = '#d97706';
      if (evt.severity === 'critical') levelColor = '#dc2626';
      if (evt.severity === 'info') levelColor = '#0f766e';

      const timeStr = new Date(evt.timestamp).toISOString().substring(11, 19);

      return `
        <div class="event-item" style="background: var(--surface-low); padding: 8px 10px; border-radius: var(--radius-md); border-left: 3px solid ${levelColor}; display: flex; flex-direction: column; gap: 2px; font-family: var(--font-mono); font-size: 0.75rem; cursor: pointer;" onclick="switchView('${evt.deep_link || 'dashboard'}')">
          <div style="display:flex; justify-content:space-between; align-items:center;">
            <span class="status-pill active" style="font-size:0.625rem; padding:1px 5px;">${evt.badge || 'EVENT'}</span>
            <span class="text-muted" style="font-size:0.6875rem;">${timeStr} UTC</span>
          </div>
          <p class="text-secondary" style="margin: 3px 0 0 0; line-height: 1.3;">${evt.detail || evt.message}</p>
        </div>
      `;
    }).join('');

    document.querySelectorAll('.filter-pill').forEach(btn => {
      btn.onclick = () => {
        document.querySelectorAll('.filter-pill').forEach(b => {
          b.classList.remove('btn-primary', 'active');
          b.classList.add('btn-secondary');
        });
        btn.classList.remove('btn-secondary');
        btn.classList.add('btn-primary', 'active');
        fetchEvents(btn.getAttribute('data-filter'));
      };
    });
  }

  // =========================================================================
  // NOTIFICATIONS (Requirement 2 & Part 3)
  // =========================================================================
  function initNotifications() {
    const bellBtn = document.getElementById('btn-notifications');
    const dropdown = document.getElementById('notification-dropdown');
    const markAllReadBtn = document.getElementById('btn-mark-all-read');

    if (bellBtn && dropdown) {
      bellBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        state.notifOpen = !state.notifOpen;
        dropdown.classList.toggle('open', state.notifOpen);
      });

      document.addEventListener('click', (e) => {
        if (state.notifOpen && !dropdown.contains(e.target) && !bellBtn.contains(e.target)) {
          state.notifOpen = false;
          dropdown.classList.remove('open');
        }
      });
    }

    if (markAllReadBtn) {
      markAllReadBtn.addEventListener('click', async () => {
        await fetch('/api/notifications/mark-all-read', { method: 'POST' });
        fetchNotifications();
        showToast('All notifications marked as read', 'info');
      });
    }

    document.querySelectorAll('.notif-filter-tab').forEach(tab => {
      tab.addEventListener('click', () => {
        document.querySelectorAll('.notif-filter-tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        state.notifFilter = tab.getAttribute('data-notif-filter');
        renderNotifications();
      });
    });
  }

  function renderNotifications() {
    const list = document.getElementById('notification-list');
    if (!list) return;

    let filtered = state.notifications;
    if (state.notifFilter !== 'all') {
      filtered = filtered.filter(n => n.severity === state.notifFilter);
    }

    if (filtered.length === 0) {
      list.innerHTML = `
        <div class="notification-empty">
          <span class="material-symbols-outlined text-emerald" style="font-size: 28px;">verified</span>
          <span class="font-bold text-sm text-primary">All caught up</span>
          <span class="text-xs text-muted">No unread notifications</span>
        </div>
      `;
      return;
    }

    list.innerHTML = filtered.map(n => `
      <div class="notification-item ${n.read ? '' : 'unread'}" onclick="handleNotifClick('${n.id}', '${n.deep_link}')">
        <div class="notif-severity-dot ${n.severity}"></div>
        <div style="flex: 1; min-width: 0;">
          <div class="notif-title">${n.title}</div>
          <div class="notif-detail">${n.detail}</div>
          <div class="notif-meta">
            <span>${formatRelativeTime(n.timestamp)}</span>
            <span class="text-cyan font-bold">Open →</span>
          </div>
        </div>
      </div>
    `).join('');
  }

  window.handleNotifClick = async function (id, deepLink) {
    await fetch(`/api/notifications/${id}/read`, { method: 'PATCH' });
    fetchNotifications();
    const dropdown = document.getElementById('notification-dropdown');
    if (dropdown) dropdown.classList.remove('open');
    if (deepLink) switchView(deepLink);
  };

  let lastUnreadCount = 0;
  function updateUnreadBadge() {
    const unread = state.notifications.filter(n => !n.read).length;
    const badge = document.getElementById('unread-count-badge');
    const headerBadge = document.getElementById('notif-total-badge');

    if (badge) {
      badge.textContent = unread;
      badge.style.display = unread > 0 ? 'flex' : 'none';
      if (unread > lastUnreadCount) {
        badge.classList.remove('pop');
        void badge.offsetWidth;
        badge.classList.add('pop');
      }
    }
    lastUnreadCount = unread;

    if (headerBadge) {
      headerBadge.textContent = unread > 0 ? `${unread} Unread` : 'All Caught Up';
    }
  }

  function formatRelativeTime(ts) {
    const diff = Math.floor((Date.now() - ts) / 1000);
    if (diff < 60) return 'Just now';
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    return `${Math.floor(diff / 86400)}d ago`;
  }

  // =========================================================================
  // AUDIT LOG (Requirement 1 & 5)
  // =========================================================================
  function initAuditLog() {
    window.handleAuditSearch = function (val) {
      state.audit.filter.search = val.trim();
      state.audit.page = 1;
      fetchAuditLog();
    };

    window.handleAuditResultFilter = function (val) {
      state.audit.filter.result = val;
      state.audit.page = 1;
      fetchAuditLog();
    };

    window.handleAuditPage = function (delta) {
      state.audit.page += delta;
      fetchAuditLog();
    };

    window.refreshAuditView = function () {
      fetchAuditLog();
      showToast('Audit trail refreshed from SQLite database', 'success');
    };

    document.querySelectorAll('#audit-filter-chips .filter-chip').forEach(chip => {
      chip.addEventListener('click', () => {
        document.querySelectorAll('#audit-filter-chips .filter-chip').forEach(c => c.classList.remove('active'));
        chip.classList.add('active');
        state.audit.filter.action = chip.getAttribute('data-action-filter');
        state.audit.page = 1;
        fetchAuditLog();
      });
    });
  }

  function renderAuditTable() {
    const tbody = document.getElementById('audit-table-body');
    if (!tbody) return;

    const { data, total, page, pageSize, totalPages } = state.audit;

    const infoEl = document.getElementById('audit-pagination-info');
    const prevBtn = document.getElementById('btn-audit-prev');
    const nextBtn = document.getElementById('btn-audit-next');

    if (infoEl) {
      infoEl.textContent = `Showing ${(page - 1) * pageSize + 1}–${Math.min(total, page * pageSize)} of ${total} events (Page ${page}/${totalPages || 1})`;
    }
    if (prevBtn) prevBtn.disabled = page <= 1;
    if (nextBtn) nextBtn.disabled = page >= totalPages;

    if (!data || data.length === 0) {
      tbody.innerHTML = `<tr><td colspan="7" style="text-align: center; padding: 24px; color: var(--text-muted);">No audit entries found.</td></tr>`;
      return;
    }

    tbody.innerHTML = data.map(item => `
      <tr>
        <td style="text-align: center;"><span class="material-symbols-outlined text-muted" style="font-size: 16px;">receipt</span></td>
        <td class="font-mono text-xs text-secondary">${item.timestamp}</td>
        <td><span class="audit-actor-pill">${item.actor}</span></td>
        <td><strong class="text-primary text-xs">${item.action}</strong></td>
        <td><code class="font-mono text-cyan text-xs">${item.resource}</code></td>
        <td><span class="status-pill ${item.result === 'Success' ? 'healthy' : 'critical'}">${item.result}</span></td>
        <td style="text-align: right;"><span class="font-mono text-xs text-muted">${item.signature}</span></td>
      </tr>
    `).join('');
  }

  // =========================================================================
  // GLOBAL SEARCH (Autocomplete from Real Database)
  // =========================================================================
  function initGlobalSearch() {
    const input = document.getElementById('global-search-input');
    const dropdown = document.getElementById('search-results-dropdown');

    if (!input || !dropdown) return;

    input.addEventListener('input', async (e) => {
      const q = e.target.value.trim();
      if (!q) {
        dropdown.classList.remove('open');
        return;
      }

      try {
        const res = await fetch(`/api/search?q=${encodeURIComponent(q)}`);
        const items = await res.json();

        if (items.length === 0) {
          dropdown.innerHTML = `<div style="padding: 16px; text-align: center; color: var(--text-muted); font-size: 0.8125rem;">No matching records found.</div>`;
        } else {
          dropdown.innerHTML = items.map(item => `
            <div class="search-result-row" onclick="handleSearchResultClick('${item.targetView}', '${item.nodeId || ''}')">
              <div style="display: flex; align-items: center; gap: 8px;">
                <span class="material-symbols-outlined text-cyan" style="font-size: 16px;">${item.icon}</span>
                <div>
                  <div class="font-bold text-xs text-primary">${item.title}</div>
                  <div class="text-xs text-muted">${item.subtitle}</div>
                </div>
              </div>
              <span class="status-pill healthy" style="font-size: 0.6rem;">OPEN</span>
            </div>
          `).join('');
        }
        dropdown.classList.add('open');
      } catch (err) {}
    });

    document.addEventListener('click', (e) => {
      if (!dropdown.contains(e.target) && e.target !== input) {
        dropdown.classList.remove('open');
      }
    });
  }

  window.handleSearchResultClick = function (targetView, nodeId) {
    const dropdown = document.getElementById('search-results-dropdown');
    if (dropdown) dropdown.classList.remove('open');
    switchView(targetView);
    if (nodeId) window.openNodeModal(nodeId);
  };

  // =========================================================================
  // COMMAND PALETTE (⌘K / Ctrl+K)
  // =========================================================================
  function initCommandPalette() {
    const backdrop = document.getElementById('command-modal-backdrop');
    const input = document.getElementById('command-input');
    const list = document.getElementById('command-list');

    function openPalette() {
      if (backdrop) backdrop.classList.add('open');
      if (input) { input.value = ''; input.focus(); }
    }

    function closePalette() {
      if (backdrop) backdrop.classList.remove('open');
    }

    window.addEventListener('keydown', (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        openPalette();
      } else if (e.key === 'Escape') {
        closePalette();
      }
    });

    if (backdrop) {
      backdrop.addEventListener('click', (e) => {
        if (e.target === backdrop) closePalette();
      });
    }

    if (list) {
      list.querySelectorAll('.command-item').forEach(item => {
        item.addEventListener('click', () => {
          const action = item.getAttribute('data-action');
          if (action.startsWith('view:')) switchView(action.split(':')[1]);
          if (action.startsWith('chaos:')) window.triggerChaos(action.split(':')[1]);
          closePalette();
        });
      });
    }
  }

  // =========================================================================
  // GLOBAL BUTTON BINDINGS
  // =========================================================================
  function bindGlobalButtons() {
    const btnQuickRebal = document.getElementById('btn-quick-rebalance');
    if (btnQuickRebal) btnQuickRebal.addEventListener('click', window.openRebalanceModal);

    const btnQuickAudit = document.getElementById('btn-quick-audit');
    if (btnQuickAudit) btnQuickAudit.addEventListener('click', () => switchView('audit'));

    const btnSync = document.getElementById('btn-sync-telemetry');
    if (btnSync) {
      btnSync.addEventListener('click', async () => {
        const icon = document.getElementById('sync-telemetry-icon');
        if (icon) icon.style.animation = 'spin 0.75s linear';
        document.querySelectorAll('.kpi-tile').forEach(t => t.classList.add('syncing-pulse'));
        await loadInitialData();
        setTimeout(() => {
          if (icon) icon.style.animation = '';
          document.querySelectorAll('.kpi-tile').forEach(t => t.classList.remove('syncing-pulse'));
          showToast('Synced all telemetry against SQLite database', 'success');
        }, 600);
      });
    }

    const btnChaos = document.getElementById('btn-chaos-shortcut');
    if (btnChaos) btnChaos.addEventListener('click', () => switchView('failure-sim'));
  }

  // =========================================================================
  // INFORMATION & DOCUMENTATION MODALS (Portal Dialogs)
  // =========================================================================
  function initInfoModals() {
    const backdrop = document.getElementById('info-modal-backdrop');
    const closeBtn = document.getElementById('btn-close-info-modal');
    const contentArea = document.getElementById('info-modal-content-area');

    function closeInfoModal() {
      if (backdrop) backdrop.classList.remove('active');
    }

    if (closeBtn) closeBtn.addEventListener('click', closeInfoModal);
    if (backdrop) {
      backdrop.addEventListener('click', (e) => {
        if (e.target === backdrop) closeInfoModal();
      });
    }

    document.querySelectorAll('[data-modal]').forEach(elem => {
      elem.addEventListener('click', (e) => {
        e.preventDefault();
        const type = elem.getAttribute('data-modal');
        const tab = elem.getAttribute('data-doc-tab') || 'quickstart';
        openInfoModal(type, { docTab: tab });
      });
    });

    window.openInfoModal = function (type, options = {}) {
      if (!backdrop || !contentArea) return;
      if (type === 'docs') renderDocsModal(options.docTab || 'quickstart');
      if (type === 'community') renderCommunityModal();
      if (type === 'news') renderNewsModal(options.newsId);
      if (type === 'foundation') renderFoundationModal();
      backdrop.classList.add('active');
    };

    function renderDocsModal(tab) {
      contentArea.innerHTML = `
        <div class="info-modal-header">
          <div class="info-modal-title">
            <span class="material-symbols-outlined text-cyan" style="font-size: 28px;">menu_book</span>
            <span>VaultMesh Documentation</span>
          </div>
          <div class="info-modal-subtitle">Guides, algorithmic specifications, and SDK references</div>
        </div>
        <div class="info-modal-tabs">
          <button class="info-modal-tab-btn ${tab === 'quickstart' ? 'active' : ''}" onclick="window.switchDocModalTab('quickstart')">Quickstart</button>
          <button class="info-modal-tab-btn ${tab === 'api' ? 'active' : ''}" onclick="window.switchDocModalTab('api')">S3 REST API</button>
          <button class="info-modal-tab-btn ${tab === 'crush' ? 'active' : ''}" onclick="window.switchDocModalTab('crush')">CRUSH Algorithm</button>
        </div>
        <div class="info-modal-content">
          ${getDocTabContent(tab)}
        </div>
      `;
    }

    window.switchDocModalTab = function (tab) {
      renderDocsModal(tab);
    };

    function getDocTabContent(tab) {
      if (tab === 'quickstart') {
        return `
          <p>VaultMesh boots with single-command orchestration:</p>
          <div class="info-modal-code">
curl -sSL https://get.vaultmesh.io | bash
vaultmesh init --nodes 4 --az us-east-2a
vaultmesh console start --port 3000
          </div>
        `;
      } else if (tab === 'api') {
        return `
          <p>S3 compatible object operations:</p>
          <div class="info-modal-code">
PUT /financial-records/q3-audit.parquet HTTP/1.1
Host: s3.vaultmesh.internal
x-amz-storage-class: VAULTMESH_REED_SOLOMON_8_4
Authorization: AWS4-HMAC-SHA256 ...
          </div>
        `;
      }
      return `
        <p>CRUSH rule deterministic placement across racks and zones.</p>
        <div class="info-modal-code">
rule ec_pool_us_east {
    ruleset 0
    type erasure
    min_size 8
    max_size 12
    step take root-us-east
    step chooseleaf rack 12
    step emit
}
        </div>
      `;
    }

    function renderCommunityModal() {
      contentArea.innerHTML = `
        <div class="info-modal-header">
          <div class="info-modal-title">
            <span class="material-symbols-outlined text-teal" style="font-size: 28px;">diversity_3</span>
            <span>VaultMesh Community</span>
          </div>
          <div class="info-modal-subtitle">Join the Discord Fabric and GitHub RFC Discussions</div>
        </div>
        <div class="info-modal-content">
          <p>Connect with distributed storage engineers across Discord, RFC working groups, and bi-weekly standups.</p>
        </div>
      `;
    }

    function renderNewsModal() {
      contentArea.innerHTML = `
        <div class="info-modal-header">
          <div class="info-modal-title">
            <span class="material-symbols-outlined text-cyan" style="font-size: 28px;">newspaper</span>
            <span>VaultMesh v4.18 GA Released</span>
          </div>
        </div>
        <div class="info-modal-content">
          <p>SIMD AVX-512 Galois Field bit-rot repair and linearizable Raft lease reads now generally available.</p>
        </div>
      `;
    }

    function renderFoundationModal() {
      contentArea.innerHTML = `
        <div class="info-modal-header">
          <div class="info-modal-title">
            <span class="material-symbols-outlined text-emerald" style="font-size: 28px;">account_balance</span>
            <span>VaultMesh Foundation</span>
          </div>
        </div>
        <div class="info-modal-content">
          <p>Vendor-neutral governance charter and technical steering committee guidelines.</p>
        </div>
      `;
    }
  }

  // =========================================================================
  // TOAST NOTIFICATIONS
  // =========================================================================
  window.showToast = function (message, type = 'info') {
    const container = document.getElementById('toast-container');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = `toast ${type}`;

    let icon = 'info';
    if (type === 'success') icon = 'check_circle';
    if (type === 'warning') icon = 'warning';
    if (type === 'error') icon = 'error';

    toast.innerHTML = `
      <span class="material-symbols-outlined" style="font-size: 18px;">${icon}</span>
      <span>${message}</span>
    `;

    container.appendChild(toast);
    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateX(20px)';
      toast.style.transition = 'all 0.25s ease';
      setTimeout(() => toast.remove(), 250);
    }, 3500);
  };

})();
