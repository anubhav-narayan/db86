/**
 * DB86 Atlas Database Studio - Main Application Controller
 */

import './styles/atlas.css';
import { api, getBaseUrl, setBaseUrl } from './services/api.js';
import { highlightJson, generateSnippets } from './utils/formatters.js';
import {
  showToast,
  closeModal,
  openConnectModal,
  openCreateDbModal,
  openCreateStorageModal,
  openInsertDocModal,
  openEditDocModal
} from './components/Modals.js';

// Expose modal helper globally for inline handlers
window.closeModal = closeModal;

// Application State
const state = {
  baseUrl: getBaseUrl(),
  isOnline: false,
  ping: null,
  health: null,
  databases: [],
  dbTree: {}, // { [dbName]: { storages: [], metadata: {} } }
  activeDb: null,
  activeStorage: null,
  activeStorageType: 'json',
  activeTab: 'documents', // 'documents' | 'table' | 'schema' | 'api'
  items: [],
  queryPath: '',
  limit: 50,
  offset: 0,
  searchFilter: '',
  lastDuration: null,
  isLoading: false
};

// Initialize App
async function init() {
  renderAppLayout();
  setupEventListeners();
  await refreshServerState();

  // Start background health poller (every 5 seconds)
  setInterval(async () => {
    await checkHealthSilently();
  }, 5000);
}

// Top-level HTML Structure
function renderAppLayout() {
  const app = document.getElementById('app');
  app.innerHTML = `
    <!-- Top Navigation Bar -->
    <header class="atlas-navbar">
      <div class="brand-section" id="nav-brand-btn">
        <div class="brand-logo">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#001E2B" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
            <ellipse cx="12" cy="5" rx="9" ry="3"></ellipse>
            <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path>
            <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>
          </svg>
        </div>
        <div class="brand-name">
          DB86 <span style="color:var(--brand-green);">Atlas</span>
          <span class="brand-badge">Studio</span>
        </div>
      </div>

      <!-- Server Host & Connection Status -->
      <div class="server-connect-bar">
        <div class="status-indicator">
          <div id="status-dot" class="status-dot ${state.isOnline ? 'online' : 'offline'}"></div>
          <span id="status-text">${state.isOnline ? `Online (${state.ping}ms)` : 'Offline'}</span>
        </div>
        <div style="color:var(--border-color);">|</div>
        <input type="text" id="host-url-input" class="host-input" value="${state.baseUrl}" title="DB86 REST Server Endpoint" />
        <button id="reconnect-btn" class="btn btn-secondary" style="padding: 4px 8px; font-size: 0.75rem;">Connect</button>
      </div>

      <!-- Quick Actions -->
      <div class="nav-actions">
        <button id="nav-connect-btn" class="btn btn-secondary">
          <span>🔗</span> Connect
        </button>
        <button id="nav-create-db-btn" class="btn btn-primary">
          <span>➕</span> Create Database
        </button>
        <a href="${state.baseUrl}/docs" target="_blank" class="btn btn-secondary" title="Open FastAPI Swagger Documentation">
          <span>📚</span> Swagger Docs
        </a>
      </div>
    </header>

    <!-- Main Workspace Container -->
    <div class="atlas-container">
      <!-- Left Sidebar: Database Tree -->
      <aside class="atlas-sidebar">
        <div class="sidebar-header">
          <div class="sidebar-title">Databases & Storages</div>
          <button id="sidebar-refresh-btn" class="btn-icon" title="Refresh Databases">🔄</button>
        </div>

        <div class="sidebar-search">
          <div class="search-input-wrap">
            <span class="search-icon">🔍</span>
            <input type="text" id="sidebar-search-input" class="search-input" placeholder="Search database or storage..." />
          </div>
        </div>

        <div class="db-tree" id="sidebar-db-tree">
          <!-- Populated dynamically -->
        </div>
      </aside>

      <!-- Main Content Area -->
      <main class="atlas-main" id="main-content">
        <!-- Rendered based on selection (Overview vs Data Explorer) -->
      </main>
    </div>
  `;
}

// Setup Event Handlers
function setupEventListeners() {
  document.getElementById('nav-brand-btn').addEventListener('click', () => {
    state.activeDb = null;
    state.activeStorage = null;
    renderSidebarTree();
    renderMainContent();
  });

  document.getElementById('reconnect-btn').addEventListener('click', async () => {
    const inputVal = document.getElementById('host-url-input').value.trim();
    if (inputVal) {
      setBaseUrl(inputVal);
      state.baseUrl = inputVal;
      await refreshServerState();
    }
  });

  document.getElementById('nav-connect-btn').addEventListener('click', () => {
    openConnectModal(state.baseUrl, state.activeDb, state.activeStorage);
  });

  document.getElementById('nav-create-db-btn').addEventListener('click', () => {
    openCreateDbModal(async (params) => {
      try {
        await api.createDatabase(params);
        showToast(`Database '${params.name}' created successfully!`);
        await refreshDatabases();
        selectDatabase(params.name);
      } catch (err) {
        showToast(`Failed to create database: ${err.message}`, 'error');
      }
    });
  });

  document.getElementById('sidebar-refresh-btn').addEventListener('click', async () => {
    await refreshServerState();
    showToast('Refreshed database cluster state');
  });

  document.getElementById('sidebar-search-input').addEventListener('input', (e) => {
    state.searchFilter = e.target.value.toLowerCase();
    renderSidebarTree();
  });
}

// Background Health Checker
async function checkHealthSilently() {
  try {
    const { data, duration } = await api.getHealth();
    state.isOnline = true;
    state.ping = duration;
    state.health = data;
    updateStatusIndicator();
  } catch (_) {
    state.isOnline = false;
    state.ping = null;
    updateStatusIndicator();
  }
}

// Full Server State Refresh
async function refreshServerState() {
  try {
    const { data, duration } = await api.getHealth();
    state.isOnline = true;
    state.ping = duration;
    state.health = data;
    updateStatusIndicator();

    await refreshDatabases();
  } catch (err) {
    state.isOnline = false;
    state.ping = null;
    updateStatusIndicator();
    showToast('Could not connect to DB86 REST Server at ' + state.baseUrl, 'error');
    renderMainContent();
  }
}

function updateStatusIndicator() {
  const dot = document.getElementById('status-dot');
  const text = document.getElementById('status-text');
  if (dot && text) {
    dot.className = `status-dot ${state.isOnline ? 'online' : 'offline'}`;
    text.textContent = state.isOnline ? `Online (${state.ping}ms)` : 'Offline';
  }
}

// Fetch all Databases & Storages
async function refreshDatabases() {
  try {
    const { data } = await api.listDatabases();
    state.databases = data.databases || [];
    state.dbTree = {};

    for (const dbName of state.databases) {
      try {
        const [metaRes, storagesRes] = await Promise.all([
          api.getDatabaseMetadata(dbName),
          api.listStorages(dbName)
        ]);
        state.dbTree[dbName] = {
          metadata: metaRes.data,
          storages: storagesRes.data.storages || []
        };
      } catch (e) {
        state.dbTree[dbName] = { metadata: {}, storages: [] };
      }
    }

    renderSidebarTree();
    renderMainContent();
  } catch (err) {
    console.error('Failed to list databases:', err);
  }
}

// Render Database Sidebar Tree
function renderSidebarTree() {
  const container = document.getElementById('sidebar-db-tree');
  if (!container) return;

  if (state.databases.length === 0) {
    container.innerHTML = `
      <div style="padding: 24px 16px; text-align:center; color:var(--text-muted); font-size:0.85rem;">
        No open databases found.<br>
        <button id="empty-create-db-btn" class="btn btn-secondary" style="margin-top:10px; font-size:0.8rem;">
          ➕ Create First Database
        </button>
      </div>
    `;
    const btn = document.getElementById('empty-create-db-btn');
    if (btn) {
      btn.addEventListener('click', () => {
        document.getElementById('nav-create-db-btn').click();
      });
    }
    return;
  }

  let html = '';
  const filter = state.searchFilter;

  for (const dbName of state.databases) {
    const dbInfo = state.dbTree[dbName] || { storages: [], metadata: {} };
    const storages = dbInfo.storages;
    
    // Filter matching
    const matchesDb = dbName.toLowerCase().includes(filter);
    const matchingStorages = storages.filter(s => s.name.toLowerCase().includes(filter));
    if (filter && !matchesDb && matchingStorages.length === 0) {
      continue;
    }

    const isActiveDb = state.activeDb === dbName;

    html += `
      <div class="db-item">
        <div class="db-header ${isActiveDb && !state.activeStorage ? 'active' : ''}" data-db="${dbName}">
          <div class="db-header-left">
            <span style="font-size:1rem;">🗄️</span>
            <span title="${dbName}">${dbName}</span>
          </div>
          <div class="db-actions">
            <button class="btn-icon add-storage-btn" data-db="${dbName}" title="Add Collection / Table">➕</button>
            <button class="btn-icon danger close-db-btn" data-db="${dbName}" title="Close Database">✕</button>
          </div>
        </div>

        <div class="storage-list">
          ${storages.map(s => {
            if (filter && !matchesDb && !s.name.toLowerCase().includes(filter)) return '';
            const isActiveStorage = isActiveDb && state.activeStorage === s.name;
            const isJson = s.storage_type === 'json';
            return `
              <div class="storage-item ${isActiveStorage ? 'active' : ''}" data-db="${dbName}" data-storage="${s.name}" data-type="${s.storage_type}">
                <div class="storage-left">
                  <span>${isJson ? '📄' : '📊'}</span>
                  <span>${s.name}</span>
                </div>
                <span class="storage-badge ${isJson ? 'badge-json' : 'badge-table'}">
                  ${isJson ? 'JSON' : 'TABLE'}
                </span>
              </div>
            `;
          }).join('')}
        </div>
      </div>
    `;
  }

  container.innerHTML = html;

  // Bind Sidebar item click events
  container.querySelectorAll('.db-header').forEach(el => {
    el.addEventListener('click', (e) => {
      if (e.target.closest('.db-actions')) return;
      const dbName = el.getAttribute('data-db');
      selectDatabase(dbName);
    });
  });

  container.querySelectorAll('.storage-item').forEach(el => {
    el.addEventListener('click', () => {
      const dbName = el.getAttribute('data-db');
      const storageName = el.getAttribute('data-storage');
      const storageType = el.getAttribute('data-type');
      selectStorage(dbName, storageName, storageType);
    });
  });

  container.querySelectorAll('.add-storage-btn').forEach(el => {
    el.addEventListener('click', (e) => {
      e.stopPropagation();
      const dbName = el.getAttribute('data-db');
      openCreateStorageModal(dbName, async (params) => {
        try {
          await api.createStorage(dbName, params);
          showToast(`Created ${params.storage_type} storage '${params.name}'!`);
          await refreshDatabases();
          selectStorage(dbName, params.name, params.storage_type);
        } catch (err) {
          showToast(`Failed to create storage: ${err.message}`, 'error');
        }
      });
    });
  });

  container.querySelectorAll('.close-db-btn').forEach(el => {
    el.addEventListener('click', async (e) => {
      e.stopPropagation();
      const dbName = el.getAttribute('data-db');
      if (confirm(`Are you sure you want to close database '${dbName}'?`)) {
        try {
          await api.closeDatabase(dbName);
          showToast(`Closed database '${dbName}'`);
          if (state.activeDb === dbName) {
            state.activeDb = null;
            state.activeStorage = null;
          }
          await refreshDatabases();
        } catch (err) {
          showToast(`Failed to close database: ${err.message}`, 'error');
        }
      }
    });
  });
}

// Select Database
function selectDatabase(dbName) {
  state.activeDb = dbName;
  state.activeStorage = null;
  state.items = [];
  renderSidebarTree();
  renderMainContent();
}

// Select Storage & Load Data
async function selectStorage(dbName, storageName, storageType = 'json') {
  state.activeDb = dbName;
  state.activeStorage = storageName;
  state.activeStorageType = storageType;
  state.activeTab = 'documents';
  state.queryPath = '';
  state.offset = 0;
  
  renderSidebarTree();
  await loadStorageItems();
}

// Load Storage Items / Run Path Query
async function loadStorageItems() {
  if (!state.activeDb || !state.activeStorage) return;

  state.isLoading = true;
  renderMainContent();

  try {
    let res;
    if (state.queryPath && state.queryPath.trim() !== '') {
      // Path query execution
      res = await api.queryPath(state.activeDb, state.activeStorage, state.queryPath.trim(), state.activeStorageType);
      state.items = (res.data.results || []).map((val, idx) => ({
        key: `match_${idx + 1}`,
        value: val
      }));
    } else {
      // Standard item list
      res = await api.listItems(state.activeDb, state.activeStorage, {
        limit: state.limit,
        offset: state.offset,
        storage_type: state.activeStorageType
      });
      
      const rawItems = res.data.items || [];
      if (Array.isArray(rawItems) && rawItems.length > 0 && Array.isArray(rawItems[0])) {
        // [ [key, val], ... ]
        state.items = rawItems.map(([k, v]) => ({ key: k, value: v }));
      } else if (Array.isArray(rawItems) && rawItems.length > 0 && typeof rawItems[0] === 'object' && 'key' in rawItems[0]) {
        state.items = rawItems;
      } else if (Array.isArray(rawItems)) {
        state.items = rawItems.map((item, idx) => {
          const keyVal = (typeof item === 'object' && item !== null) ? Object.values(item)[0] || `row_${idx}` : `row_${idx}`;
          return { key: String(keyVal), value: item };
        });
      } else {
        state.items = [];
      }
    }

    state.lastDuration = res.duration;
  } catch (err) {
    showToast(`Error loading data: ${err.message}`, 'error');
    state.items = [];
  } finally {
    state.isLoading = false;
    renderMainContent();
  }
}

// Render Main Content (Cluster Overview vs Storage Data Explorer)
function renderMainContent() {
  const main = document.getElementById('main-content');
  if (!main) return;

  if (!state.activeDb || !state.activeStorage) {
    main.innerHTML = renderClusterOverviewHtml();
    bindOverviewEvents();
  } else {
    main.innerHTML = renderDataExplorerHtml();
    bindExplorerEvents();
  }
}

// Cluster Overview View
function renderClusterOverviewHtml() {
  const h = state.health || {};
  const metrics = h.system_metrics || {};
  const proc = metrics.process || {};
  const sys = metrics.system || {};
  const uptime = h.uptime || '0d 00:00:00';
  const totalDbs = state.databases.length;
  
  let totalStorages = 0;
  for (const dbName of state.databases) {
    totalStorages += (state.dbTree[dbName]?.storages?.length || 0);
  }

  const memoryMb = proc.memory_mb || 0;
  const memoryPercent = proc.memory_percent || 0;
  const cpuPercent = proc.cpu_percent || 0;
  const diskPercent = sys.disk_usage_percent || 0;

  return `
    <div class="overview-view">
      <div class="overview-hero">
        <div>
          <h1 class="hero-title">Local DB86 Cluster Overview</h1>
          <p class="hero-subtitle">High-performance SQLite3 multi-threaded document & table database engine</p>
        </div>
        <div style="display:flex; gap:10px;">
          <button id="overview-create-db-btn" class="btn btn-primary">
            <span>➕</span> New Database
          </button>
          <button id="overview-connect-btn" class="btn btn-secondary">
            <span>🔗</span> Connection String
          </button>
        </div>
      </div>

      <!-- Metrics Cards Grid -->
      <div class="metrics-grid">
        <div class="metric-card">
          <div class="metric-card-top">
            <span class="metric-card-title">Server Status</span>
            <span>⚡</span>
          </div>
          <div class="metric-value" style="color:${state.isOnline ? 'var(--brand-green)' : 'var(--brand-red)'};">
            ${state.isOnline ? 'ONLINE' : 'OFFLINE'}
          </div>
          <div class="metric-subtext">Uptime: ${uptime}</div>
        </div>

        <div class="metric-card">
          <div class="metric-card-top">
            <span class="metric-card-title">Databases & Storages</span>
            <span>🗄️</span>
          </div>
          <div class="metric-value">${totalDbs} / ${totalStorages}</div>
          <div class="metric-subtext">${totalDbs} active databases, ${totalStorages} collections</div>
        </div>

        <div class="metric-card">
          <div class="metric-card-top">
            <span class="metric-card-title">Memory Usage</span>
            <span>🧠</span>
          </div>
          <div class="metric-value">${memoryMb} <span style="font-size:1rem; font-weight:500;">MB</span></div>
          <div class="progress-bar-bg">
            <div class="progress-bar-fill" style="width:${Math.min(100, Math.max(5, memoryPercent * 5))}%;"></div>
          </div>
          <div class="metric-subtext">${memoryPercent}% of host memory</div>
        </div>

        <div class="metric-card">
          <div class="metric-card-top">
            <span class="metric-card-title">System Load</span>
            <span>📊</span>
          </div>
          <div class="metric-value">${cpuPercent}% <span style="font-size:1rem; font-weight:500;">CPU</span></div>
          <div class="progress-bar-bg">
            <div class="progress-bar-fill" style="width:${Math.min(100, Math.max(5, cpuPercent))}%;"></div>
          </div>
          <div class="metric-subtext">Disk usage: ${diskPercent}%</div>
        </div>
      </div>

      <!-- Overview Details Section -->
      <div class="overview-sections">
        <!-- Database List Panel -->
        <div class="panel">
          <div class="panel-header">
            <div class="panel-title">Active Databases</div>
            <span style="font-size:0.85rem; color:var(--text-secondary);">${state.databases.length} Databases</span>
          </div>

          ${state.databases.length === 0 ? `
            <div style="text-align:center; padding:30px; color:var(--text-muted);">
              No active databases. Click "New Database" above to create one.
            </div>
          ` : `
            <div style="display:flex; flex-direction:column; gap:10px;">
              ${state.databases.map(dbName => {
                const info = state.dbTree[dbName] || {};
                const storages = info.storages || [];
                const meta = info.metadata || {};
                return `
                  <div style="background:var(--bg-input); border:1px solid var(--border-color); border-radius:var(--radius-sm); padding:14px 18px; display:flex; justify-content:space-between; align-items:center;">
                    <div>
                      <div style="font-weight:700; font-size:0.95rem; display:flex; align-items:center; gap:8px;">
                        <span>🗄️</span> ${dbName}
                        <span style="font-size:0.7rem; background:rgba(255,255,255,0.06); padding:2px 6px; border-radius:4px; color:var(--text-secondary); font-family:var(--font-mono);">
                          ${meta.filename || `${dbName}.db`}
                        </span>
                      </div>
                      <div style="font-size:0.8rem; color:var(--text-secondary); margin-top:4px;">
                        Journal: <code>${meta.journal_mode || 'WAL'}</code> · Autocommit: <code>${meta.autocommit ? 'Yes' : 'No'}</code> · ${storages.length} Collections/Tables
                      </div>
                    </div>
                    <div style="display:flex; gap:8px;">
                      <button class="btn btn-secondary overview-view-db-btn" data-db="${dbName}" style="padding:4px 10px; font-size:0.8rem;">Explore</button>
                    </div>
                  </div>
                `;
              }).join('')}
            </div>
          `}
        </div>

        <!-- Quick Info Panel -->
        <div class="panel">
          <div class="panel-header">
            <div class="panel-title">Quick Guide</div>
          </div>
          <div style="font-size:0.85rem; color:var(--text-secondary); line-height:1.6;">
            <p style="margin-bottom:10px;">
              <strong style="color:var(--brand-green);">DB86</strong> provides instant dict-like storage over SQLite with automatic thread isolation.
            </p>
            <ul style="padding-left:18px; margin-bottom:14px;">
              <li><strong>JSON Storages</strong>: Store nested JSON documents with path query support.</li>
              <li><strong>Table Storages</strong>: Relational tables with named columns.</li>
              <li><strong>WAL Mode</strong>: Concurrent multi-read write-ahead logging enabled.</li>
            </ul>
            <div style="margin-top:16px;">
              <button id="overview-open-connect" class="btn btn-primary" style="width:100%;">
                <span>🔗</span> View API Snippets
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  `;
}

function bindOverviewEvents() {
  const createBtn = document.getElementById('overview-create-db-btn');
  if (createBtn) {
    createBtn.addEventListener('click', () => {
      document.getElementById('nav-create-db-btn').click();
    });
  }

  const connectBtn = document.getElementById('overview-connect-btn');
  if (connectBtn) {
    connectBtn.addEventListener('click', () => {
      openConnectModal(state.baseUrl, state.activeDb, state.activeStorage);
    });
  }

  const openConnect = document.getElementById('overview-open-connect');
  if (openConnect) {
    openConnect.addEventListener('click', () => {
      openConnectModal(state.baseUrl, state.activeDb, state.activeStorage);
    });
  }

  document.querySelectorAll('.overview-view-db-btn').forEach(el => {
    el.addEventListener('click', () => {
      const dbName = el.getAttribute('data-db');
      const storages = state.dbTree[dbName]?.storages || [];
      if (storages.length > 0) {
        selectStorage(dbName, storages[0].name, storages[0].storage_type);
      } else {
        selectDatabase(dbName);
      }
    });
  });
}

// Data Explorer View (MongoDB Atlas Browser)
function renderDataExplorerHtml() {
  const isJson = state.activeStorageType === 'json';

  return `
    <div class="explorer-view">
      <!-- Storage Top Bar -->
      <div class="explorer-top-bar">
        <div class="storage-breadcrumbs">
          <span style="color:var(--text-secondary); cursor:pointer;" id="bc-db-name">🗄️ ${state.activeDb}</span>
          <span class="breadcrumb-sep">/</span>
          <span>${isJson ? '📄' : '📊'} ${state.activeStorage}</span>
          <span class="storage-badge ${isJson ? 'badge-json' : 'badge-table'}">
            ${isJson ? 'JSON Collection' : 'Relational Table'}
          </span>
        </div>

        <!-- Navigation Tabs -->
        <div class="tab-nav">
          <button class="tab-btn ${state.activeTab === 'documents' ? 'active' : ''}" data-tab="documents">
            <span>📄</span> Documents
          </button>
          <button class="tab-btn ${state.activeTab === 'table' ? 'active' : ''}" data-tab="table">
            <span>📊</span> Table Grid
          </button>
          <button class="tab-btn ${state.activeTab === 'schema' ? 'active' : ''}" data-tab="schema">
            <span>📐</span> Schema & Indexes
          </button>
          <button class="tab-btn ${state.activeTab === 'api' ? 'active' : ''}" data-tab="api">
            <span>⚡</span> REST API
          </button>
        </div>
      </div>

      <!-- Atlas Query / Find Bar (Active for Documents & Table tabs) -->
      ${state.activeTab === 'documents' || state.activeTab === 'table' ? `
        <div class="atlas-query-bar">
          <div class="query-input-group">
            <span class="query-label">Find:</span>
            <input type="text" id="atlas-path-query-input" class="query-input" 
              placeholder="${isJson ? 'e.g. */email or users/*/role or alice/address' : 'e.g. path query'}" 
              value="${state.queryPath}" />
          </div>

          <div class="query-controls">
            <span style="font-size:0.8rem; color:var(--text-secondary);">Limit:</span>
            <input type="number" id="query-limit-input" class="limit-input" value="${state.limit}" min="1" max="1000" />
            
            <button id="query-execute-btn" class="btn btn-primary">
              <span>🔍</span> Execute
            </button>
            <button id="query-reset-btn" class="btn btn-secondary">
              Reset
            </button>
          </div>
        </div>
      ` : ''}

      <!-- Main Explorer Body -->
      <div class="explorer-content">
        ${renderTabContent()}
      </div>
    </div>
  `;
}

// Render Inner Tab Content
function renderTabContent() {
  if (state.isLoading) {
    return `
      <div style="padding:60px 0; text-align:center; color:var(--text-secondary);">
        <div style="font-size:2rem; margin-bottom:12px; animation:spin 1s linear infinite;">⏳</div>
        <div>Loading storage items...</div>
      </div>
    `;
  }

  if (state.activeTab === 'documents') {
    return renderDocumentsTab();
  } else if (state.activeTab === 'table') {
    return renderTableGridTab();
  } else if (state.activeTab === 'schema') {
    return renderSchemaTab();
  } else if (state.activeTab === 'api') {
    return renderApiTab();
  }
}

// Documents View (JSON Cards)
function renderDocumentsTab() {
  const isQuery = state.queryPath && state.queryPath.trim() !== '';

  return `
    <div class="explorer-actions-bar">
      <div class="doc-count-tag">
        Showing <strong>${state.items.length}</strong> items 
        ${state.lastDuration !== null ? `<span style="margin-left:8px; font-family:var(--font-mono); color:var(--brand-green);">(${state.lastDuration}ms)</span>` : ''}
        ${isQuery ? `<span style="margin-left:8px; color:#60a5fa; font-family:var(--font-mono);">[Filter: ${state.queryPath}]</span>` : ''}
      </div>

      <div style="display:flex; gap:8px;">
        <button id="insert-doc-btn" class="btn btn-primary">
          <span>➕</span> Insert Document
        </button>
        <button id="export-json-btn" class="btn btn-secondary" title="Export all loaded items as JSON">
          <span>💾</span> Export JSON
        </button>
        <button id="drop-storage-btn" class="btn btn-danger" title="Drop this storage">
          <span>🗑️</span> Drop Collection
        </button>
      </div>
    </div>

    ${state.items.length === 0 ? `
      <div style="padding:50px 0; text-align:center; color:var(--text-muted); border:1px dashed var(--border-color); border-radius:var(--radius-md);">
        <div style="font-size:1.8rem; margin-bottom:8px;">📭</div>
        <div style="font-size:1rem; font-weight:600; color:var(--text-secondary); margin-bottom:6px;">No documents found</div>
        <p style="font-size:0.85rem; margin-bottom:14px;">This storage currently contains no items matching the query.</p>
        <button id="empty-insert-btn" class="btn btn-primary">Insert First Document</button>
      </div>
    ` : `
      <div class="doc-cards-list">
        ${state.items.map(item => {
          const highlighted = highlightJson(item.value);
          return `
            <div class="doc-card" data-key="${item.key}">
              <div class="doc-card-header">
                <div class="doc-key">
                  <span>🔑</span>
                  <span>"${item.key}"</span>
                </div>
                <div class="doc-actions">
                  <button class="btn-icon copy-doc-btn" data-key="${item.key}" title="Copy JSON">📋</button>
                  <button class="btn-icon edit-doc-btn" data-key="${item.key}" title="Edit Document">✏️</button>
                  <button class="btn-icon danger delete-doc-btn" data-key="${item.key}" title="Delete Document">🗑️</button>
                </div>
              </div>
              <pre class="doc-body">${highlighted}</pre>
            </div>
          `;
        }).join('')}
      </div>
    `}
  `;
}

// Table Grid View
function renderTableGridTab() {
  if (state.items.length === 0) {
    return `
      <div style="padding:50px 0; text-align:center; color:var(--text-muted); border:1px dashed var(--border-color); border-radius:var(--radius-md);">
        No tabular rows found.
      </div>
    `;
  }

  // Derive columns from items
  const colSet = new Set();
  colSet.add('_key');

  state.items.forEach(item => {
    if (typeof item.value === 'object' && item.value !== null && !Array.isArray(item.value)) {
      Object.keys(item.value).forEach(k => colSet.add(k));
    } else {
      colSet.add('value');
    }
  });

  const columns = Array.from(colSet);

  return `
    <div class="explorer-actions-bar">
      <div class="doc-count-tag">
        Showing <strong>${state.items.length}</strong> rows 
        ${state.lastDuration !== null ? `<span style="margin-left:8px; font-family:var(--font-mono); color:var(--brand-green);">(${state.lastDuration}ms)</span>` : ''}
      </div>
      <div style="display:flex; gap:8px;">
        <button id="insert-row-btn" class="btn btn-primary">➕ Add Row</button>
        <button id="export-json-btn" class="btn btn-secondary">💾 Export</button>
      </div>
    </div>

    <div class="table-grid-wrap">
      <table class="atlas-table">
        <thead>
          <tr>
            ${columns.map(col => `<th>${col}</th>`).join('')}
            <th style="text-align:right;">Actions</th>
          </tr>
        </thead>
        <tbody>
          ${state.items.map(item => {
            return `
              <tr>
                ${columns.map(col => {
                  let cellVal = '';
                  if (col === '_key') {
                    cellVal = `<strong style="color:var(--brand-green-light);">${item.key}</strong>`;
                  } else if (typeof item.value === 'object' && item.value !== null && col in item.value) {
                    const raw = item.value[col];
                    cellVal = typeof raw === 'object' ? JSON.stringify(raw) : String(raw);
                  } else if (col === 'value') {
                    cellVal = typeof item.value === 'object' ? JSON.stringify(item.value) : String(item.value);
                  }
                  return `<td>${cellVal}</td>`;
                }).join('')}
                <td style="text-align:right;">
                  <button class="btn-icon edit-doc-btn" data-key="${item.key}" title="Edit">✏️</button>
                  <button class="btn-icon danger delete-doc-btn" data-key="${item.key}" title="Delete">🗑️</button>
                </td>
              </tr>
            `;
          }).join('')}
        </tbody>
      </table>
    </div>
  `;
}

// Schema & Indexes Tab
function renderSchemaTab() {
  const dbInfo = state.dbTree[state.activeDb] || {};
  const meta = dbInfo.metadata || {};

  return `
    <div style="max-width:850px;">
      <div class="panel" style="margin-bottom:20px;">
        <div class="panel-header">
          <div class="panel-title">Storage Information</div>
        </div>
        <table class="atlas-table">
          <tbody>
            <tr><td style="width:200px; color:var(--text-secondary);">Database</td><td><strong>${state.activeDb}</strong></td></tr>
            <tr><td style="color:var(--text-secondary);">Storage Name</td><td><strong>${state.activeStorage}</strong></td></tr>
            <tr><td style="color:var(--text-secondary);">Storage Type</td><td><code>${state.activeStorageType}</code></td></tr>
            <tr><td style="color:var(--text-secondary);">Total Loaded Entries</td><td>${state.items.length}</td></tr>
            <tr><td style="color:var(--text-secondary);">Database File</td><td><code>${meta.filename || `${state.activeDb}.db`}</code></td></tr>
            <tr><td style="color:var(--text-secondary);">Journal Mode</td><td><code>${meta.journal_mode || 'WAL'}</code></td></tr>
          </tbody>
        </table>
      </div>

      <div class="panel">
        <div class="panel-header">
          <div class="panel-title">Indexes & Views</div>
        </div>
        <div style="font-size:0.85rem; color:var(--text-secondary);">
          <div style="margin-bottom:8px;"><strong>Indices:</strong></div>
          <pre class="code-box">${JSON.stringify(meta.indices || {}, null, 2)}</pre>
          <div style="margin-top:16px; margin-bottom:8px;"><strong>Views:</strong></div>
          <pre class="code-box">${JSON.stringify(meta.views || {}, null, 2)}</pre>
        </div>
      </div>
    </div>
  `;
}

// REST API & Snippets Tab
function renderApiTab() {
  const { curlSnippet, pythonSnippet, fetchSnippet } = generateSnippets(state.baseUrl, state.activeDb, state.activeStorage);

  return `
    <div style="max-width:900px;">
      <div class="panel" style="margin-bottom:20px;">
        <div class="panel-header">
          <div class="panel-title">⚡ Interactive REST API Console</div>
        </div>
        <p style="font-size:0.85rem; color:var(--text-secondary); margin-bottom:14px;">
          Use these ready-to-run snippets in your frontend application, Python scripts, or terminal.
        </p>

        <div style="margin-bottom:20px;">
          <label class="form-label">cURL (Command Line)</label>
          <div class="code-box">${curlSnippet.replace(/\n/g, '<br>')}</div>
        </div>

        <div style="margin-bottom:20px;">
          <label class="form-label">Python (db86 SDK)</label>
          <div class="code-box">${pythonSnippet.replace(/\n/g, '<br>')}</div>
        </div>

        <div>
          <label class="form-label">JavaScript (fetch API)</label>
          <div class="code-box">${fetchSnippet.replace(/\n/g, '<br>')}</div>
        </div>
      </div>
    </div>
  `;
}

// Bind Data Explorer Events
function bindExplorerEvents() {
  const bcDb = document.getElementById('bc-db-name');
  if (bcDb) {
    bcDb.addEventListener('click', () => {
      selectDatabase(state.activeDb);
    });
  }

  // Tabs
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      state.activeTab = btn.getAttribute('data-tab');
      renderMainContent();
    });
  });

  // Query Execution
  const executeBtn = document.getElementById('query-execute-btn');
  const queryInput = document.getElementById('atlas-path-query-input');
  const limitInput = document.getElementById('query-limit-input');

  if (executeBtn && queryInput) {
    executeBtn.addEventListener('click', async () => {
      state.queryPath = queryInput.value;
      if (limitInput) state.limit = parseInt(limitInput.value) || 50;
      await loadStorageItems();
    });

    queryInput.addEventListener('keydown', async (e) => {
      if (e.key === 'Enter') {
        state.queryPath = queryInput.value;
        if (limitInput) state.limit = parseInt(limitInput.value) || 50;
        await loadStorageItems();
      }
    });
  }

  const resetBtn = document.getElementById('query-reset-btn');
  if (resetBtn) {
    resetBtn.addEventListener('click', async () => {
      state.queryPath = '';
      if (queryInput) queryInput.value = '';
      await loadStorageItems();
    });
  }

  // Insert Document Button
  const insertBtn = document.getElementById('insert-doc-btn') || document.getElementById('insert-row-btn') || document.getElementById('empty-insert-btn');
  if (insertBtn) {
    insertBtn.addEventListener('click', () => {
      openInsertDocModal(state.activeDb, state.activeStorage, state.activeStorageType, async (key, val) => {
        try {
          await api.upsertItem(state.activeDb, state.activeStorage, key, val, state.activeStorageType);
          showToast(`Saved item '${key}' successfully!`);
          await loadStorageItems();
        } catch (err) {
          showToast(`Failed to save item: ${err.message}`, 'error');
        }
      });
    });
  }

  // Export JSON Button
  const exportBtn = document.getElementById('export-json-btn');
  if (exportBtn) {
    exportBtn.addEventListener('click', () => {
      const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(state.items, null, 2));
      const downloadAnchor = document.createElement('a');
      downloadAnchor.setAttribute("href", dataStr);
      downloadAnchor.setAttribute("download", `${state.activeDb}_${state.activeStorage}_export.json`);
      document.body.appendChild(downloadAnchor);
      downloadAnchor.click();
      downloadAnchor.remove();
      showToast('Exported items to JSON file');
    });
  }

  // Drop Storage Button
  const dropBtn = document.getElementById('drop-storage-btn');
  if (dropBtn) {
    dropBtn.addEventListener('click', async () => {
      if (confirm(`Are you sure you want to permanently DROP storage '${state.activeStorage}' from database '${state.activeDb}'?`)) {
        try {
          await api.deleteStorage(state.activeDb, state.activeStorage);
          showToast(`Dropped storage '${state.activeStorage}'`);
          state.activeStorage = null;
          await refreshDatabases();
        } catch (err) {
          showToast(`Failed to drop storage: ${err.message}`, 'error');
        }
      }
    });
  }

  // Card & Table Actions: Copy, Edit, Delete
  document.querySelectorAll('.copy-doc-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const key = btn.getAttribute('data-key');
      const item = state.items.find(i => i.key === key);
      if (item) {
        navigator.clipboard.writeText(JSON.stringify(item.value, null, 2));
        showToast(`Copied document '${key}' to clipboard`);
      }
    });
  });

  document.querySelectorAll('.edit-doc-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const key = btn.getAttribute('data-key');
      const item = state.items.find(i => i.key === key);
      if (item) {
        openEditDocModal(key, item.value, async (k, updatedVal) => {
          try {
            await api.upsertItem(state.activeDb, state.activeStorage, k, updatedVal, state.activeStorageType);
            showToast(`Updated document '${k}'`);
            await loadStorageItems();
          } catch (err) {
            showToast(`Failed to update: ${err.message}`, 'error');
          }
        });
      }
    });
  });

  document.querySelectorAll('.delete-doc-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
      const key = btn.getAttribute('data-key');
      if (confirm(`Delete document '${key}'?`)) {
        try {
          await api.deleteItem(state.activeDb, state.activeStorage, key, state.activeStorageType);
          showToast(`Deleted document '${key}'`);
          await loadStorageItems();
        } catch (err) {
          showToast(`Failed to delete document: ${err.message}`, 'error');
        }
      }
    });
  });
}

// Launch
init();
