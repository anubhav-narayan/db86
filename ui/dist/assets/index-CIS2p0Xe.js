(function(){const a=document.createElement("link").relList;if(a&&a.supports&&a.supports("modulepreload"))return;for(const o of document.querySelectorAll('link[rel="modulepreload"]'))s(o);new MutationObserver(o=>{for(const r of o)if(r.type==="childList")for(const i of r.addedNodes)i.tagName==="LINK"&&i.rel==="modulepreload"&&s(i)}).observe(document,{childList:!0,subtree:!0});function n(o){const r={};return o.integrity&&(r.integrity=o.integrity),o.referrerPolicy&&(r.referrerPolicy=o.referrerPolicy),o.crossOrigin==="use-credentials"?r.credentials="include":o.crossOrigin==="anonymous"?r.credentials="omit":r.credentials="same-origin",r}function s(o){if(o.ep)return;o.ep=!0;const r=n(o);fetch(o.href,r)}})();let E=localStorage.getItem("db86_host")||"http://127.0.0.1:8000";function N(){return E}function q(t){E=t.replace(/\/+$/,""),localStorage.setItem("db86_host",E)}async function b(t,a={}){const n=`${E}${t}`,s={"Content-Type":"application/json",...a.headers||{}},o=performance.now();try{const r=await fetch(n,{...a,headers:s}),i=Math.round(performance.now()-o);if(!r.ok){let l=`HTTP ${r.status} ${r.statusText}`;try{const d=await r.json();d.detail&&(l=d.detail)}catch{}throw new Error(l)}return{data:await r.json(),duration:i,ok:!0}}catch(r){const i=Math.round(performance.now()-o);throw{message:r.message||"Network connection failed",duration:i,ok:!1}}}const m={async getHealth(){return b("/")},async listDatabases(){return b("/databases")},async getDatabaseMetadata(t){return b(`/databases/${encodeURIComponent(t)}`)},async createDatabase({name:t,memory:a=!1,journal_mode:n="WAL",autocommit:s=!0,flag:o="c"}){return b("/databases",{method:"POST",body:JSON.stringify({name:t,memory:a,journal_mode:n,autocommit:s,flag:o})})},async closeDatabase(t){return b(`/databases/${encodeURIComponent(t)}/close`,{method:"POST"})},async deleteDatabase(t){return b(`/databases/${encodeURIComponent(t)}`,{method:"DELETE"})},async listStorages(t){return b(`/databases/${encodeURIComponent(t)}/storages`)},async getStorageMetadata(t,a,n){const s=n?`?storage_type=${n}`:"";return b(`/databases/${encodeURIComponent(t)}/storages/${encodeURIComponent(a)}${s}`)},async createStorage(t,{name:a,storage_type:n="json"}){return b(`/databases/${encodeURIComponent(t)}/storages`,{method:"POST",body:JSON.stringify({name:a,storage_type:n})})},async deleteStorage(t,a){return b(`/databases/${encodeURIComponent(t)}/storages/${encodeURIComponent(a)}`,{method:"DELETE"})},async listItems(t,a,{limit:n=50,offset:s=0,storage_type:o}={}){const r=new URLSearchParams;return n&&r.set("limit",n),s&&r.set("offset",s),o&&r.set("storage_type",o),b(`/databases/${encodeURIComponent(t)}/storages/${encodeURIComponent(a)}/items?${r.toString()}`)},async getItem(t,a,n,s){const o=s?`?storage_type=${s}`:"";return b(`/databases/${encodeURIComponent(t)}/storages/${encodeURIComponent(a)}/items/${encodeURIComponent(n)}${o}`)},async upsertItem(t,a,n,s,o){const r=o?`?storage_type=${o}`:"";return b(`/databases/${encodeURIComponent(t)}/storages/${encodeURIComponent(a)}/items/${encodeURIComponent(n)}${r}`,{method:"PUT",body:JSON.stringify({value:s})})},async bulkUpsertItems(t,a,n,s){const o=s?`?storage_type=${s}`:"";return b(`/databases/${encodeURIComponent(t)}/storages/${encodeURIComponent(a)}/items${o}`,{method:"POST",body:JSON.stringify({items:n})})},async deleteItem(t,a,n,s){const o=s?`?storage_type=${s}`:"";return b(`/databases/${encodeURIComponent(t)}/storages/${encodeURIComponent(a)}/items/${encodeURIComponent(n)}${o}`,{method:"DELETE"})},async queryPath(t,a,n,s){const o=s?`?storage_type=${s}`:"";return b(`/databases/${encodeURIComponent(t)}/storages/${encodeURIComponent(a)}/${n}${o}`)}};function P(t){return typeof t!="string"&&(t=JSON.stringify(t,null,2)),t?(t=t.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"),t.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,function(a){let n="json-number";return/^"/.test(a)?/:$/.test(a)?n="json-key":n="json-string":/true|false/.test(a)?n="json-boolean":/null/.test(a)&&(n="json-null"),'<span class="'+n+'">'+a+"</span>"})):""}function B(t,a,n){const s=t||"http://127.0.0.1:8000",o=a||"mydb",r=n||"users",i=`# 1. Health check
curl -X GET "${s}/"

# 2. Get items in collection
curl -X GET "${s}/databases/${o}/storages/${r}/items?limit=10"

# 3. Insert or update item
curl -X PUT "${s}/databases/${o}/storages/${r}/items/user_101" \\
  -H "Content-Type: application/json" \\
  -d '{"value": {"name": "Alice", "role": "admin", "tags": ["db", "fastapi"]}}'

# 4. Path query (wildcards)
curl -X GET "${s}/databases/${o}/storages/${r}/*/role"`,u=`# Using Python db86 library directly
from db86 import Database

db = Database("${o}.db", autocommit=True, journal_mode="WAL")
store = db["${r}"]  # JSONStorage

# Upsert document
store["user_101"] = {
    "name": "Alice",
    "role": "admin",
    "tags": ["db", "fastapi"]
}

# Read document
print(store["user_101"])

# Path query with wildcards
roles = list(store.get_path("*/role"))
print("Roles:", roles)
db.close()`,l=`// Using JavaScript fetch
const baseUrl = "${s}";

// Insert document
await fetch(\`\${baseUrl}/databases/${o}/storages/${r}/items/user_101\`, {
  method: 'PUT',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    value: { name: 'Alice', role: 'admin', tags: ['db', 'fastapi'] }
  })
});

// Fetch documents
const res = await fetch(\`\${baseUrl}/databases/${o}/storages/${r}/items?limit=20\`);
const { items } = await res.json();
console.log(items);`;return{curlSnippet:i,pythonSnippet:u,fetchSnippet:l}}function c(t,a="info"){let n=document.getElementById("toast-container");n||(n=document.createElement("div"),n.id="toast-container",n.className="toast-container",document.body.appendChild(n));const s=document.createElement("div");s.className=`toast ${a==="error"?"error":""}`,s.innerHTML=`
    <span>${a==="error"?"⚠️":"✅"}</span>
    <span>${t}</span>
  `,n.appendChild(s),setTimeout(()=>{s.style.opacity="0",s.style.transform="translateX(50px)",s.style.transition="all 0.3s",setTimeout(()=>s.remove(),300)},3500)}function x(t){f();const a=document.createElement("div");a.id="active-modal-overlay",a.className="modal-overlay",a.innerHTML=`
    <div class="modal-content">
      ${t}
    </div>
  `,a.addEventListener("click",n=>{n.target===a&&f()}),document.body.appendChild(a)}function f(){const t=document.getElementById("active-modal-overlay");t&&t.remove()}function L(t,a,n){const{curlSnippet:s,pythonSnippet:o,fetchSnippet:r}=B(t,a,n),i=`
    <div class="modal-header">
      <div class="modal-title">🔗 Connect to DB86 Cluster</div>
      <button class="btn-icon" onclick="window.closeModal()">✕</button>
    </div>
    <div class="modal-body">
      <div style="font-size:0.85rem; color:var(--text-secondary); margin-bottom:14px;">
        Connect your application or CLI tools to the local DB86 REST endpoint.
      </div>
      <div class="form-group">
        <label class="form-label">Cluster Host Endpoint</label>
        <div class="code-box" style="margin-top:0;">${t}</div>
      </div>

      <div style="margin-top: 18px;">
        <label class="form-label">cURL / CLI Request</label>
        <div class="code-box">${s.replace(/\n/g,"<br>")}</div>
      </div>

      <div style="margin-top: 18px;">
        <label class="form-label">Python (db86 SDK)</label>
        <div class="code-box">${o.replace(/\n/g,"<br>")}</div>
      </div>

      <div style="margin-top: 18px;">
        <label class="form-label">JavaScript (fetch API)</label>
        <div class="code-box">${r.replace(/\n/g,"<br>")}</div>
      </div>
    </div>
    <div class="modal-footer">
      <button class="btn btn-primary" onclick="window.closeModal()">Done</button>
    </div>
  `;x(i)}function U(t){x(`
    <div class="modal-header">
      <div class="modal-title">📁 Create Database</div>
      <button class="btn-icon" onclick="window.closeModal()">✕</button>
    </div>
    <form id="create-db-form">
      <div class="modal-body">
        <div class="form-group">
          <label class="form-label">Database Name *</label>
          <input type="text" id="new-db-name" class="form-input" placeholder="e.g. inventory_db" required autofocus />
        </div>
        
        <div class="form-group">
          <label class="form-label">Storage Persistence</label>
          <select id="new-db-memory" class="form-select">
            <option value="false">Disk File (.db file)</option>
            <option value="true">In-Memory (:memory:)</option>
          </select>
        </div>

        <div class="form-group">
          <label class="form-label">SQLite Journal Mode</label>
          <select id="new-db-journal" class="form-select">
            <option value="WAL">WAL (Write-Ahead Logging - Recommended)</option>
            <option value="DELETE">DELETE (Default SQLite rollback)</option>
            <option value="OFF">OFF (Maximum write speed)</option>
          </select>
        </div>

        <div class="form-group" style="display:flex; align-items:center; gap:8px;">
          <input type="checkbox" id="new-db-autocommit" checked style="accent-color:var(--brand-green); width:16px; height:16px;" />
          <label for="new-db-autocommit" style="font-size:0.85rem; color:var(--text-primary); cursor:pointer;">Autocommit changes immediately</label>
        </div>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary" onclick="window.closeModal()">Cancel</button>
        <button type="submit" class="btn btn-primary">Create Database</button>
      </div>
    </form>
  `),document.getElementById("create-db-form").addEventListener("submit",n=>{n.preventDefault();const s=document.getElementById("new-db-name").value.trim(),o=document.getElementById("new-db-memory").value==="true",r=document.getElementById("new-db-journal").value,i=document.getElementById("new-db-autocommit").checked;s&&(t({name:s,memory:o,journal_mode:r,autocommit:i}),f())})}function J(t,a){const n=`
    <div class="modal-header">
      <div class="modal-title">🗂️ Create Collection / Table</div>
      <button class="btn-icon" onclick="window.closeModal()">✕</button>
    </div>
    <form id="create-storage-form">
      <div class="modal-body">
        <div style="font-size:0.85rem; color:var(--text-secondary); margin-bottom:12px;">
          Database: <strong style="color:var(--text-primary);">${t}</strong>
        </div>
        <div class="form-group">
          <label class="form-label">Storage / Collection Name *</label>
          <input type="text" id="new-storage-name" class="form-input" placeholder="e.g. customers or products" required autofocus />
        </div>
        
        <div class="form-group">
          <label class="form-label">Storage Type</label>
          <select id="new-storage-type" class="form-select">
            <option value="json">JSON Document Store (Atlas NoSQL style - recommended)</option>
            <option value="table">Relational Table (Structured SQL columns)</option>
          </select>
        </div>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary" onclick="window.closeModal()">Cancel</button>
        <button type="submit" class="btn btn-primary">Create</button>
      </div>
    </form>
  `;x(n),document.getElementById("create-storage-form").addEventListener("submit",s=>{s.preventDefault();const o=document.getElementById("new-storage-name").value.trim(),r=document.getElementById("new-storage-type").value;o&&(a({name:o,storage_type:r}),f())})}function M(t,a,n,s){x(`
    <div class="modal-header">
      <div class="modal-title">➕ Insert Document / Record</div>
      <button class="btn-icon" onclick="window.closeModal()">✕</button>
    </div>
    <form id="insert-doc-form">
      <div class="modal-body">
        <div class="form-group">
          <label class="form-label">Document Key / Primary Key *</label>
          <input type="text" id="doc-key-input" class="form-input" placeholder="e.g. user_101 or item_alpha" required autofocus />
        </div>

        <div class="form-group">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
            <label class="form-label" style="margin:0;">JSON Value (Object) *</label>
            <button type="button" id="format-json-btn" class="btn btn-secondary" style="padding:2px 8px; font-size:0.75rem;">Format JSON</button>
          </div>
          <textarea id="doc-value-input" class="form-textarea" spellcheck="false" required>${n==="table"?`{
  "name": "Widget Pro",
  "price": 29.99,
  "stock": 100
}`:`{
  "name": "Jane Doe",
  "email": "jane@example.com",
  "status": "active",
  "profile": {
    "role": "admin",
    "tags": ["vip", "early-adopter"]
  }
}`}</textarea>
        </div>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary" onclick="window.closeModal()">Cancel</button>
        <button type="submit" class="btn btn-primary">Insert Document</button>
      </div>
    </form>
  `),document.getElementById("format-json-btn").addEventListener("click",()=>{const i=document.getElementById("doc-value-input").value;try{const u=JSON.parse(i);document.getElementById("doc-value-input").value=JSON.stringify(u,null,2)}catch{c("Invalid JSON syntax","error")}}),document.getElementById("insert-doc-form").addEventListener("submit",i=>{i.preventDefault();const u=document.getElementById("doc-key-input").value.trim(),l=document.getElementById("doc-value-input").value;try{const d=JSON.parse(l);s(u,d),f()}catch(d){c("Please provide valid JSON: "+d.message,"error")}})}function R(t,a,n){const s=JSON.stringify(a,null,2),o=`
    <div class="modal-header">
      <div class="modal-title">✏️ Edit Document: <code style="color:var(--brand-green);">${t}</code></div>
      <button class="btn-icon" onclick="window.closeModal()">✕</button>
    </div>
    <form id="edit-doc-form">
      <div class="modal-body">
        <div class="form-group">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
            <label class="form-label" style="margin:0;">JSON Payload</label>
            <button type="button" id="format-json-btn" class="btn btn-secondary" style="padding:2px 8px; font-size:0.75rem;">Format JSON</button>
          </div>
          <textarea id="edit-value-input" class="form-textarea" spellcheck="false" style="min-height:220px;" required>${s}</textarea>
        </div>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary" onclick="window.closeModal()">Cancel</button>
        <button type="submit" class="btn btn-primary">Save Changes</button>
      </div>
    </form>
  `;x(o),document.getElementById("format-json-btn").addEventListener("click",()=>{const r=document.getElementById("edit-value-input").value;try{const i=JSON.parse(r);document.getElementById("edit-value-input").value=JSON.stringify(i,null,2)}catch{c("Invalid JSON syntax","error")}}),document.getElementById("edit-doc-form").addEventListener("submit",r=>{r.preventDefault();const i=document.getElementById("edit-value-input").value;try{const u=JSON.parse(i);n(t,u),f()}catch(u){c("Please provide valid JSON: "+u.message,"error")}})}window.closeModal=f;const e={baseUrl:N(),isOnline:!1,ping:null,health:null,databases:[],dbTree:{},activeDb:null,activeStorage:null,activeStorageType:"json",activeTab:"documents",items:[],queryPath:"",limit:50,offset:0,searchFilter:"",lastDuration:null,isLoading:!1};async function j(){_(),F(),await T(),setInterval(async()=>{await z()},5e3)}function _(){const t=document.getElementById("app");t.innerHTML=`
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
          <div id="status-dot" class="status-dot ${e.isOnline?"online":"offline"}"></div>
          <span id="status-text">${e.isOnline?`Online (${e.ping}ms)`:"Offline"}</span>
        </div>
        <div style="color:var(--border-color);">|</div>
        <input type="text" id="host-url-input" class="host-input" value="${e.baseUrl}" title="DB86 REST Server Endpoint" />
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
        <a href="${e.baseUrl}/docs" target="_blank" class="btn btn-secondary" title="Open FastAPI Swagger Documentation">
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
  `}function F(){document.getElementById("nav-brand-btn").addEventListener("click",()=>{e.activeDb=null,e.activeStorage=null,S(),h()}),document.getElementById("reconnect-btn").addEventListener("click",async()=>{const t=document.getElementById("host-url-input").value.trim();t&&(q(t),e.baseUrl=t,await T())}),document.getElementById("nav-connect-btn").addEventListener("click",()=>{L(e.baseUrl,e.activeDb,e.activeStorage)}),document.getElementById("nav-create-db-btn").addEventListener("click",()=>{U(async t=>{try{await m.createDatabase(t),c(`Database '${t.name}' created successfully!`),await $(),D(t.name)}catch(a){c(`Failed to create database: ${a.message}`,"error")}})}),document.getElementById("sidebar-refresh-btn").addEventListener("click",async()=>{await T(),c("Refreshed database cluster state")}),document.getElementById("sidebar-search-input").addEventListener("input",t=>{e.searchFilter=t.target.value.toLowerCase(),S()})}async function z(){try{const{data:t,duration:a}=await m.getHealth();e.isOnline=!0,e.ping=a,e.health=t,I()}catch{e.isOnline=!1,e.ping=null,I()}}async function T(){try{const{data:t,duration:a}=await m.getHealth();e.isOnline=!0,e.ping=a,e.health=t,I(),await $()}catch{e.isOnline=!1,e.ping=null,I(),c("Could not connect to DB86 REST Server at "+e.baseUrl,"error"),h()}}function I(){const t=document.getElementById("status-dot"),a=document.getElementById("status-text");t&&a&&(t.className=`status-dot ${e.isOnline?"online":"offline"}`,a.textContent=e.isOnline?`Online (${e.ping}ms)`:"Offline")}async function $(){try{const{data:t}=await m.listDatabases();e.databases=t.databases||[],e.dbTree={};for(const a of e.databases)try{const[n,s]=await Promise.all([m.getDatabaseMetadata(a),m.listStorages(a)]);e.dbTree[a]={metadata:n.data,storages:s.data.storages||[]}}catch{e.dbTree[a]={metadata:{},storages:[]}}S(),h()}catch(t){console.error("Failed to list databases:",t)}}function S(){const t=document.getElementById("sidebar-db-tree");if(!t)return;if(e.databases.length===0){t.innerHTML=`
      <div style="padding: 24px 16px; text-align:center; color:var(--text-muted); font-size:0.85rem;">
        No open databases found.<br>
        <button id="empty-create-db-btn" class="btn btn-secondary" style="margin-top:10px; font-size:0.8rem;">
          ➕ Create First Database
        </button>
      </div>
    `;const s=document.getElementById("empty-create-db-btn");s&&s.addEventListener("click",()=>{document.getElementById("nav-create-db-btn").click()});return}let a="";const n=e.searchFilter;for(const s of e.databases){const r=(e.dbTree[s]||{storages:[]}).storages,i=s.toLowerCase().includes(n),u=r.filter(d=>d.name.toLowerCase().includes(n));if(n&&!i&&u.length===0)continue;const l=e.activeDb===s;a+=`
      <div class="db-item">
        <div class="db-header ${l&&!e.activeStorage?"active":""}" data-db="${s}">
          <div class="db-header-left">
            <span style="font-size:1rem;">🗄️</span>
            <span title="${s}">${s}</span>
          </div>
          <div class="db-actions">
            <button class="btn-icon add-storage-btn" data-db="${s}" title="Add Collection / Table">➕</button>
            <button class="btn-icon danger close-db-btn" data-db="${s}" title="Close Database">✕</button>
          </div>
        </div>

        <div class="storage-list">
          ${r.map(d=>{if(n&&!i&&!d.name.toLowerCase().includes(n))return"";const p=l&&e.activeStorage===d.name,v=d.storage_type==="json";return`
              <div class="storage-item ${p?"active":""}" data-db="${s}" data-storage="${d.name}" data-type="${d.storage_type}">
                <div class="storage-left">
                  <span>${v?"📄":"📊"}</span>
                  <span>${d.name}</span>
                </div>
                <span class="storage-badge ${v?"badge-json":"badge-table"}">
                  ${v?"JSON":"TABLE"}
                </span>
              </div>
            `}).join("")}
        </div>
      </div>
    `}t.innerHTML=a,t.querySelectorAll(".db-header").forEach(s=>{s.addEventListener("click",o=>{if(o.target.closest(".db-actions"))return;const r=s.getAttribute("data-db");D(r)})}),t.querySelectorAll(".storage-item").forEach(s=>{s.addEventListener("click",()=>{const o=s.getAttribute("data-db"),r=s.getAttribute("data-storage"),i=s.getAttribute("data-type");C(o,r,i)})}),t.querySelectorAll(".add-storage-btn").forEach(s=>{s.addEventListener("click",o=>{o.stopPropagation();const r=s.getAttribute("data-db");J(r,async i=>{try{await m.createStorage(r,i),c(`Created ${i.storage_type} storage '${i.name}'!`),await $(),C(r,i.name,i.storage_type)}catch(u){c(`Failed to create storage: ${u.message}`,"error")}})})}),t.querySelectorAll(".close-db-btn").forEach(s=>{s.addEventListener("click",async o=>{o.stopPropagation();const r=s.getAttribute("data-db");if(confirm(`Are you sure you want to close database '${r}'?`))try{await m.closeDatabase(r),c(`Closed database '${r}'`),e.activeDb===r&&(e.activeDb=null,e.activeStorage=null),await $()}catch(i){c(`Failed to close database: ${i.message}`,"error")}})})}function D(t){e.activeDb=t,e.activeStorage=null,e.items=[],S(),h()}async function C(t,a,n="json"){e.activeDb=t,e.activeStorage=a,e.activeStorageType=n,e.activeTab="documents",e.queryPath="",e.offset=0,S(),await g()}async function g(){if(!(!e.activeDb||!e.activeStorage)){e.isLoading=!0,h();try{let t;if(e.queryPath&&e.queryPath.trim()!=="")t=await m.queryPath(e.activeDb,e.activeStorage,e.queryPath.trim(),e.activeStorageType),e.items=(t.data.results||[]).map((a,n)=>({key:`match_${n+1}`,value:a}));else{t=await m.listItems(e.activeDb,e.activeStorage,{limit:e.limit,offset:e.offset,storage_type:e.activeStorageType});const a=t.data.items||[];Array.isArray(a)&&a.length>0&&Array.isArray(a[0])?e.items=a.map(([n,s])=>({key:n,value:s})):Array.isArray(a)&&a.length>0&&typeof a[0]=="object"&&"key"in a[0]?e.items=a:Array.isArray(a)?e.items=a.map((n,s)=>{const o=typeof n=="object"&&n!==null?Object.values(n)[0]||`row_${s}`:`row_${s}`;return{key:String(o),value:n}}):e.items=[]}e.lastDuration=t.duration}catch(t){c(`Error loading data: ${t.message}`,"error"),e.items=[]}finally{e.isLoading=!1,h()}}}function h(){const t=document.getElementById("main-content");t&&(!e.activeDb||!e.activeStorage?(t.innerHTML=H(),Q()):(t.innerHTML=V(),Z()))}function H(){var v,w;const t=e.health||{},a=t.system_metrics||{},n=a.process||{},s=a.system||{},o=t.uptime||"0d 00:00:00",r=e.databases.length;let i=0;for(const y of e.databases)i+=((w=(v=e.dbTree[y])==null?void 0:v.storages)==null?void 0:w.length)||0;const u=n.memory_mb||0,l=n.memory_percent||0,d=n.cpu_percent||0,p=s.disk_usage_percent||0;return`
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
          <div class="metric-value" style="color:${e.isOnline?"var(--brand-green)":"var(--brand-red)"};">
            ${e.isOnline?"ONLINE":"OFFLINE"}
          </div>
          <div class="metric-subtext">Uptime: ${o}</div>
        </div>

        <div class="metric-card">
          <div class="metric-card-top">
            <span class="metric-card-title">Databases & Storages</span>
            <span>🗄️</span>
          </div>
          <div class="metric-value">${r} / ${i}</div>
          <div class="metric-subtext">${r} active databases, ${i} collections</div>
        </div>

        <div class="metric-card">
          <div class="metric-card-top">
            <span class="metric-card-title">Memory Usage</span>
            <span>🧠</span>
          </div>
          <div class="metric-value">${u} <span style="font-size:1rem; font-weight:500;">MB</span></div>
          <div class="progress-bar-bg">
            <div class="progress-bar-fill" style="width:${Math.min(100,Math.max(5,l*5))}%;"></div>
          </div>
          <div class="metric-subtext">${l}% of host memory</div>
        </div>

        <div class="metric-card">
          <div class="metric-card-top">
            <span class="metric-card-title">System Load</span>
            <span>📊</span>
          </div>
          <div class="metric-value">${d}% <span style="font-size:1rem; font-weight:500;">CPU</span></div>
          <div class="progress-bar-bg">
            <div class="progress-bar-fill" style="width:${Math.min(100,Math.max(5,d))}%;"></div>
          </div>
          <div class="metric-subtext">Disk usage: ${p}%</div>
        </div>
      </div>

      <!-- Overview Details Section -->
      <div class="overview-sections">
        <!-- Database List Panel -->
        <div class="panel">
          <div class="panel-header">
            <div class="panel-title">Active Databases</div>
            <span style="font-size:0.85rem; color:var(--text-secondary);">${e.databases.length} Databases</span>
          </div>

          ${e.databases.length===0?`
            <div style="text-align:center; padding:30px; color:var(--text-muted);">
              No active databases. Click "New Database" above to create one.
            </div>
          `:`
            <div style="display:flex; flex-direction:column; gap:10px;">
              ${e.databases.map(y=>{const O=e.dbTree[y]||{},A=O.storages||[],k=O.metadata||{};return`
                  <div style="background:var(--bg-input); border:1px solid var(--border-color); border-radius:var(--radius-sm); padding:14px 18px; display:flex; justify-content:space-between; align-items:center;">
                    <div>
                      <div style="font-weight:700; font-size:0.95rem; display:flex; align-items:center; gap:8px;">
                        <span>🗄️</span> ${y}
                        <span style="font-size:0.7rem; background:rgba(255,255,255,0.06); padding:2px 6px; border-radius:4px; color:var(--text-secondary); font-family:var(--font-mono);">
                          ${k.filename||`${y}.db`}
                        </span>
                      </div>
                      <div style="font-size:0.8rem; color:var(--text-secondary); margin-top:4px;">
                        Journal: <code>${k.journal_mode||"WAL"}</code> · Autocommit: <code>${k.autocommit?"Yes":"No"}</code> · ${A.length} Collections/Tables
                      </div>
                    </div>
                    <div style="display:flex; gap:8px;">
                      <button class="btn btn-secondary overview-view-db-btn" data-db="${y}" style="padding:4px 10px; font-size:0.8rem;">Explore</button>
                    </div>
                  </div>
                `}).join("")}
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
  `}function Q(){const t=document.getElementById("overview-create-db-btn");t&&t.addEventListener("click",()=>{document.getElementById("nav-create-db-btn").click()});const a=document.getElementById("overview-connect-btn");a&&a.addEventListener("click",()=>{L(e.baseUrl,e.activeDb,e.activeStorage)});const n=document.getElementById("overview-open-connect");n&&n.addEventListener("click",()=>{L(e.baseUrl,e.activeDb,e.activeStorage)}),document.querySelectorAll(".overview-view-db-btn").forEach(s=>{s.addEventListener("click",()=>{var i;const o=s.getAttribute("data-db"),r=((i=e.dbTree[o])==null?void 0:i.storages)||[];r.length>0?C(o,r[0].name,r[0].storage_type):D(o)})})}function V(){const t=e.activeStorageType==="json";return`
    <div class="explorer-view">
      <!-- Storage Top Bar -->
      <div class="explorer-top-bar">
        <div class="storage-breadcrumbs">
          <span style="color:var(--text-secondary); cursor:pointer;" id="bc-db-name">🗄️ ${e.activeDb}</span>
          <span class="breadcrumb-sep">/</span>
          <span>${t?"📄":"📊"} ${e.activeStorage}</span>
          <span class="storage-badge ${t?"badge-json":"badge-table"}">
            ${t?"JSON Collection":"Relational Table"}
          </span>
        </div>

        <!-- Navigation Tabs -->
        <div class="tab-nav">
          <button class="tab-btn ${e.activeTab==="documents"?"active":""}" data-tab="documents">
            <span>📄</span> Documents
          </button>
          <button class="tab-btn ${e.activeTab==="table"?"active":""}" data-tab="table">
            <span>📊</span> Table Grid
          </button>
          <button class="tab-btn ${e.activeTab==="schema"?"active":""}" data-tab="schema">
            <span>📐</span> Schema & Indexes
          </button>
          <button class="tab-btn ${e.activeTab==="api"?"active":""}" data-tab="api">
            <span>⚡</span> REST API
          </button>
        </div>
      </div>

      <!-- Atlas Query / Find Bar (Active for Documents & Table tabs) -->
      ${e.activeTab==="documents"||e.activeTab==="table"?`
        <div class="atlas-query-bar">
          <div class="query-input-group">
            <span class="query-label">Find:</span>
            <input type="text" id="atlas-path-query-input" class="query-input" 
              placeholder="${t?"e.g. */email or users/*/role or alice/address":"e.g. path query"}" 
              value="${e.queryPath}" />
          </div>

          <div class="query-controls">
            <span style="font-size:0.8rem; color:var(--text-secondary);">Limit:</span>
            <input type="number" id="query-limit-input" class="limit-input" value="${e.limit}" min="1" max="1000" />
            
            <button id="query-execute-btn" class="btn btn-primary">
              <span>🔍</span> Execute
            </button>
            <button id="query-reset-btn" class="btn btn-secondary">
              Reset
            </button>
          </div>
        </div>
      `:""}

      <!-- Main Explorer Body -->
      <div class="explorer-content">
        ${W()}
      </div>
    </div>
  `}function W(){if(e.isLoading)return`
      <div style="padding:60px 0; text-align:center; color:var(--text-secondary);">
        <div style="font-size:2rem; margin-bottom:12px; animation:spin 1s linear infinite;">⏳</div>
        <div>Loading storage items...</div>
      </div>
    `;if(e.activeTab==="documents")return G();if(e.activeTab==="table")return K();if(e.activeTab==="schema")return X();if(e.activeTab==="api")return Y()}function G(){const t=e.queryPath&&e.queryPath.trim()!=="";return`
    <div class="explorer-actions-bar">
      <div class="doc-count-tag">
        Showing <strong>${e.items.length}</strong> items 
        ${e.lastDuration!==null?`<span style="margin-left:8px; font-family:var(--font-mono); color:var(--brand-green);">(${e.lastDuration}ms)</span>`:""}
        ${t?`<span style="margin-left:8px; color:#60a5fa; font-family:var(--font-mono);">[Filter: ${e.queryPath}]</span>`:""}
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

    ${e.items.length===0?`
      <div style="padding:50px 0; text-align:center; color:var(--text-muted); border:1px dashed var(--border-color); border-radius:var(--radius-md);">
        <div style="font-size:1.8rem; margin-bottom:8px;">📭</div>
        <div style="font-size:1rem; font-weight:600; color:var(--text-secondary); margin-bottom:6px;">No documents found</div>
        <p style="font-size:0.85rem; margin-bottom:14px;">This storage currently contains no items matching the query.</p>
        <button id="empty-insert-btn" class="btn btn-primary">Insert First Document</button>
      </div>
    `:`
      <div class="doc-cards-list">
        ${e.items.map(a=>{const n=P(a.value);return`
            <div class="doc-card" data-key="${a.key}">
              <div class="doc-card-header">
                <div class="doc-key">
                  <span>🔑</span>
                  <span>"${a.key}"</span>
                </div>
                <div class="doc-actions">
                  <button class="btn-icon copy-doc-btn" data-key="${a.key}" title="Copy JSON">📋</button>
                  <button class="btn-icon edit-doc-btn" data-key="${a.key}" title="Edit Document">✏️</button>
                  <button class="btn-icon danger delete-doc-btn" data-key="${a.key}" title="Delete Document">🗑️</button>
                </div>
              </div>
              <pre class="doc-body">${n}</pre>
            </div>
          `}).join("")}
      </div>
    `}
  `}function K(){if(e.items.length===0)return`
      <div style="padding:50px 0; text-align:center; color:var(--text-muted); border:1px dashed var(--border-color); border-radius:var(--radius-md);">
        No tabular rows found.
      </div>
    `;const t=new Set;t.add("_key"),e.items.forEach(n=>{typeof n.value=="object"&&n.value!==null&&!Array.isArray(n.value)?Object.keys(n.value).forEach(s=>t.add(s)):t.add("value")});const a=Array.from(t);return`
    <div class="explorer-actions-bar">
      <div class="doc-count-tag">
        Showing <strong>${e.items.length}</strong> rows 
        ${e.lastDuration!==null?`<span style="margin-left:8px; font-family:var(--font-mono); color:var(--brand-green);">(${e.lastDuration}ms)</span>`:""}
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
            ${a.map(n=>`<th>${n}</th>`).join("")}
            <th style="text-align:right;">Actions</th>
          </tr>
        </thead>
        <tbody>
          ${e.items.map(n=>`
              <tr>
                ${a.map(s=>{let o="";if(s==="_key")o=`<strong style="color:var(--brand-green-light);">${n.key}</strong>`;else if(typeof n.value=="object"&&n.value!==null&&s in n.value){const r=n.value[s];o=typeof r=="object"?JSON.stringify(r):String(r)}else s==="value"&&(o=typeof n.value=="object"?JSON.stringify(n.value):String(n.value));return`<td>${o}</td>`}).join("")}
                <td style="text-align:right;">
                  <button class="btn-icon edit-doc-btn" data-key="${n.key}" title="Edit">✏️</button>
                  <button class="btn-icon danger delete-doc-btn" data-key="${n.key}" title="Delete">🗑️</button>
                </td>
              </tr>
            `).join("")}
        </tbody>
      </table>
    </div>
  `}function X(){const a=(e.dbTree[e.activeDb]||{}).metadata||{};return`
    <div style="max-width:850px;">
      <div class="panel" style="margin-bottom:20px;">
        <div class="panel-header">
          <div class="panel-title">Storage Information</div>
        </div>
        <table class="atlas-table">
          <tbody>
            <tr><td style="width:200px; color:var(--text-secondary);">Database</td><td><strong>${e.activeDb}</strong></td></tr>
            <tr><td style="color:var(--text-secondary);">Storage Name</td><td><strong>${e.activeStorage}</strong></td></tr>
            <tr><td style="color:var(--text-secondary);">Storage Type</td><td><code>${e.activeStorageType}</code></td></tr>
            <tr><td style="color:var(--text-secondary);">Total Loaded Entries</td><td>${e.items.length}</td></tr>
            <tr><td style="color:var(--text-secondary);">Database File</td><td><code>${a.filename||`${e.activeDb}.db`}</code></td></tr>
            <tr><td style="color:var(--text-secondary);">Journal Mode</td><td><code>${a.journal_mode||"WAL"}</code></td></tr>
          </tbody>
        </table>
      </div>

      <div class="panel">
        <div class="panel-header">
          <div class="panel-title">Indexes & Views</div>
        </div>
        <div style="font-size:0.85rem; color:var(--text-secondary);">
          <div style="margin-bottom:8px;"><strong>Indices:</strong></div>
          <pre class="code-box">${JSON.stringify(a.indices||{},null,2)}</pre>
          <div style="margin-top:16px; margin-bottom:8px;"><strong>Views:</strong></div>
          <pre class="code-box">${JSON.stringify(a.views||{},null,2)}</pre>
        </div>
      </div>
    </div>
  `}function Y(){const{curlSnippet:t,pythonSnippet:a,fetchSnippet:n}=B(e.baseUrl,e.activeDb,e.activeStorage);return`
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
          <div class="code-box">${t.replace(/\n/g,"<br>")}</div>
        </div>

        <div style="margin-bottom:20px;">
          <label class="form-label">Python (db86 SDK)</label>
          <div class="code-box">${a.replace(/\n/g,"<br>")}</div>
        </div>

        <div>
          <label class="form-label">JavaScript (fetch API)</label>
          <div class="code-box">${n.replace(/\n/g,"<br>")}</div>
        </div>
      </div>
    </div>
  `}function Z(){const t=document.getElementById("bc-db-name");t&&t.addEventListener("click",()=>{D(e.activeDb)}),document.querySelectorAll(".tab-btn").forEach(l=>{l.addEventListener("click",()=>{e.activeTab=l.getAttribute("data-tab"),h()})});const a=document.getElementById("query-execute-btn"),n=document.getElementById("atlas-path-query-input"),s=document.getElementById("query-limit-input");a&&n&&(a.addEventListener("click",async()=>{e.queryPath=n.value,s&&(e.limit=parseInt(s.value)||50),await g()}),n.addEventListener("keydown",async l=>{l.key==="Enter"&&(e.queryPath=n.value,s&&(e.limit=parseInt(s.value)||50),await g())}));const o=document.getElementById("query-reset-btn");o&&o.addEventListener("click",async()=>{e.queryPath="",n&&(n.value=""),await g()});const r=document.getElementById("insert-doc-btn")||document.getElementById("insert-row-btn")||document.getElementById("empty-insert-btn");r&&r.addEventListener("click",()=>{M(e.activeDb,e.activeStorage,e.activeStorageType,async(l,d)=>{try{await m.upsertItem(e.activeDb,e.activeStorage,l,d,e.activeStorageType),c(`Saved item '${l}' successfully!`),await g()}catch(p){c(`Failed to save item: ${p.message}`,"error")}})});const i=document.getElementById("export-json-btn");i&&i.addEventListener("click",()=>{const l="data:text/json;charset=utf-8,"+encodeURIComponent(JSON.stringify(e.items,null,2)),d=document.createElement("a");d.setAttribute("href",l),d.setAttribute("download",`${e.activeDb}_${e.activeStorage}_export.json`),document.body.appendChild(d),d.click(),d.remove(),c("Exported items to JSON file")});const u=document.getElementById("drop-storage-btn");u&&u.addEventListener("click",async()=>{if(confirm(`Are you sure you want to permanently DROP storage '${e.activeStorage}' from database '${e.activeDb}'?`))try{await m.deleteStorage(e.activeDb,e.activeStorage),c(`Dropped storage '${e.activeStorage}'`),e.activeStorage=null,await $()}catch(l){c(`Failed to drop storage: ${l.message}`,"error")}}),document.querySelectorAll(".copy-doc-btn").forEach(l=>{l.addEventListener("click",()=>{const d=l.getAttribute("data-key"),p=e.items.find(v=>v.key===d);p&&(navigator.clipboard.writeText(JSON.stringify(p.value,null,2)),c(`Copied document '${d}' to clipboard`))})}),document.querySelectorAll(".edit-doc-btn").forEach(l=>{l.addEventListener("click",()=>{const d=l.getAttribute("data-key"),p=e.items.find(v=>v.key===d);p&&R(d,p.value,async(v,w)=>{try{await m.upsertItem(e.activeDb,e.activeStorage,v,w,e.activeStorageType),c(`Updated document '${v}'`),await g()}catch(y){c(`Failed to update: ${y.message}`,"error")}})})}),document.querySelectorAll(".delete-doc-btn").forEach(l=>{l.addEventListener("click",async()=>{const d=l.getAttribute("data-key");if(confirm(`Delete document '${d}'?`))try{await m.deleteItem(e.activeDb,e.activeStorage,d,e.activeStorageType),c(`Deleted document '${d}'`),await g()}catch(p){c(`Failed to delete document: ${p.message}`,"error")}})})}j();
