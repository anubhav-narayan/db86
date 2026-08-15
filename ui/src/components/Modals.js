/**
 * Modal Dialogs & Toast Notifications
 */

import { generateSnippets } from '../utils/formatters.js';

export function showToast(message, type = 'info') {
  let container = document.getElementById('toast-container');
  if (!container) {
    container = document.createElement('div');
    container.id = 'toast-container';
    container.className = 'toast-container';
    document.body.appendChild(container);
  }

  const toast = document.createElement('div');
  toast.className = `toast ${type === 'error' ? 'error' : ''}`;
  toast.innerHTML = `
    <span>${type === 'error' ? '⚠️' : '✅'}</span>
    <span>${message}</span>
  `;
  container.appendChild(toast);

  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateX(50px)';
    toast.style.transition = 'all 0.3s';
    setTimeout(() => toast.remove(), 300);
  }, 3500);
}

export function openModal(contentHtml) {
  closeModal();
  const overlay = document.createElement('div');
  overlay.id = 'active-modal-overlay';
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `
    <div class="modal-content">
      ${contentHtml}
    </div>
  `;
  
  overlay.addEventListener('click', (e) => {
    if (e.target === overlay) closeModal();
  });
  
  document.body.appendChild(overlay);
}

export function closeModal() {
  const existing = document.getElementById('active-modal-overlay');
  if (existing) existing.remove();
}

export function openConnectModal(baseUrl, activeDb, activeStorage) {
  const { curlSnippet, pythonSnippet, fetchSnippet } = generateSnippets(baseUrl, activeDb, activeStorage);

  const html = `
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
        <div class="code-box" style="margin-top:0;">${baseUrl}</div>
      </div>

      <div style="margin-top: 18px;">
        <label class="form-label">cURL / CLI Request</label>
        <div class="code-box">${curlSnippet.replace(/\n/g, '<br>')}</div>
      </div>

      <div style="margin-top: 18px;">
        <label class="form-label">Python (db86 SDK)</label>
        <div class="code-box">${pythonSnippet.replace(/\n/g, '<br>')}</div>
      </div>

      <div style="margin-top: 18px;">
        <label class="form-label">JavaScript (fetch API)</label>
        <div class="code-box">${fetchSnippet.replace(/\n/g, '<br>')}</div>
      </div>
    </div>
    <div class="modal-footer">
      <button class="btn btn-primary" onclick="window.closeModal()">Done</button>
    </div>
  `;
  openModal(html);
}

export function openCreateDbModal(onSubmit) {
  const html = `
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
  `;
  openModal(html);

  document.getElementById('create-db-form').addEventListener('submit', (e) => {
    e.preventDefault();
    const name = document.getElementById('new-db-name').value.trim();
    const memory = document.getElementById('new-db-memory').value === 'true';
    const journal_mode = document.getElementById('new-db-journal').value;
    const autocommit = document.getElementById('new-db-autocommit').checked;

    if (!name) return;
    onSubmit({ name, memory, journal_mode, autocommit });
    closeModal();
  });
}

export function openCreateStorageModal(dbName, onSubmit) {
  const html = `
    <div class="modal-header">
      <div class="modal-title">🗂️ Create Collection / Table</div>
      <button class="btn-icon" onclick="window.closeModal()">✕</button>
    </div>
    <form id="create-storage-form">
      <div class="modal-body">
        <div style="font-size:0.85rem; color:var(--text-secondary); margin-bottom:12px;">
          Database: <strong style="color:var(--text-primary);">${dbName}</strong>
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
  `;
  openModal(html);

  document.getElementById('create-storage-form').addEventListener('submit', (e) => {
    e.preventDefault();
    const name = document.getElementById('new-storage-name').value.trim();
    const storage_type = document.getElementById('new-storage-type').value;

    if (!name) return;
    onSubmit({ name, storage_type });
    closeModal();
  });
}

export function openInsertDocModal(dbName, storageName, storageType, onSubmit) {
  const sampleJson = storageType === 'table' 
    ? '{\n  "name": "Widget Pro",\n  "price": 29.99,\n  "stock": 100\n}'
    : '{\n  "name": "Jane Doe",\n  "email": "jane@example.com",\n  "status": "active",\n  "profile": {\n    "role": "admin",\n    "tags": ["vip", "early-adopter"]\n  }\n}';

  const html = `
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
          <textarea id="doc-value-input" class="form-textarea" spellcheck="false" required>${sampleJson}</textarea>
        </div>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary" onclick="window.closeModal()">Cancel</button>
        <button type="submit" class="btn btn-primary">Insert Document</button>
      </div>
    </form>
  `;
  openModal(html);

  document.getElementById('format-json-btn').addEventListener('click', () => {
    const val = document.getElementById('doc-value-input').value;
    try {
      const parsed = JSON.parse(val);
      document.getElementById('doc-value-input').value = JSON.stringify(parsed, null, 2);
    } catch (e) {
      showToast('Invalid JSON syntax', 'error');
    }
  });

  document.getElementById('insert-doc-form').addEventListener('submit', (e) => {
    e.preventDefault();
    const key = document.getElementById('doc-key-input').value.trim();
    const rawVal = document.getElementById('doc-value-input').value;

    try {
      const value = JSON.parse(rawVal);
      onSubmit(key, value);
      closeModal();
    } catch (err) {
      showToast('Please provide valid JSON: ' + err.message, 'error');
    }
  });
}

export function openEditDocModal(key, currentValue, onSubmit) {
  const formattedJson = JSON.stringify(currentValue, null, 2);

  const html = `
    <div class="modal-header">
      <div class="modal-title">✏️ Edit Document: <code style="color:var(--brand-green);">${key}</code></div>
      <button class="btn-icon" onclick="window.closeModal()">✕</button>
    </div>
    <form id="edit-doc-form">
      <div class="modal-body">
        <div class="form-group">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
            <label class="form-label" style="margin:0;">JSON Payload</label>
            <button type="button" id="format-json-btn" class="btn btn-secondary" style="padding:2px 8px; font-size:0.75rem;">Format JSON</button>
          </div>
          <textarea id="edit-value-input" class="form-textarea" spellcheck="false" style="min-height:220px;" required>${formattedJson}</textarea>
        </div>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary" onclick="window.closeModal()">Cancel</button>
        <button type="submit" class="btn btn-primary">Save Changes</button>
      </div>
    </form>
  `;
  openModal(html);

  document.getElementById('format-json-btn').addEventListener('click', () => {
    const val = document.getElementById('edit-value-input').value;
    try {
      const parsed = JSON.parse(val);
      document.getElementById('edit-value-input').value = JSON.stringify(parsed, null, 2);
    } catch (e) {
      showToast('Invalid JSON syntax', 'error');
    }
  });

  document.getElementById('edit-doc-form').addEventListener('submit', (e) => {
    e.preventDefault();
    const rawVal = document.getElementById('edit-value-input').value;
    try {
      const value = JSON.parse(rawVal);
      onSubmit(key, value);
      closeModal();
    } catch (err) {
      showToast('Please provide valid JSON: ' + err.message, 'error');
    }
  });
}
