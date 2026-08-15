/**
 * DB86 REST API Client Service
 */

let baseUrl = localStorage.getItem('db86_host') || 'http://127.0.0.1:8000';

export function getBaseUrl() {
  return baseUrl;
}

export function setBaseUrl(url) {
  // Strip trailing slashes
  baseUrl = url.replace(/\/+$/, '');
  localStorage.setItem('db86_host', baseUrl);
}

async function request(endpoint, options = {}) {
  const url = `${baseUrl}${endpoint}`;
  const headers = {
    'Content-Type': 'application/json',
    ...(options.headers || {})
  };

  const startTime = performance.now();
  try {
    const response = await fetch(url, {
      ...options,
      headers
    });
    const duration = Math.round(performance.now() - startTime);

    if (!response.ok) {
      let errorDetail = `HTTP ${response.status} ${response.statusText}`;
      try {
        const errJson = await response.json();
        if (errJson.detail) errorDetail = errJson.detail;
      } catch (_) {}
      throw new Error(errorDetail);
    }

    const data = await response.json();
    return { data, duration, ok: true };
  } catch (error) {
    const duration = Math.round(performance.now() - startTime);
    throw { message: error.message || 'Network connection failed', duration, ok: false };
  }
}

export const api = {
  // Health & Metrics
  async getHealth() {
    return request('/');
  },

  // Databases
  async listDatabases() {
    return request('/databases');
  },

  async getDatabaseMetadata(dbName) {
    return request(`/databases/${encodeURIComponent(dbName)}`);
  },

  async createDatabase({ name, memory = false, journal_mode = 'WAL', autocommit = true, flag = 'c' }) {
    return request('/databases', {
      method: 'POST',
      body: JSON.stringify({ name, memory, journal_mode, autocommit, flag })
    });
  },

  async closeDatabase(dbName) {
    return request(`/databases/${encodeURIComponent(dbName)}/close`, {
      method: 'POST'
    });
  },

  async deleteDatabase(dbName) {
    return request(`/databases/${encodeURIComponent(dbName)}`, {
      method: 'DELETE'
    });
  },

  // Storages (Collections & Tables)
  async listStorages(dbName) {
    return request(`/databases/${encodeURIComponent(dbName)}/storages`);
  },

  async getStorageMetadata(dbName, storageName, storageType) {
    const query = storageType ? `?storage_type=${storageType}` : '';
    return request(`/databases/${encodeURIComponent(dbName)}/storages/${encodeURIComponent(storageName)}${query}`);
  },

  async createStorage(dbName, { name, storage_type = 'json' }) {
    return request(`/databases/${encodeURIComponent(dbName)}/storages`, {
      method: 'POST',
      body: JSON.stringify({ name, storage_type })
    });
  },

  async deleteStorage(dbName, storageName) {
    return request(`/databases/${encodeURIComponent(dbName)}/storages/${encodeURIComponent(storageName)}`, {
      method: 'DELETE'
    });
  },

  // Items / Documents
  async listItems(dbName, storageName, { limit = 50, offset = 0, storage_type } = {}) {
    const params = new URLSearchParams();
    if (limit) params.set('limit', limit);
    if (offset) params.set('offset', offset);
    if (storage_type) params.set('storage_type', storage_type);
    
    return request(`/databases/${encodeURIComponent(dbName)}/storages/${encodeURIComponent(storageName)}/items?${params.toString()}`);
  },

  async getItem(dbName, storageName, itemKey, storageType) {
    const query = storageType ? `?storage_type=${storageType}` : '';
    return request(`/databases/${encodeURIComponent(dbName)}/storages/${encodeURIComponent(storageName)}/items/${encodeURIComponent(itemKey)}${query}`);
  },

  async upsertItem(dbName, storageName, itemKey, value, storageType) {
    const query = storageType ? `?storage_type=${storageType}` : '';
    return request(`/databases/${encodeURIComponent(dbName)}/storages/${encodeURIComponent(storageName)}/items/${encodeURIComponent(itemKey)}${query}`, {
      method: 'PUT',
      body: JSON.stringify({ value })
    });
  },

  async bulkUpsertItems(dbName, storageName, itemsDict, storageType) {
    const query = storageType ? `?storage_type=${storageType}` : '';
    return request(`/databases/${encodeURIComponent(dbName)}/storages/${encodeURIComponent(storageName)}/items${query}`, {
      method: 'POST',
      body: JSON.stringify({ items: itemsDict })
    });
  },

  async deleteItem(dbName, storageName, itemKey, storageType) {
    const query = storageType ? `?storage_type=${storageType}` : '';
    return request(`/databases/${encodeURIComponent(dbName)}/storages/${encodeURIComponent(storageName)}/items/${encodeURIComponent(itemKey)}${query}`, {
      method: 'DELETE'
    });
  },

  // Path Query (JSON storage)
  async queryPath(dbName, storageName, pathQuery, storageType) {
    const query = storageType ? `?storage_type=${storageType}` : '';
    // path query can have slashes
    return request(`/databases/${encodeURIComponent(dbName)}/storages/${encodeURIComponent(storageName)}/${pathQuery}${query}`);
  }
};
