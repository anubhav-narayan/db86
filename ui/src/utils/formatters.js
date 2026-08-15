/**
 * Utility functions for JSON syntax highlighting and code generation
 */

export function highlightJson(json) {
  if (typeof json !== 'string') {
    json = JSON.stringify(json, null, 2);
  }
  
  if (!json) return '';

  json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
    let cls = 'json-number';
    if (/^"/.test(match)) {
      if (/:$/.test(match)) {
        cls = 'json-key';
      } else {
        cls = 'json-string';
      }
    } else if (/true|false/.test(match)) {
      cls = 'json-boolean';
    } else if (/null/.test(match)) {
      cls = 'json-null';
    }
    return '<span class="' + cls + '">' + match + '</span>';
  });
}

export function generateSnippets(baseUrl, dbName, storageName) {
  const host = baseUrl || 'http://127.0.0.1:8000';
  const db = dbName || 'mydb';
  const storage = storageName || 'users';

  const curlSnippet = 
`# 1. Health check
curl -X GET "${host}/"

# 2. Get items in collection
curl -X GET "${host}/databases/${db}/storages/${storage}/items?limit=10"

# 3. Insert or update item
curl -X PUT "${host}/databases/${db}/storages/${storage}/items/user_101" \\
  -H "Content-Type: application/json" \\
  -d '{"value": {"name": "Alice", "role": "admin", "tags": ["db", "fastapi"]}}'

# 4. Path query (wildcards)
curl -X GET "${host}/databases/${db}/storages/${storage}/*/role"`;

  const pythonSnippet = 
`# Using Python db86 library directly
from db86 import Database

db = Database("${db}.db", autocommit=True, journal_mode="WAL")
store = db["${storage}"]  # JSONStorage

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
db.close()`;

  const fetchSnippet = 
`// Using JavaScript fetch
const baseUrl = "${host}";

// Insert document
await fetch(\`\${baseUrl}/databases/${db}/storages/${storage}/items/user_101\`, {
  method: 'PUT',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    value: { name: 'Alice', role: 'admin', tags: ['db', 'fastapi'] }
  })
});

// Fetch documents
const res = await fetch(\`\${baseUrl}/databases/${db}/storages/${storage}/items?limit=20\`);
const { items } = await res.json();
console.log(items);`;

  return { curlSnippet, pythonSnippet, fetchSnippet };
}
