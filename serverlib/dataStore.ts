/**
 * Plush Comfort — persistent data store
 *
 * Two interchangeable back-ends, chosen automatically from environment variables:
 *
 *  1. MySQL / MariaDB (recommended for live hosting, e.g. Hostinger)
 *     Set DATABASE_URL="mysql://USER:PASSWORD@HOST:3306/DATABASE"
 *     A single table `pc_store` (key VARCHAR, value LONGTEXT JSON) is created automatically.
 *
 *  2. JSON files (default — zero setup)
 *     Files are written to ./data (or DATA_DIR). Works on any VPS / Node host with a persistent disk.
 *
 * All reads are served from an in-memory cache so request handlers stay synchronous.
 * In MySQL mode the cache is refreshed from the database (at most every few seconds) so that
 * several server instances always show the same live data.
 */
import fs from 'fs';
import path from 'path';

type Json = any;

interface Backend {
  name: 'mysql' | 'file';
  loadAll(): Promise<Record<string, Json>>;
  save(key: string, value: Json): Promise<void>;
}

const cache = new Map<string, Json>();
let backend: Backend;
let lastRefresh = 0;
const REFRESH_MS = Number(process.env.DB_REFRESH_MS || 3000);

function fileBackend(dir: string): Backend {
  try {
    fs.mkdirSync(dir, { recursive: true });
  } catch {}
  return {
    name: 'file',
    async loadAll() {
      const out: Record<string, Json> = {};
      try {
        for (const f of fs.readdirSync(dir)) {
          if (!f.endsWith('.json')) continue;
          try {
            out[f.replace(/\.json$/, '')] = JSON.parse(fs.readFileSync(path.join(dir, f), 'utf-8'));
          } catch (err) {
            console.error(`[dataStore] Could not parse ${f}:`, err);
          }
        }
      } catch {}
      return out;
    },
    async save(key, value) {
      const target = path.join(dir, `${key}.json`);
      const tmp = `${target}.tmp`;
      fs.writeFileSync(tmp, JSON.stringify(value, null, 2), 'utf-8');
      fs.renameSync(tmp, target); // atomic replace — no half-written files
    },
  };
}

async function mysqlBackend(url: string): Promise<Backend> {
  const mysql = await import('mysql2/promise');
  const pool = mysql.createPool({
    uri: url,
    waitForConnections: true,
    connectionLimit: 5,
    ssl: process.env.DATABASE_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
  });
  await pool.query(
    `CREATE TABLE IF NOT EXISTS pc_store (
       store_key VARCHAR(120) NOT NULL PRIMARY KEY,
       store_value LONGTEXT NOT NULL,
       updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
     ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`
  );
  return {
    name: 'mysql',
    async loadAll() {
      const [rows] = await pool.query('SELECT store_key, store_value FROM pc_store');
      const out: Record<string, Json> = {};
      for (const r of rows as Array<{ store_key: string; store_value: string }>) {
        try {
          out[r.store_key] = JSON.parse(r.store_value);
        } catch {}
      }
      return out;
    },
    async save(key, value) {
      await pool.query(
        'INSERT INTO pc_store (store_key, store_value) VALUES (?, ?) ON DUPLICATE KEY UPDATE store_value = VALUES(store_value)',
        [key, JSON.stringify(value)]
      );
    },
  };
}

let initPromise: Promise<void> | null = null;

export function initDataStore(fileDir: string): Promise<void> {
  if (initPromise) return initPromise;
  initPromise = (async () => {
    const url = process.env.DATABASE_URL;
    if (url && url.startsWith('mysql')) {
      try {
        backend = await mysqlBackend(url);
        console.log('[dataStore] Connected to MySQL database.');
        // First run: migrate any existing JSON files into MySQL so nothing is lost
        const existing = await backend.loadAll();
        if (Object.keys(existing).length === 0) {
          const legacy = await fileBackend(fileDir).loadAll();
          for (const [k, v] of Object.entries(legacy)) await backend.save(k, v);
          if (Object.keys(legacy).length) console.log(`[dataStore] Migrated ${Object.keys(legacy).length} JSON files into MySQL.`);
        }
      } catch (err) {
        console.error('[dataStore] MySQL connection failed — falling back to JSON files:', err);
        backend = fileBackend(fileDir);
      }
    } else {
      backend = fileBackend(fileDir);
      console.log(`[dataStore] Using JSON file storage in ${fileDir}`);
    }
    const all = await backend.loadAll();
    cache.clear();
    for (const [k, v] of Object.entries(all)) cache.set(k, v);
    lastRefresh = Date.now();
  })();
  return initPromise;
}

/** Re-sync the cache from MySQL when it is older than REFRESH_MS (no-op for file storage). */
export async function refreshIfStale(): Promise<void> {
  if (!backend || backend.name !== 'mysql') return;
  if (Date.now() - lastRefresh < REFRESH_MS) return;
  lastRefresh = Date.now();
  try {
    const all = await backend.loadAll();
    for (const [k, v] of Object.entries(all)) cache.set(k, v);
  } catch (err) {
    console.warn('[dataStore] refresh failed:', err);
  }
}

export function readData<T>(key: string, fallback: T): T {
  return cache.has(key) ? (cache.get(key) as T) : fallback;
}

export function writeData<T>(key: string, value: T): void {
  cache.set(key, value);
  if (!backend) return;
  backend.save(key, value).catch((err) => console.error(`[dataStore] save "${key}" failed:`, err));
}

export function storageBackendName(): string {
  return backend?.name || 'starting';
}
