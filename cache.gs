/**
 * UBYGUARD - Wrapper de CacheService con chunking.
 * CacheService limita 100KB por entrada; serializamos en piezas
 * para soportar estructuras grandes (índice DATA_SAP con 15k+ filas).
 */

const CACHE_CHUNK_SIZE = 90000;

function cachePonerJSON_(keyBase, payload, ttlSegundos) {
  const cache = CacheService.getScriptCache();
  const json = JSON.stringify(payload);
  const chunks = [];
  for (let i = 0; i < json.length; i += CACHE_CHUNK_SIZE) {
    chunks.push(json.substring(i, i + CACHE_CHUNK_SIZE));
  }
  const meta = { total: chunks.length, ts: Date.now(), v: 1 };
  const entradas = {};
  entradas[keyBase + "_meta"] = JSON.stringify(meta);
  for (let i = 0; i < chunks.length; i++) {
    entradas[keyBase + "_" + i] = chunks[i];
  }
  try {
    cache.putAll(entradas, Math.min(ttlSegundos || 3600, 21600));
  } catch (err) {
    cacheInvalidar_(keyBase);
  }
  return meta;
}

function cacheObtenerJSON_(keyBase) {
  const cache = CacheService.getScriptCache();
  const metaRaw = cache.get(keyBase + "_meta");
  if (!metaRaw) return null;
  let meta;
  try { meta = JSON.parse(metaRaw); } catch (e) { return null; }
  if (!meta || !meta.total) return null;
  const keys = [];
  for (let i = 0; i < meta.total; i++) keys.push(keyBase + "_" + i);
  const result = cache.getAll(keys);
  let json = "";
  for (let i = 0; i < meta.total; i++) {
    const piece = result[keyBase + "_" + i];
    if (piece == null) return null;
    json += piece;
  }
  try { return JSON.parse(json); } catch (e) { return null; }
}

function cacheInvalidar_(keyBase) {
  const cache = CacheService.getScriptCache();
  const metaRaw = cache.get(keyBase + "_meta");
  if (!metaRaw) {
    try { cache.remove(keyBase); } catch (e) {}
    return;
  }
  let meta;
  try { meta = JSON.parse(metaRaw); } catch (e) { return; }
  const keys = [keyBase + "_meta"];
  for (let i = 0; i < meta.total; i++) keys.push(keyBase + "_" + i);
  try { cache.removeAll(keys); } catch (e) {}
}

function cachePonerSimple_(key, valor, ttl) {
  try {
    CacheService.getScriptCache().put(key, JSON.stringify(valor), Math.min(ttl || 3600, 21600));
  } catch (e) {}
}

function cacheObtenerSimple_(key) {
  const raw = CacheService.getScriptCache().get(key);
  if (!raw) return null;
  try { return JSON.parse(raw); } catch (e) { return null; }
}

function cacheInvalidarSimple_(key) {
  try { CacheService.getScriptCache().remove(key); } catch (e) {}
}
