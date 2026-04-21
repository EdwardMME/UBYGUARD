/**
 * UBYGUARD - Búsqueda en DATA_SAP.
 * Usa índice cacheado en memoria → O(1) exacta / O(n) in-memory substring.
 * Sin esta capa, cada búsqueda leía 15k filas completas desde Sheets.
 */

function buscarInventario(tipo, valor) {
  try {
    const tipoNormal = normalizarMayus(tipo);
    const valorNormal = normalizarTexto(valor);
    if (!valorNormal) return [];
    return buscarEnIndice_(tipoNormal, valorNormal, LIMITES.RESULTADOS_BUSQUEDA);
  } catch (e) {
    return [];
  }
}

/**
 * Wrapper de autocomplete expuesto al frontend. Alias de indice.gs.
 */
function sugerirPartes(prefijo) {
  try {
    return autocompletarParte(prefijo);
  } catch (e) {
    return [];
  }
}
