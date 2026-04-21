/**
 * UBYGUARD - Listas de valores (responsables, etc).
 * Cachea 1h porque rara vez cambia; evita la llamada a Sheets en cada carga.
 */

function obtenerResponsables() {
  try {
    const cached = cacheObtenerSimple_(CACHE_KEYS.RESPONSABLES);
    if (cached && Array.isArray(cached)) return cached;

    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.LISTAS_CONTROL);
    if (!sheet) return [];

    const responsables = sheet.getRange("C2:C").getValues().flat().filter(String);
    cachePonerSimple_(CACHE_KEYS.RESPONSABLES, responsables, CACHE_TTL.RESPONSABLES);
    return responsables;
  } catch (e) {
    return [];
  }
}

function invalidarResponsables() {
  cacheInvalidarSimple_(CACHE_KEYS.RESPONSABLES);
}

/**
 * Carga inicial combinada: en una sola llamada del frontend devolvemos
 * responsables + dashboard + total SAP. Reduce latencia de arranque.
 */
function obtenerDatosIniciales() {
  return {
    responsables: obtenerResponsables(),
    resumen: obtenerResumenInicio(),
    usuario: usuarioActual_(),
    indiceSap: obtenerEstadoIndice()
  };
}
