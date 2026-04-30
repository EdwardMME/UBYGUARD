/**
 * UBYGUARD - Búsqueda en DATA_SAP. Requiere sesión activa (AUXILIAR+).
 */

function buscarInventario(token, tipo, valor) {
  return conSesion_(token, ROLES.COMERCIAL, function() {
    const tipoNormal = normalizarMayus(tipo);
    const valorNormal = normalizarTexto(valor);
    if (!valorNormal) return { exito: true, resultados: [] };
    const resultados = buscarEnIndice_(tipoNormal, valorNormal, LIMITES.RESULTADOS_BUSQUEDA);
    return { exito: true, resultados: resultados };
  });
}

function sugerirPartes(token, prefijo) {
  return conSesion_(token, ROLES.COMERCIAL, function() {
    return { exito: true, sugerencias: autocompletarParte(prefijo) };
  });
}
