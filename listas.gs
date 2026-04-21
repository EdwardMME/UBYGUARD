/**
 * UBYGUARD - Listas de valores + carga inicial combinada. Requiere sesión.
 */

function obtenerResponsables(token) {
  return conSesion_(token, ROLES.AUXILIAR, function() {
    const cached = cacheObtenerSimple_(CACHE_KEYS.RESPONSABLES);
    if (cached && Array.isArray(cached)) return { exito: true, responsables: cached };

    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.LISTAS_CONTROL);
    if (!sheet) return { exito: true, responsables: [] };

    const responsables = sheet.getRange("C2:C").getValues().flat().filter(String);
    cachePonerSimple_(CACHE_KEYS.RESPONSABLES, responsables, CACHE_TTL.RESPONSABLES);
    return { exito: true, responsables: responsables };
  });
}

function invalidarResponsables() {
  cacheInvalidarSimple_(CACHE_KEYS.RESPONSABLES);
}

/**
 * Carga inicial combinada post-login.
 * Devuelve responsables + dashboard + info de la sesión + índice SAP.
 */
function obtenerDatosIniciales(token) {
  return conSesion_(token, ROLES.AUXILIAR, function(sesion) {
    const sheet = asegurarHojaUsuarios_();
    const fila = buscarFilaUsuario_(sheet, sesion.usuario);
    const nombreUsuario = fila ? (fila.data[USUARIOS_COLS.NOMBRE - 1] || sesion.usuario) : sesion.usuario;

    const respCached = cacheObtenerSimple_(CACHE_KEYS.RESPONSABLES);
    let responsables;
    if (respCached && Array.isArray(respCached)) {
      responsables = respCached;
    } else {
      const listaSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.LISTAS_CONTROL);
      responsables = listaSheet ? listaSheet.getRange("C2:C").getValues().flat().filter(String) : [];
      cachePonerSimple_(CACHE_KEYS.RESPONSABLES, responsables, CACHE_TTL.RESPONSABLES);
    }

    // Resumen inline (sin volver a pasar por conSesion_)
    let resumen = cacheObtenerSimple_(CACHE_KEYS.RESUMEN_INICIO);
    if (!resumen) {
      const r = obtenerResumenInicio(token);
      resumen = r;
      delete resumen._token;
      delete resumen.exito;
    }

    return {
      exito: true,
      responsables: responsables,
      resumen: resumen,
      usuario: sesion.usuario,
      nombre: nombreUsuario,
      rol: sesion.rol,
      indiceSap: obtenerEstadoIndice()
    };
  });
}
