/**
 * UBYGUARD - Movimientos individuales.
 * Busca por parte o código. Requiere sesión (AUXILIAR+).
 */

function buscarArticulo(token, identificador) {
  return conSesion_(token, ROLES.COMERCIAL, function() {
    const art = obtenerArticuloPorIdentificador_(identificador);
    return { exito: true, articulo: art };
  });
}

function registrarMovimiento(token, datos) {
  return conSesion_(token, ROLES.AUXILIAR, function(sesion) {
    if (!datos) return { exito: false, mensaje: "Faltan datos" };

    const parte = validarTexto_(datos.parte, REGEX.NUMERO_PARTE, "número de parte");
    if (!parte.ok) return { exito: false, mensaje: parte.mensaje };

    const destino = validarTexto_(datos.destino, REGEX.UBICACION, "ubicación destino");
    if (!destino.ok) return { exito: false, mensaje: destino.mensaje };

    const responsable = validarTexto_(datos.responsable, null, "responsable");
    if (!responsable.ok) return { exito: false, mensaje: responsable.mensaje };

    const cantidad = validarCantidad_(datos.cantidad, "cantidad");
    if (!cantidad.ok) return { exito: false, mensaje: cantidad.mensaje };

    const articulo = obtenerArticuloPorIdentificador_(parte.valor);
    if (!articulo) {
      return { exito: false, mensaje: "El número de parte o código no existe en DATA_SAP" };
    }

    const documento = normalizarTexto(datos.documento);
    const tipo = normalizarMayus(datos.tipo) || TIPO_MOVIMIENTO.INDIVIDUAL;

    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.BASE_OPERATIVA);
    if (!sheet) return { exito: false, mensaje: "No existe la hoja BASE_OPERATIVA" };

    const idMovimiento = generarID();

    const fila = [
      idMovimiento,
      new Date(),
      tipo,
      documento,
      articulo.parte || parte.valor,
      articulo.codigo || "",
      articulo.descripcion || "",
      cantidad.valor,
      articulo.ubicacion || "",
      destino.valor.toUpperCase(),
      responsable.valor,
      ESTADO_MOVIMIENTO.UBICADO,
      false,
      "",
      "",
      ""
    ];

    sheet.appendRow(fila);
    cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);

    return {
      exito: true,
      idMovimiento: idMovimiento,
      registradoPor: sesion.usuario
    };
  });
}

/**
 * Registra varios movimientos en una sola operación atómica.
 * Diseñado para el carrito de movimientos (escaneo masivo).
 *
 * Validaciones GLOBALES (una vez): destino, responsable, tipo, documento.
 * Validaciones POR ITEM: existencia en DATA_SAP, cantidad > 0.
 *
 * Ventajas vs N llamadas a registrarMovimiento:
 *   - 1 sola adquisición de LockService (evita race conditions)
 *   - 1 sola escritura getRange().setValues() en bloque (vs N appendRow)
 *   - Si una validación crítica falla, no se escribe nada (atomicidad)
 */
function registrarMovimientosBatch(token, payload) {
  return conSesion_(token, ROLES.AUXILIAR, function(sesion) {
    if (!payload || !Array.isArray(payload.items) || payload.items.length === 0) {
      return { exito: false, mensaje: "Sin items para registrar" };
    }
    if (payload.items.length > 100) {
      return { exito: false, mensaje: "Máximo 100 items por lote" };
    }

    // Validaciones globales
    const destino = validarTexto_(payload.destino, REGEX.UBICACION, "ubicación destino");
    if (!destino.ok) return { exito: false, mensaje: destino.mensaje };

    const responsable = validarTexto_(payload.responsable, null, "responsable");
    if (!responsable.ok) return { exito: false, mensaje: responsable.mensaje };

    const documento = normalizarTexto(payload.documento || "");
    const tipo = normalizarMayus(payload.tipo) || TIPO_MOVIMIENTO.INDIVIDUAL;
    const destinoUpper = destino.valor.toUpperCase();

    // Validar cada item y resolver contra el índice
    const filas = [];
    const errores = [];
    const ids = [];

    for (let i = 0; i < payload.items.length; i++) {
      const item = payload.items[i];
      const parteVal = validarTexto_(item.parte, REGEX.NUMERO_PARTE, "parte (item " + (i + 1) + ")");
      if (!parteVal.ok) { errores.push(parteVal.mensaje); continue; }

      const cantVal = validarCantidad_(item.cantidad, "cantidad (item " + (i + 1) + ")");
      if (!cantVal.ok) { errores.push(cantVal.mensaje); continue; }

      const articulo = obtenerArticuloPorIdentificador_(parteVal.valor);
      if (!articulo) {
        errores.push("Item " + (i + 1) + " (" + parteVal.valor + ") no existe en DATA_SAP");
        continue;
      }

      const idMov = generarID();
      ids.push(idMov);
      filas.push([
        idMov,
        new Date(),
        tipo,
        documento,
        articulo.parte || parteVal.valor,
        articulo.codigo || "",
        articulo.descripcion || "",
        cantVal.valor,
        articulo.ubicacion || "",
        destinoUpper,
        responsable.valor,
        ESTADO_MOVIMIENTO.UBICADO,
        false,
        "",
        "",
        ""
      ]);
    }

    if (filas.length === 0) {
      return { exito: false, mensaje: "Ningún item válido. " + errores.slice(0, 3).join("; ") };
    }

    // Escritura atómica con lock
    const lock = LockService.getDocumentLock();
    try {
      lock.waitLock(30000);
      const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.BASE_OPERATIVA);
      if (!sheet) return { exito: false, mensaje: "No existe la hoja BASE_OPERATIVA" };
      const filaInicial = sheet.getLastRow() + 1;
      sheet.getRange(filaInicial, 1, filas.length, BASE_OPERATIVA_WIDTH).setValues(filas);
      cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
    } finally {
      try { lock.releaseLock(); } catch (e) {}
    }

    return {
      exito: true,
      registrados: filas.length,
      saltados: errores.length,
      errores: errores.slice(0, 5),
      ids: ids,
      registradoPor: sesion.usuario
    };
  });
}
