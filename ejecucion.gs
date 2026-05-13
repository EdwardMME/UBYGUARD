/**
 * UBYGUARD - Módulo de Ejecución SAP. Solo AGENTE.
 * El "ejecutor" se toma del token (sesion.nombre resuelto en backend).
 */

function obtenerPendientesSap(token, limite) {
  return conSesion_(token, ROLES.AGENTE, function() {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.BASE_OPERATIVA);
    if (!sheet) return { exito: true, pendientes: [] };
    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return { exito: true, pendientes: [] };

    const ventana = Math.min(lastRow - 1, Math.max(Number(limite) || 1000, 50));
    const filaInicial = lastRow - ventana + 1;
    const data = sheet.getRange(filaInicial, 1, ventana, BASE_OPERATIVA_WIDTH).getValues();
    const timeZone = Session.getScriptTimeZone();

    const resultados = [];
    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      if (!tieneDatosHistorial(row)) continue;
      if (row[12] === true) continue;
      // Excluye los movimientos generados por Picking (tipo PEDIDO) — esos no
      // necesitan acción manual en SAP, la cotización se factura por otro lado
      if (String(row[2] || "").toUpperCase() === "PEDIDO") continue;
      const mov = mapearMovimientoHistorial(row, timeZone);
      mov.rowNumber = filaInicial + i;
      resultados.push(mov);
    }
    return { exito: true, pendientes: resultados.reverse() };
  });
}

function ejecutarSap(token, rowNumber, comentario) {
  return conSesion_(token, ROLES.AGENTE, function(sesion) {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.BASE_OPERATIVA);
    if (!sheet) return { exito: false, mensaje: "No existe BASE_OPERATIVA" };

    const row = Number(rowNumber);
    if (!row || row < 2 || row > sheet.getLastRow()) {
      return { exito: false, mensaje: "Fila inválida" };
    }

    const nombreEjecutor = obtenerNombreUsuario_(sesion.usuario);

    const actual = sheet.getRange(row, 13).getValue();
    if (actual === true) return { exito: false, mensaje: "Ya estaba ejecutado" };

    sheet.getRange(row, 13).setValue(true);
    sheet.getRange(row, 14).setValue(new Date());
    sheet.getRange(row, 15).setValue(nombreEjecutor);
    if (comentario) sheet.getRange(row, 16).setValue(normalizarTexto(comentario));

    cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
    return { exito: true, mensaje: "Ejecutado por " + nombreEjecutor };
  });
}

function ejecutarSapLote(token, rowNumbers) {
  return conSesion_(token, ROLES.AGENTE, function(sesion) {
    if (!Array.isArray(rowNumbers) || rowNumbers.length === 0) {
      return { exito: false, mensaje: "No seleccionaste movimientos" };
    }
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.BASE_OPERATIVA);
    if (!sheet) return { exito: false, mensaje: "No existe BASE_OPERATIVA" };

    const nombreEjecutor = obtenerNombreUsuario_(sesion.usuario);
    const ahora = new Date();
    let exitos = 0, saltados = 0, errores = 0;

    for (let i = 0; i < rowNumbers.length; i++) {
      const row = Number(rowNumbers[i]);
      if (!row || row < 2 || row > sheet.getLastRow()) { errores++; continue; }
      try {
        const actual = sheet.getRange(row, 13).getValue();
        if (actual === true) { saltados++; continue; }
        sheet.getRange(row, 13).setValue(true);
        sheet.getRange(row, 14).setValue(ahora);
        sheet.getRange(row, 15).setValue(nombreEjecutor);
        exitos++;
      } catch (e) { errores++; }
    }

    cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
    return {
      exito: true,
      mensaje: exitos + " ejecutados" + (saltados ? " · " + saltados + " ya estaban" : "") + (errores ? " · " + errores + " errores" : ""),
      exitos: exitos, saltados: saltados, errores: errores
    };
  });
}

function obtenerNombreUsuario_(usuario) {
  try {
    const sheet = asegurarHojaUsuarios_();
    const fila = buscarFilaUsuario_(sheet, usuario);
    if (fila) return fila.data[USUARIOS_COLS.NOMBRE - 1] || usuario;
  } catch (e) {}
  return usuario;
}
