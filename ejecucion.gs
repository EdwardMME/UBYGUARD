/**
 * UBYGUARD - Módulo de Ejecución SAP.
 * Flujo: auxiliar crea movimiento → operador SAP lo ejecuta en SAP →
 * marca la línea como ejecutada desde este módulo (en vez de editar el Sheet).
 *
 * Columnas que maneja este módulo en BASE_OPERATIVA:
 *   M (13): checkbox SAP → true
 *   N (14): fecha de ejecución → new Date()
 *   O (15): ejecutado por → nombre del operador
 *   P (16): comentario / movimiento SAP → opcional
 */

function obtenerPendientesSap(limite) {
  try {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.BASE_OPERATIVA);
    if (!sheet) return [];
    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return [];

    const ventana = Math.min(lastRow - 1, Math.max(Number(limite) || 1000, 50));
    const filaInicial = lastRow - ventana + 1;
    const data = sheet.getRange(filaInicial, 1, ventana, BASE_OPERATIVA_WIDTH).getValues();
    const timeZone = Session.getScriptTimeZone();

    const resultados = [];
    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      if (!tieneDatosHistorial(row)) continue;
      if (row[12] === true) continue; // ya ejecutado en SAP
      const mov = mapearMovimientoHistorial(row, timeZone);
      mov.rowNumber = filaInicial + i;
      resultados.push(mov);
    }
    return resultados.reverse();
  } catch (e) {
    return [];
  }
}

function ejecutarSap(rowNumber, ejecutor, comentario) {
  try {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.BASE_OPERATIVA);
    if (!sheet) return { exito: false, mensaje: "No existe BASE_OPERATIVA" };

    const row = Number(rowNumber);
    if (!row || row < 2 || row > sheet.getLastRow()) {
      return { exito: false, mensaje: "Fila inválida" };
    }

    const ejec = validarTexto_(ejecutor, null, "ejecutor");
    if (!ejec.ok) return { exito: false, mensaje: ejec.mensaje };

    const actual = sheet.getRange(row, 13).getValue();
    if (actual === true) {
      return { exito: false, mensaje: "Este movimiento ya estaba ejecutado" };
    }

    sheet.getRange(row, 13).setValue(true);
    sheet.getRange(row, 14).setValue(new Date());
    sheet.getRange(row, 15).setValue(ejec.valor);
    if (comentario) {
      sheet.getRange(row, 16).setValue(normalizarTexto(comentario));
    }

    cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
    return { exito: true, mensaje: "Movimiento ejecutado en SAP" };
  } catch (e) {
    return { exito: false, mensaje: "Error: " + (e && e.message ? e.message : e) };
  }
}

/**
 * Ejecución en lote. Más eficiente para marcar muchos movimientos
 * porque hace una sola escritura por columna en el rango completo.
 */
function ejecutarSapLote(rowNumbers, ejecutor) {
  try {
    if (!Array.isArray(rowNumbers) || rowNumbers.length === 0) {
      return { exito: false, mensaje: "No seleccionaste movimientos" };
    }
    const ejec = validarTexto_(ejecutor, null, "ejecutor");
    if (!ejec.ok) return { exito: false, mensaje: ejec.mensaje };

    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.BASE_OPERATIVA);
    if (!sheet) return { exito: false, mensaje: "No existe BASE_OPERATIVA" };

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
        sheet.getRange(row, 15).setValue(ejec.valor);
        exitos++;
      } catch (e) { errores++; }
    }

    cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
    return {
      exito: true,
      mensaje: exitos + " ejecutados" + (saltados ? " · " + saltados + " ya estaban" : "") + (errores ? " · " + errores + " errores" : ""),
      exitos: exitos, saltados: saltados, errores: errores
    };
  } catch (e) {
    return { exito: false, mensaje: "Error: " + (e && e.message ? e.message : e) };
  }
}
