/**
 * UBYGUARD - Historial y dashboard. Requiere sesión (AUXILIAR+).
 */

function obtenerHistorialMovimientos(token, limite) {
  return conSesion_(token, ROLES.AUXILIAR, function() {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.BASE_OPERATIVA);
    if (!sheet) return { exito: true, registros: [] };
    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return { exito: true, registros: [] };

    const totalRegistros = lastRow - 1;
    const pedido = Number(limite) || LIMITES.HISTORIAL_DEFAULT;
    const cantidad = Math.min(Math.max(pedido, 1), Math.min(LIMITES.HISTORIAL_MAX, totalRegistros));
    const filaInicial = lastRow - cantidad + 1;
    const data = sheet
      .getRange(filaInicial, 1, cantidad, BASE_OPERATIVA_WIDTH)
      .getValues()
      .reverse();
    const timeZone = Session.getScriptTimeZone();

    const registros = data
      .filter(tieneDatosHistorial)
      .map(row => mapearMovimientoHistorial(row, timeZone));
    return { exito: true, registros: registros };
  });
}

function obtenerResumenInicio(token) {
  return conSesion_(token, ROLES.AUXILIAR, function() {
    const cached = cacheObtenerSimple_(CACHE_KEYS.RESUMEN_INICIO);
    if (cached) return Object.assign({ exito: true }, cached);

    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const baseOperativa = spreadsheet.getSheetByName(HOJAS.BASE_OPERATIVA);
    const trabajoMasivo = spreadsheet.getSheetByName(HOJAS.TRABAJO_MASIVO);

    let movimientosHoy = 0;
    let pendientesSap = 0;
    let movimientos = [];
    const tendencia7d = new Array(7).fill(0);
    const timeZone = Session.getScriptTimeZone();
    const hoy = Utilities.formatDate(new Date(), timeZone, "yyyyMMdd");
    const ahora = new Date();

    if (baseOperativa) {
      const lastRow = baseOperativa.getLastRow();
      if (lastRow > 1) {
        const ventana = Math.min(lastRow - 1, 3000);
        const filaInicial = lastRow - ventana + 1;
        const data = baseOperativa
          .getRange(filaInicial, 1, ventana, BASE_OPERATIVA_WIDTH)
          .getValues();

        for (let i = 0; i < data.length; i++) {
          const row = data[i];
          if (!tieneDatosHistorial(row)) continue;
          if (esFechaDeHoy(row[1], hoy, timeZone)) movimientosHoy++;
          // Pendientes SAP excluye tipo PEDIDO (Picking): esos no van a la cola
          if (row[12] !== true && String(row[2] || "").toUpperCase() !== "PEDIDO") pendientesSap++;
          const diasAtras = diferenciaDias_(row[1], ahora);
          if (diasAtras != null && diasAtras >= 0 && diasAtras < 7) {
            tendencia7d[6 - diasAtras]++;
          }
          movimientos.push(mapearMovimientoHistorial(row, timeZone));
        }
      }
    }

    const resumenTrabajoMasivo = obtenerResumenTrabajoMasivoInicio_(trabajoMasivo);
    const ultimosMovimientos = movimientos.slice(-5).reverse();

    const resumen = {
      movimientosHoy: movimientosHoy,
      pendientesSap: pendientesSap,
      lineasActivasMasivo: resumenTrabajoMasivo.lineasActivas,
      documentosActivosMasivo: resumenTrabajoMasivo.documentosActivos,
      ultimoMovimiento: ultimosMovimientos[0] || null,
      ultimosMovimientos: ultimosMovimientos,
      tendencia7d: tendencia7d,
      totalSap: (obtenerEstadoIndice() || {}).total || 0,
      generadoEn: Date.now()
    };

    cachePonerSimple_(CACHE_KEYS.RESUMEN_INICIO, resumen, CACHE_TTL.RESUMEN_INICIO);
    return Object.assign({ exito: true }, resumen);
  });
}

function obtenerResumenTrabajoMasivoInicio_(sheet) {
  if (!sheet || sheet.getLastRow() < 2) return { lineasActivas: 0, documentosActivos: 0 };
  const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, 12).getValues();
  const documentosActivos = {};
  let lineasActivas = 0;
  for (let i = 0; i < data.length; i++) {
    const documento = String(data[i][0] || "").trim();
    const numeroArticulo = String(data[i][1] || "").trim();
    const numeroParte = String(data[i][3] || "").trim();
    const estado = String(data[i][10] || ESTADOS_LINEA.PENDIENTE).trim().toUpperCase();
    const bloqueado = data[i][11] === true;
    if (!documento && !numeroArticulo && !numeroParte) continue;
    if (!bloqueado && estado !== ESTADOS_LINEA.COMPLETO) {
      lineasActivas++;
      if (documento) documentosActivos[documento] = true;
    }
  }
  return { lineasActivas: lineasActivas, documentosActivos: Object.keys(documentosActivos).length };
}

function formatearFechaHistorial(valor, timeZone) {
  if (Object.prototype.toString.call(valor) === "[object Date]" && !isNaN(valor)) {
    return Utilities.formatDate(valor, timeZone, "dd/MM/yyyy HH:mm:ss");
  }
  return valor || "";
}

function mapearMovimientoHistorial(row, timeZone) {
  return {
    id: row[0] || "",
    fecha: formatearFechaHistorial(row[1], timeZone),
    tipo: row[2] || "",
    documento: row[3] || "",
    parte: row[4] || "",
    codigo: row[5] || "",
    descripcion: row[6] || "",
    cantidad: row[7] || 0,
    ubicacionOrigen: row[8] || "",
    ubicacionFinal: row[9] || "",
    responsable: row[10] || "",
    estado: row[11] || "",
    sap: row[12] === true ? "SI" : row[12] === false ? "NO" : (row[12] || ""),
    fechaEjecucion: formatearFechaHistorial(row[13], timeZone),
    ejecutadoPor: row[14] || "",
    movimientoSap: row[15] || ""
  };
}

function tieneDatosHistorial(row) {
  return row.some(cell => cell !== "");
}

function esFechaDeHoy(valor, hoy, timeZone) {
  if (Object.prototype.toString.call(valor) !== "[object Date]" || isNaN(valor)) return false;
  return Utilities.formatDate(valor, timeZone, "yyyyMMdd") === hoy;
}

function diferenciaDias_(valor, referencia) {
  if (Object.prototype.toString.call(valor) !== "[object Date]" || isNaN(valor)) return null;
  const msPorDia = 24 * 60 * 60 * 1000;
  const dRef = new Date(referencia.getFullYear(), referencia.getMonth(), referencia.getDate()).getTime();
  const dVal = new Date(valor.getFullYear(), valor.getMonth(), valor.getDate()).getTime();
  return Math.round((dRef - dVal) / msPorDia);
}
