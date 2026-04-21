/**
 * UBYGUARD - Ingreso masivo.
 * Las constantes TRABAJO_MASIVO_HEADERS/COLS viven en constantes.gs.
 * Este archivo usa obtenerIndiceSap_() para evitar releer DATA_SAP.
 */

function prepararTrabajoMasivo(documento) {
  try {
    const doc = validarTexto_(documento, REGEX.DOCUMENTO, "documento");
    if (!doc.ok) return { exito: false, mensaje: doc.mensaje };
    const documentoNormalizado = doc.valor;

    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const dataMasivoSheet = spreadsheet.getSheetByName(HOJAS.DATA_MASIVO);

    if (!dataMasivoSheet || dataMasivoSheet.getLastRow() < 2) {
      return { exito: false, mensaje: "No hay datos cargados en DATA_MASIVO" };
    }

    const trabajoSheet = asegurarHojaTrabajoMasivo_();
    const dataMasivo = dataMasivoSheet.getDataRange().getValues();
    const idxSap = obtenerIndiceSap_(false);
    const existentes = construirIndiceTrabajoMasivoPorDocumento_(trabajoSheet, documentoNormalizado);

    const filas = [];
    let yaExistian = 0;
    let sinSap = 0;

    for (let i = 1; i < dataMasivo.length; i++) {
      const numeroArticulo = normalizarTexto(dataMasivo[i][0]);
      const descripcion = normalizarTexto(dataMasivo[i][1]);
      const numeroParte = normalizarTexto(dataMasivo[i][2]);
      const cantidadTotal = Number(dataMasivo[i][3] || 0);
      const almacen = normalizarTexto(dataMasivo[i][4]);

      if (!numeroParte || cantidadTotal <= 0) continue;

      const linea = {
        documento: documentoNormalizado,
        numeroArticulo: numeroArticulo,
        descripcion: descripcion,
        numeroParte: numeroParte,
        cantidadTotal: cantidadTotal,
        almacen: almacen
      };

      const claveLinea = construirClaveTrabajoMasivo_(linea);
      if (existentes[claveLinea]) { yaExistian++; continue; }

      const sapItem = idxSap.porParte[numeroParte.toUpperCase()] || null;
      if (!sapItem) sinSap++;

      filas.push([
        documentoNormalizado,
        numeroArticulo || (sapItem ? sapItem[1] : ""),
        descripcion || (sapItem ? sapItem[2] : ""),
        numeroParte,
        cantidadTotal,
        0,
        cantidadTotal,
        almacen,
        sapItem ? sapItem[4] : "",
        "",
        ESTADOS_LINEA.PENDIENTE,
        false,
        "",
        "",
        "",
        true,
        false
      ]);

      existentes[claveLinea] = true;
    }

    if (filas.length > 0) {
      const filaInicial = obtenerSiguienteFilaTrabajoMasivo_(trabajoSheet);
      trabajoSheet
        .getRange(filaInicial, 1, filas.length, TRABAJO_MASIVO_HEADERS.length)
        .setValues(filas);
    }

    cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);

    return {
      exito: true,
      mensaje: filas.length > 0 ? "Trabajo preparado correctamente" : "El documento ya existe en TRABAJO_MASIVO",
      creados: filas.length,
      existentes: yaExistian,
      sinSap: sinSap
    };
  } catch (e) {
    return { exito: false, mensaje: "Error interno: " + (e && e.message ? e.message : e) };
  }
}

function obtenerTrabajoMasivo(documento, estado) {
  try {
    const trabajoSheet = asegurarHojaTrabajoMasivo_();
    if (trabajoSheet.getLastRow() < 2) return [];

    const documentoFiltro = normalizarTexto(documento);
    const estadoFiltro = normalizarMayus(estado);
    const timeZone = Session.getScriptTimeZone();
    const data = trabajoSheet
      .getRange(2, 1, trabajoSheet.getLastRow() - 1, TRABAJO_MASIVO_HEADERS.length)
      .getValues();

    return data
      .map((row, index) => mapearFilaTrabajoMasivo_(row, index + 2, timeZone))
      .filter(item => {
        const coincideDocumento = !documentoFiltro || item.documento === documentoFiltro;
        const coincideEstado = !estadoFiltro
          || (estadoFiltro === "ACTIVOS" && item.estado !== ESTADOS_LINEA.COMPLETO && item.bloqueado !== true)
          || item.estado === estadoFiltro;
        return coincideDocumento && coincideEstado;
      })
      .sort(ordenarTrabajoMasivo_);
  } catch (e) {
    return [];
  }
}

function guardarEdicionTrabajoMasivo(rowNumber, observacion) {
  try {
    const trabajoSheet = asegurarHojaTrabajoMasivo_();
    const row = obtenerFilaTrabajoMasivoPorNumero_(trabajoSheet, rowNumber);

    if (!row) return { exito: false, mensaje: "No se encontró la línea de trabajo" };
    if (row.bloqueado || row.estado === ESTADOS_LINEA.COMPLETO || row.editable !== true) {
      return { exito: false, mensaje: "La línea ya no permite edición" };
    }

    const obs = validarTexto_(observacion || " ", REGEX.TEXTO_LIBRE, "observación");
    trabajoSheet.getRange(rowNumber, TRABAJO_MASIVO_COLS.OBSERVACION)
      .setValue(normalizarTexto(observacion));

    return { exito: true, mensaje: "Observación actualizada" };
  } catch (e) {
    return { exito: false, mensaje: "Error interno: " + (e && e.message ? e.message : e) };
  }
}

function registrarTrabajoMasivo(rowNumber, cantidadRegistrar, ubicacionFinal, responsable, observacion) {
  try {
    const trabajoSheet = asegurarHojaTrabajoMasivo_();
    const row = obtenerFilaTrabajoMasivoPorNumero_(trabajoSheet, rowNumber);

    if (!row) return { exito: false, mensaje: "No se encontró la línea de trabajo" };
    if (row.bloqueado || row.estado === ESTADOS_LINEA.COMPLETO || row.editable !== true) {
      return { exito: false, mensaje: "La línea ya está completada o bloqueada" };
    }

    const respVal = validarTexto_(responsable, null, "responsable");
    if (!respVal.ok) return { exito: false, mensaje: respVal.mensaje };

    const ubicVal = validarTexto_(ubicacionFinal, REGEX.UBICACION, "ubicación destino");
    if (!ubicVal.ok) return { exito: false, mensaje: ubicVal.mensaje };

    const cantVal = validarCantidad_(cantidadRegistrar, "cantidad");
    if (!cantVal.ok) return { exito: false, mensaje: cantVal.mensaje };

    if (cantVal.valor > row.cantidadPendiente) {
      return { exito: false, mensaje: "La cantidad excede la pendiente (" + row.cantidadPendiente + ")" };
    }

    const baseOperativaSheet = SpreadsheetApp
      .getActiveSpreadsheet()
      .getSheetByName(HOJAS.BASE_OPERATIVA);

    if (!baseOperativaSheet) return { exito: false, mensaje: "No existe la hoja BASE_OPERATIVA" };

    const ubicacionFinalNormalizada = ubicVal.valor.toUpperCase();

    // "Ejecutado Por" (col O) queda vacío — se llena desde el módulo de Ejecución SAP
    const filaOperativa = [[
      generarID(),
      new Date(),
      TIPO_MOVIMIENTO.INGRESO_MASIVO,
      row.documento,
      row.numeroParte,
      row.numeroArticulo,
      row.descripcionArticulo,
      cantVal.valor,
      row.ubicacionOrigen,
      ubicacionFinalNormalizada,
      respVal.valor,
      ESTADO_MOVIMIENTO.UBICADO,
      false,
      "",
      "",
      ""
    ]];

    baseOperativaSheet
      .getRange(baseOperativaSheet.getLastRow() + 1, 1, 1, filaOperativa[0].length)
      .setValues(filaOperativa);

    const nuevaCantidadRegistrada = row.cantidadRegistrada + cantVal.valor;
    const nuevaCantidadPendiente = Math.max(row.cantidadTotal - nuevaCantidadRegistrada, 0);
    const estado = calcularEstadoTrabajoMasivo_(nuevaCantidadRegistrada, row.cantidadTotal);
    const bloqueado = estado === ESTADOS_LINEA.COMPLETO;
    const editable = !bloqueado;

    trabajoSheet.getRange(rowNumber, TRABAJO_MASIVO_COLS.CANTIDAD_REGISTRADA).setValue(nuevaCantidadRegistrada);
    trabajoSheet.getRange(rowNumber, TRABAJO_MASIVO_COLS.CANTIDAD_PENDIENTE).setValue(nuevaCantidadPendiente);
    trabajoSheet.getRange(rowNumber, TRABAJO_MASIVO_COLS.ULTIMA_UBICACION_DESTINO).setValue(ubicacionFinalNormalizada);
    trabajoSheet.getRange(rowNumber, TRABAJO_MASIVO_COLS.ESTADO).setValue(estado);
    trabajoSheet.getRange(rowNumber, TRABAJO_MASIVO_COLS.BLOQUEADO).setValue(bloqueado);
    trabajoSheet.getRange(rowNumber, TRABAJO_MASIVO_COLS.RESPONSABLE_ULTIMO_MOVIMIENTO).setValue(respVal.valor);
    trabajoSheet.getRange(rowNumber, TRABAJO_MASIVO_COLS.FECHA_ULTIMO_MOVIMIENTO).setValue(new Date());
    trabajoSheet.getRange(rowNumber, TRABAJO_MASIVO_COLS.OBSERVACION).setValue(normalizarTexto(observacion));
    trabajoSheet.getRange(rowNumber, TRABAJO_MASIVO_COLS.EDITABLE).setValue(editable);

    cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);

    return {
      exito: true,
      mensaje: estado === ESTADOS_LINEA.COMPLETO ? "Línea completada" : "Movimiento registrado",
      estado: estado
    };
  } catch (e) {
    return { exito: false, mensaje: "Error interno: " + (e && e.message ? e.message : e) };
  }
}

/**
 * Lista todos los documentos que tienen al menos una línea en TRABAJO_MASIVO.
 * Incluye conteo por estado para poder mostrar tarjetas en la UI y evitar
 * que el usuario cree documentos duplicados.
 */
function obtenerDocumentosActivos() {
  try {
    const sheet = asegurarHojaTrabajoMasivo_();
    if (sheet.getLastRow() < 2) return [];
    const data = sheet
      .getRange(2, 1, sheet.getLastRow() - 1, TRABAJO_MASIVO_HEADERS.length)
      .getValues();

    const docs = {};
    for (let i = 0; i < data.length; i++) {
      const doc = normalizarTexto(data[i][TRABAJO_MASIVO_COLS.DOCUMENTO - 1]);
      if (!doc) continue;
      const estado = (normalizarTexto(data[i][TRABAJO_MASIVO_COLS.ESTADO - 1]) || ESTADOS_LINEA.PENDIENTE).toUpperCase();
      const bloqueado = data[i][TRABAJO_MASIVO_COLS.BLOQUEADO - 1] === true;

      if (!docs[doc]) {
        docs[doc] = {
          documento: doc,
          total: 0,
          pendientes: 0,
          parciales: 0,
          completos: 0,
          activas: 0
        };
      }
      docs[doc].total++;
      if (estado === ESTADOS_LINEA.PENDIENTE) docs[doc].pendientes++;
      else if (estado === ESTADOS_LINEA.PARCIAL) docs[doc].parciales++;
      else if (estado === ESTADOS_LINEA.COMPLETO) docs[doc].completos++;
      if (!bloqueado && estado !== ESTADOS_LINEA.COMPLETO) docs[doc].activas++;
    }

    return Object.keys(docs).map(k => docs[k]).sort((a, b) => {
      if (a.activas !== b.activas) return b.activas - a.activas;
      return a.documento.localeCompare(b.documento);
    });
  } catch (e) {
    return [];
  }
}

function obtenerResumenMasivo(documento) {
  const items = obtenerTrabajoMasivo(documento, "");
  let pendientes = 0, parciales = 0, completos = 0, errores = 0;
  items.forEach(item => {
    if (item.estado === ESTADOS_LINEA.PENDIENTE) pendientes++;
    if (item.estado === ESTADOS_LINEA.PARCIAL) parciales++;
    if (item.estado === ESTADOS_LINEA.COMPLETO) completos++;
    if (item.estado === ESTADOS_LINEA.ERROR) errores++;
  });
  return {
    total: items.length,
    pendientes: pendientes,
    parciales: parciales,
    completos: completos,
    errores: errores
  };
}

function asegurarHojaTrabajoMasivo_() {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = spreadsheet.getSheetByName(HOJAS.TRABAJO_MASIVO);
  if (!sheet) sheet = spreadsheet.insertSheet(HOJAS.TRABAJO_MASIVO);

  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, TRABAJO_MASIVO_HEADERS.length).setValues([TRABAJO_MASIVO_HEADERS]);
    sheet.setFrozenRows(1);
    return sheet;
  }

  const headers = sheet.getRange(1, 1, 1, TRABAJO_MASIVO_HEADERS.length).getValues()[0];
  if (headers.join("|") !== TRABAJO_MASIVO_HEADERS.join("|")) {
    sheet.getRange(1, 1, 1, TRABAJO_MASIVO_HEADERS.length).setValues([TRABAJO_MASIVO_HEADERS]);
    sheet.setFrozenRows(1);
  }

  return sheet;
}

function construirIndiceTrabajoMasivoPorDocumento_(sheet, documento) {
  const index = {};
  if (sheet.getMaxRows() < 2) return index;
  const totalRows = Math.max(sheet.getMaxRows() - 1, 1);
  const data = sheet.getRange(2, 1, totalRows, TRABAJO_MASIVO_HEADERS.length).getValues();

  for (let i = 0; i < data.length; i++) {
    const documentoActual = normalizarTexto(data[i][TRABAJO_MASIVO_COLS.DOCUMENTO - 1]);
    const numeroParte = normalizarTexto(data[i][TRABAJO_MASIVO_COLS.NUMERO_PARTE - 1]);
    if (!documentoActual || !numeroParte) continue;
    if (documentoActual === documento) {
      index[construirClaveTrabajoMasivo_({
        documento: documentoActual,
        numeroArticulo: data[i][TRABAJO_MASIVO_COLS.NUMERO_ARTICULO - 1],
        descripcion: data[i][TRABAJO_MASIVO_COLS.DESCRIPCION_ARTICULO - 1],
        numeroParte: numeroParte,
        cantidadTotal: data[i][TRABAJO_MASIVO_COLS.CANTIDAD_TOTAL - 1],
        almacen: data[i][TRABAJO_MASIVO_COLS.ALMACEN - 1]
      })] = true;
    }
  }
  return index;
}

function obtenerFilaTrabajoMasivoPorNumero_(sheet, rowNumber) {
  if (!rowNumber || rowNumber < 2 || rowNumber > sheet.getLastRow()) return null;
  const row = sheet.getRange(rowNumber, 1, 1, TRABAJO_MASIVO_HEADERS.length).getValues()[0];
  if (!row || !row[TRABAJO_MASIVO_COLS.NUMERO_PARTE - 1]) return null;
  return mapearFilaTrabajoMasivo_(row, rowNumber, Session.getScriptTimeZone());
}

function mapearFilaTrabajoMasivo_(row, rowNumber, timeZone) {
  return {
    rowNumber: rowNumber,
    documento: normalizarTexto(row[TRABAJO_MASIVO_COLS.DOCUMENTO - 1]),
    numeroArticulo: normalizarTexto(row[TRABAJO_MASIVO_COLS.NUMERO_ARTICULO - 1]),
    descripcionArticulo: normalizarTexto(row[TRABAJO_MASIVO_COLS.DESCRIPCION_ARTICULO - 1]),
    numeroParte: normalizarTexto(row[TRABAJO_MASIVO_COLS.NUMERO_PARTE - 1]),
    cantidadTotal: Number(row[TRABAJO_MASIVO_COLS.CANTIDAD_TOTAL - 1] || 0),
    cantidadRegistrada: Number(row[TRABAJO_MASIVO_COLS.CANTIDAD_REGISTRADA - 1] || 0),
    cantidadPendiente: Number(row[TRABAJO_MASIVO_COLS.CANTIDAD_PENDIENTE - 1] || 0),
    almacen: normalizarTexto(row[TRABAJO_MASIVO_COLS.ALMACEN - 1]),
    ubicacionOrigen: normalizarTexto(row[TRABAJO_MASIVO_COLS.UBICACION_ORIGEN - 1]),
    ultimaUbicacionDestino: normalizarTexto(row[TRABAJO_MASIVO_COLS.ULTIMA_UBICACION_DESTINO - 1]),
    estado: normalizarTexto(row[TRABAJO_MASIVO_COLS.ESTADO - 1]) || ESTADOS_LINEA.PENDIENTE,
    bloqueado: row[TRABAJO_MASIVO_COLS.BLOQUEADO - 1] === true,
    responsableUltimoMovimiento: normalizarTexto(row[TRABAJO_MASIVO_COLS.RESPONSABLE_ULTIMO_MOVIMIENTO - 1]),
    fechaUltimoMovimiento: formatearFechaMasivo_(row[TRABAJO_MASIVO_COLS.FECHA_ULTIMO_MOVIMIENTO - 1], timeZone),
    observacion: normalizarTexto(row[TRABAJO_MASIVO_COLS.OBSERVACION - 1]),
    editable: row[TRABAJO_MASIVO_COLS.EDITABLE - 1] !== false,
    errorCorregido: row[TRABAJO_MASIVO_COLS.ERROR_CORREGIDO - 1] === true
  };
}

function calcularEstadoTrabajoMasivo_(cantidadRegistrada, cantidadTotal) {
  if (cantidadRegistrada <= 0) return ESTADOS_LINEA.PENDIENTE;
  if (cantidadRegistrada >= cantidadTotal) return ESTADOS_LINEA.COMPLETO;
  return ESTADOS_LINEA.PARCIAL;
}

function ordenarTrabajoMasivo_(a, b) {
  const orden = { PENDIENTE: 1, PARCIAL: 2, ERROR: 3, COMPLETO: 4, BLOQUEADO: 5 };
  const ordenA = orden[a.estado] || 99;
  const ordenB = orden[b.estado] || 99;
  if (ordenA !== ordenB) return ordenA - ordenB;
  if (a.documento !== b.documento) return a.documento.localeCompare(b.documento);
  return a.numeroParte.localeCompare(b.numeroParte);
}

function formatearFechaMasivo_(valor, timeZone) {
  if (Object.prototype.toString.call(valor) === "[object Date]" && !isNaN(valor)) {
    return Utilities.formatDate(valor, timeZone, "dd/MM/yyyy HH:mm:ss");
  }
  return normalizarTexto(valor);
}

function construirClaveTrabajoMasivo_(item) {
  return [
    normalizarTexto(item.documento),
    normalizarTexto(item.numeroArticulo),
    normalizarTexto(item.descripcion),
    normalizarTexto(item.numeroParte),
    Number(item.cantidadTotal || 0),
    normalizarTexto(item.almacen)
  ].join("|");
}

function obtenerSiguienteFilaTrabajoMasivo_(sheet) {
  if (sheet.getMaxRows() < 2) return 2;
  const totalRows = Math.max(sheet.getMaxRows() - 1, 1);
  const data = sheet.getRange(2, 1, totalRows, TRABAJO_MASIVO_HEADERS.length).getValues();
  for (let i = data.length - 1; i >= 0; i--) {
    const tieneContenido = data[i].some(celda => normalizarTexto(celda) !== "");
    if (tieneContenido) return i + 3;
  }
  return 2;
}

// Alias legacy para no romper llamadas previas
function normalizarTextoMasivo(valor) {
  return normalizarTexto(valor);
}
