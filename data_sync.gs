/**
 * UBYGUARD - Sincronización masiva de DATA_SAP desde la API SAP.
 *
 * Lee /api/ubyguard/items paginando hasta agotar (hasMore=false) y reescribe
 * la hoja DATA_SAP en bloque. Después invalida el índice O(1) para que la
 * próxima búsqueda lo reconstruya con la data fresca.
 *
 * El endpoint debe estar publicado por Andre. Mientras no exista, este sync
 * devuelve { pendienteAndre: true, mensaje } sin tocar la hoja.
 *
 * Modos:
 *   - Full: trae todo y reescribe la hoja (excepto header) — usado por trigger nocturno o "Refrescar todo"
 *   - Delta: usa modifiedSince para traer sólo lo cambiado y hace upsert por itemCode
 *
 * Spec del endpoint: docs/solicitudes-api-andre.pdf (sección 9)
 */

const DATA_SYNC_BATCH = 500;
const DATA_SYNC_MAX_LOOPS = 50; // hard stop ~25.000 ítems

/**
 * Sync completo: reescribe DATA_SAP de cero. Usar para refresh nocturno
 * o cuando se sospecha que la hoja quedó inconsistente.
 *
 * @param {Object} opts { warehouse?, activeOnly? }
 */
function sincronizarDataSapDesdeSap(opts) {
  return _runDataSync_(opts || {}, /* deltaMode */ false);
}

/**
 * Sync delta: trae sólo ítems modificados desde lastSyncTs y hace upsert.
 * Mucho más liviano para corridas frecuentes.
 *
 * @param {Object} opts { modifiedSince? (default: lastSyncTs), warehouse? }
 */
function sincronizarDataSapDelta(opts) {
  const o = opts || {};
  if (!o.modifiedSince) {
    const last = PropertiesService.getScriptProperties().getProperty("UBY_DATA_SAP_LAST_SYNC");
    if (last) o.modifiedSince = last;
  }
  return _runDataSync_(o, /* deltaMode */ true);
}

function _runDataSync_(opts, deltaMode) {
  try {
    const t0 = Date.now();
    const warehouse = opts.warehouse || SAP_WAREHOUSE;
    const activeOnly = opts.activeOnly !== false; // default true

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName(HOJAS.DATA_SAP);
    if (!sheet) return { exito: false, mensaje: "Hoja DATA_SAP no existe" };

    // Pagina hasta que el endpoint diga hasMore=false
    const traidos = [];
    let offset = 0;
    let loops = 0;
    let totalRemoto = null;

    while (loops < DATA_SYNC_MAX_LOOPS) {
      const filtros = { warehouse: warehouse, limit: DATA_SYNC_BATCH, offset: offset, activeOnly: activeOnly };
      if (deltaMode && opts.modifiedSince) filtros.modifiedSince = opts.modifiedSince;

      const resp = sapListarItems_(filtros);
      const pagina = (resp && resp.data) || [];
      if (totalRemoto == null && resp && resp.meta) totalRemoto = resp.meta.total;

      for (let i = 0; i < pagina.length; i++) traidos.push(pagina[i]);

      const hasMore = resp && resp.meta && resp.meta.hasMore === true;
      if (!hasMore || pagina.length === 0) break;

      offset += pagina.length;
      loops++;
    }

    if (traidos.length === 0) {
      return {
        exito: true,
        modo: deltaMode ? "delta" : "full",
        actualizados: 0,
        creados: 0,
        ms: Date.now() - t0,
        mensaje: deltaMode ? "No hay ítems modificados" : "Endpoint devolvió 0 ítems"
      };
    }

    // Aplica al sheet
    const lock = LockService.getDocumentLock();
    let creados = 0, actualizados = 0;
    try {
      lock.waitLock(60000);
      if (deltaMode) {
        const r = _upsertDataSap_(sheet, traidos);
        creados = r.creados; actualizados = r.actualizados;
      } else {
        _rewriteDataSap_(sheet, traidos);
        creados = traidos.length;
      }
      // Invalida el índice para que la próxima búsqueda lo reconstruya
      cacheInvalidarSimple_(CACHE_KEYS.SAP_INDEX);
      PropertiesService.getScriptProperties()
        .setProperty("UBY_DATA_SAP_LAST_SYNC", new Date().toISOString());
    } finally {
      try { lock.releaseLock(); } catch (e) {}
    }

    const ms = Date.now() - t0;
    console.log("[DATA_SAP sync " + (deltaMode ? "delta" : "full") + "] " + ms +
                "ms · creados=" + creados + " actualizados=" + actualizados +
                " traidos=" + traidos.length + " (remoto=" + totalRemoto + ")");

    return {
      exito: true,
      modo: deltaMode ? "delta" : "full",
      traidos: traidos.length,
      creados: creados,
      actualizados: actualizados,
      totalRemoto: totalRemoto,
      ms: ms
    };
  } catch (e) {
    const msg = String(e && e.message ? e.message : e);
    if (/HTTP 404|\/items|endpoint/i.test(msg)) {
      return {
        exito: false,
        pendienteAndre: true,
        mensaje: "Endpoint /api/ubyguard/items no responde. Confirmar con Andre — ver docs/solicitudes-api-andre.pdf (Parte B)"
      };
    }
    console.error("[DATA_SAP sync] Error:", msg);
    return { exito: false, mensaje: msg };
  }
}

/**
 * Reescribe la hoja DATA_SAP de cero (mantiene header en fila 1).
 * Mapea el shape del endpoint a las columnas existentes (ver SAP_COL en constantes).
 */
function _rewriteDataSap_(sheet, items) {
  const filas = items.map(_itemAFila_);
  if (sheet.getLastRow() > 1) {
    sheet.getRange(2, 1, sheet.getLastRow() - 1, Math.max(sheet.getLastColumn(), 8)).clearContent();
  }
  if (filas.length === 0) return;
  sheet.getRange(2, 1, filas.length, filas[0].length).setValues(filas);
}

/**
 * Upsert por itemCode (col CODIGO = índice 2 en 0-based).
 * Existente → actualiza fila. Nuevo → append.
 */
function _upsertDataSap_(sheet, items) {
  let creados = 0, actualizados = 0;
  const lastRow = sheet.getLastRow();

  // Mapea itemCode → fila para upsert rápido
  const indice = {};
  if (lastRow > 1) {
    const codigos = sheet.getRange(2, SAP_COL.CODIGO + 1, lastRow - 1, 1).getValues();
    for (let i = 0; i < codigos.length; i++) {
      const c = String(codigos[i][0] || "").trim().toUpperCase();
      if (c) indice[c] = i + 2;
    }
  }

  const nuevos = [];
  for (let i = 0; i < items.length; i++) {
    const it = items[i];
    const code = String(it.itemCode || "").trim().toUpperCase();
    if (!code) continue;
    const fila = _itemAFila_(it);
    if (indice[code]) {
      sheet.getRange(indice[code], 1, 1, fila.length).setValues([fila]);
      actualizados++;
    } else {
      nuevos.push(fila);
    }
  }
  if (nuevos.length > 0) {
    sheet.getRange(sheet.getLastRow() + 1, 1, nuevos.length, nuevos[0].length).setValues(nuevos);
    creados = nuevos.length;
  }
  return { creados: creados, actualizados: actualizados };
}

/**
 * Convierte un item del endpoint a la fila de DATA_SAP.
 * Estructura esperada según constantes.SAP_COL:
 *   0 ID, 1 NUMERO_PARTE, 2 CODIGO, 3 DESCRIPCION, 4 EXISTENCIA, 5, 6, 7 UBICACION
 */
function _itemAFila_(it) {
  const fila = new Array(8).fill("");
  fila[SAP_COL.ID] = it.docEntry || it.itemCode || "";
  fila[SAP_COL.NUMERO_PARTE] = it.partNumber || "";
  fila[SAP_COL.CODIGO] = it.itemCode || "";
  fila[SAP_COL.DESCRIPCION] = it.itemName || "";
  fila[SAP_COL.EXISTENCIA] = Number(it.onHand != null ? it.onHand : (it.available || 0));
  fila[SAP_COL.UBICACION] = it.binCode || "";
  return fila;
}

// ============ Endpoints públicos (frontend) ============

/**
 * Llamada desde el frontend (botón "Refrescar DATA_SAP desde SAP" del agente).
 * Solo AGENTE.
 */
function refrescarDataSapDesdeApi(token) {
  return conSesion_(token, ROLES.AGENTE, function() {
    return sincronizarDataSapDesdeSap({});
  });
}

/**
 * Trigger nocturno: instalar UNA VEZ desde el editor. Corre delta diario.
 * Run → instalarTriggerSyncDataSap
 */
function instalarTriggerSyncDataSap() {
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === "sincronizarDataSapDelta") {
      ScriptApp.deleteTrigger(t);
    }
  });
  ScriptApp.newTrigger("sincronizarDataSapDelta").timeBased().atHour(3).everyDays(1).create();
  console.log("✅ Trigger nocturno instalado (delta sync a las 3am)");
  return "Trigger instalado: DATA_SAP delta sync diario a las 3am";
}
