/**
 * UBYGUARD - Módulo de Tickets de Preparación.
 *
 * Flujo:
 *   SAP /quotations (abiertas)  ──sync horario──> hoja TICKETS + TICKETS_LINEAS
 *           ↓
 *   AUXILIAR ve cuadritos disponibles → toma uno (contador empieza)
 *           ↓
 *   Marca cada item como recogido o faltante → auto-genera movimiento en BASE_OPERATIVA
 *           ↓
 *   Cuando todos los items están resueltos → LISTO
 *           ↓
 *   AUXILIAR marca ENTREGADO al retirar el cliente
 *
 * Decisiones del owner:
 *   - Todas las cotizaciones del día son para almacén (no hay gate del agente)
 *   - Cualquier auxiliar puede tomar cualquier pedido (sin asignación previa)
 *   - Sólo líneas con WarehouseCode = "SPS0002"
 *   - Al recoger item, auto-mueve en BASE_OPERATIVA → destino STAGING-DESPACHO
 */

const WAREHOUSE_SP = "SPS0002";
const STAGING_DESPACHO = "STAGING-DESPACHO";

// =====================================================================
// SINCRONIZACIÓN DESDE SAP
// =====================================================================

/**
 * Sincroniza cotizaciones abiertas desde SAP con la hoja TICKETS.
 * - Upsert: si el ticket ya existe, actualiza solo campos sincronizables.
 * - No toca tickets ya tomados (EN_PREP, LISTO, ENTREGADO) — sólo refresca datos del cliente/comentarios si cambiaron.
 * - Cierra tickets locales si la cotización ya no aparece como abierta en SAP (estado CANCELADO).
 *
 * Para ejecutar manual: Run → sincronizarTicketsDesdeSap
 * Para automático: instalar trigger horario con instalarTriggerSyncTickets.
 */
function sincronizarTicketsDesdeSap() {
  try {
    const t0 = Date.now();
    const resp = sapListarCotizaciones_({ status: "open", limit: 200 });
    const remoto = (resp && resp.data) || [];

    const ticketsSheet = asegurarHojaTickets_();
    const lineasSheet = asegurarHojaTicketsLineas_();
    const lock = LockService.getDocumentLock();
    let creados = 0, actualizados = 0, saltados = 0;

    try {
      lock.waitLock(30000);

      const indiceLocal = indiceTicketsExistentes_(ticketsSheet);
      const docNumsRemotos = {};

      for (let i = 0; i < remoto.length; i++) {
        const q = remoto[i];
        if (!q || !q.docNum) continue;

        // Filtra solo las líneas del almacén SP0002
        const lineasSp = (q.lines || []).filter(function(l) {
          return l && l.warehouseCode === WAREHOUSE_SP;
        });
        if (lineasSp.length === 0) { saltados++; continue; }

        const ticketId = String(q.docNum);
        docNumsRemotos[ticketId] = true;
        const existente = indiceLocal[ticketId];

        if (!existente) {
          // CREAR nuevo ticket
          crearTicket_(ticketsSheet, lineasSheet, ticketId, q, lineasSp);
          creados++;
        } else {
          // ACTUALIZAR sólo si está ABIERTO (no toca los que ya tomó un auxiliar)
          if (existente.estado === ESTADOS_TICKET.ABIERTO) {
            actualizarTicketAbierto_(ticketsSheet, lineasSheet, existente.row, ticketId, q, lineasSp);
            actualizados++;
          } else {
            // Sólo refresca fecha_sync para indicar que vino en el último pull
            ticketsSheet.getRange(existente.row, TICKETS_COLS.FECHA_SYNC).setValue(new Date());
          }
        }
      }

      // Detecta tickets locales ABIERTOS que ya no vienen de SAP → marcar CANCELADO
      let cancelados = 0;
      Object.keys(indiceLocal).forEach(function(tid) {
        if (!docNumsRemotos[tid] && indiceLocal[tid].estado === ESTADOS_TICKET.ABIERTO) {
          ticketsSheet.getRange(indiceLocal[tid].row, TICKETS_COLS.ESTADO).setValue(ESTADOS_TICKET.CANCELADO);
          cancelados++;
        }
      });

      cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);

      const ms = Date.now() - t0;
      console.log("[Tickets sync] " + ms + "ms · creados=" + creados + " actualizados=" + actualizados + " cancelados=" + cancelados + " saltados=" + saltados);
      return { exito: true, creados: creados, actualizados: actualizados, cancelados: cancelados, saltados: saltados, ms: ms };
    } finally {
      try { lock.releaseLock(); } catch (e) {}
    }
  } catch (e) {
    console.error("[Tickets sync] Error:", e && e.message ? e.message : e);
    return { exito: false, mensaje: String(e && e.message ? e.message : e) };
  }
}

/**
 * Instala trigger horario de sincronización. Ejecutar UNA VEZ desde el editor.
 */
function instalarTriggerSyncTickets() {
  // Borra triggers viejos del mismo handler para no duplicar
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === "sincronizarTicketsDesdeSap") {
      ScriptApp.deleteTrigger(t);
    }
  });
  ScriptApp.newTrigger("sincronizarTicketsDesdeSap").timeBased().everyHours(1).create();
  console.log("✅ Trigger horario instalado");
  return "Trigger instalado: sincronización cada 1 hora";
}

function crearTicket_(ticketsSheet, lineasSheet, ticketId, q, lineas) {
  const fila = new Array(TICKETS_HEADERS.length).fill("");
  fila[TICKETS_COLS.TICKET_ID - 1] = ticketId;
  fila[TICKETS_COLS.DOC_NUM - 1] = q.docNum;
  fila[TICKETS_COLS.DOC_ENTRY - 1] = q.docEntry || "";
  fila[TICKETS_COLS.DOC_DATE - 1] = q.docDate ? new Date(q.docDate) : "";
  fila[TICKETS_COLS.CARD_CODE - 1] = q.cardCode || "";
  fila[TICKETS_COLS.CARD_NAME - 1] = q.cardName || "";
  fila[TICKETS_COLS.COMENTARIOS - 1] = q.comments || "";
  fila[TICKETS_COLS.NUM_AT_CARD - 1] = q.numAtCard || "";
  fila[TICKETS_COLS.SALES_PERSON - 1] = q.salesPersonName || "";
  fila[TICKETS_COLS.ESTADO - 1] = ESTADOS_TICKET.ABIERTO;
  fila[TICKETS_COLS.ITEMS_TOTAL - 1] = lineas.length;
  fila[TICKETS_COLS.ITEMS_RECOGIDOS - 1] = 0;
  fila[TICKETS_COLS.FECHA_SYNC - 1] = new Date();
  ticketsSheet.appendRow(fila);

  // Líneas
  const filasLineas = lineas.map(function(l, idx) {
    const f = new Array(TICKETS_LINEAS_HEADERS.length).fill("");
    f[TICKETS_LINEAS_COLS.TICKET_ID - 1] = ticketId;
    f[TICKETS_LINEAS_COLS.LINE_NUM - 1] = l.lineNum != null ? l.lineNum : idx;
    f[TICKETS_LINEAS_COLS.ITEM_CODE - 1] = l.itemCode || "";
    f[TICKETS_LINEAS_COLS.DESCRIPCION - 1] = l.itemDescription || "";
    f[TICKETS_LINEAS_COLS.CANTIDAD_PEDIDA - 1] = Number(l.quantity || 0);
    f[TICKETS_LINEAS_COLS.CANTIDAD_RECOGIDA - 1] = 0;
    f[TICKETS_LINEAS_COLS.UBICACION - 1] = l.binCode || "";
    f[TICKETS_LINEAS_COLS.ESTADO_LINEA - 1] = ESTADOS_LINEA_TICKET.PENDIENTE;
    return f;
  });
  if (filasLineas.length > 0) {
    const fi = lineasSheet.getLastRow() + 1;
    lineasSheet.getRange(fi, 1, filasLineas.length, TICKETS_LINEAS_HEADERS.length).setValues(filasLineas);
  }
}

function actualizarTicketAbierto_(ticketsSheet, lineasSheet, row, ticketId, q, lineas) {
  // Solo actualiza campos "informativos" (comentarios, cliente, total items)
  // No toca estado ni auxiliar.
  ticketsSheet.getRange(row, TICKETS_COLS.COMENTARIOS).setValue(q.comments || "");
  ticketsSheet.getRange(row, TICKETS_COLS.CARD_NAME).setValue(q.cardName || "");
  ticketsSheet.getRange(row, TICKETS_COLS.NUM_AT_CARD).setValue(q.numAtCard || "");
  ticketsSheet.getRange(row, TICKETS_COLS.SALES_PERSON).setValue(q.salesPersonName || "");
  ticketsSheet.getRange(row, TICKETS_COLS.ITEMS_TOTAL).setValue(lineas.length);
  ticketsSheet.getRange(row, TICKETS_COLS.FECHA_SYNC).setValue(new Date());
  // No re-escribimos las líneas para no perder progreso si el auxiliar empezó
}

function indiceTicketsExistentes_(sheet) {
  if (sheet.getLastRow() < 2) return {};
  const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, TICKETS_HEADERS.length).getValues();
  const indice = {};
  for (let i = 0; i < data.length; i++) {
    const id = String(data[i][TICKETS_COLS.TICKET_ID - 1]).trim();
    if (!id) continue;
    indice[id] = {
      row: i + 2,
      estado: String(data[i][TICKETS_COLS.ESTADO - 1] || ESTADOS_TICKET.ABIERTO)
    };
  }
  return indice;
}

// =====================================================================
// ENDPOINTS PÚBLICOS (frontend)
// =====================================================================

/**
 * Lista tickets visibles para el usuario actual. Por default trae los del día.
 * AUXILIAR y AGENTE ven los mismos cuadritos.
 */
function obtenerTickets(token, opts) {
  return conSesion_(token, ROLES.AUXILIAR, function(sesion) {
    const sheet = asegurarHojaTickets_();
    if (sheet.getLastRow() < 2) return { exito: true, tickets: [] };
    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, TICKETS_HEADERS.length).getValues();
    const tz = Session.getScriptTimeZone();
    const opciones = opts || {};
    const dias = Number(opciones.dias || 1); // últimos N días
    const corte = new Date();
    corte.setHours(0, 0, 0, 0);
    corte.setDate(corte.getDate() - (dias - 1));

    const tickets = data
      .filter(function(r) {
        const id = String(r[TICKETS_COLS.TICKET_ID - 1]).trim();
        if (!id) return false;
        // filtra por fecha (DOC_DATE o FECHA_SYNC como fallback)
        const fecha = r[TICKETS_COLS.DOC_DATE - 1] || r[TICKETS_COLS.FECHA_SYNC - 1];
        if (fecha instanceof Date && !isNaN(fecha)) return fecha >= corte;
        return true;
      })
      .map(function(r) { return mapearTicketFila_(r, tz); })
      .sort(function(a, b) {
        // ABIERTO primero, luego EN_PREP, luego LISTO, ENTREGADO al final
        const orden = { ABIERTO: 1, EN_PREP: 2, LISTO: 3, ENTREGADO: 4, CANCELADO: 5 };
        const oa = orden[a.estado] || 99;
        const ob = orden[b.estado] || 99;
        if (oa !== ob) return oa - ob;
        // dentro del mismo estado, más reciente primero
        return new Date(b.docDate || 0) - new Date(a.docDate || 0);
      });

    return { exito: true, tickets: tickets };
  });
}

/**
 * Detalle de un ticket: cabecera + líneas con estado de cada una.
 */
function obtenerTicketDetalle(token, ticketId) {
  return conSesion_(token, ROLES.AUXILIAR, function() {
    const sheet = asegurarHojaTickets_();
    const fila = buscarTicketRow_(sheet, ticketId);
    if (!fila) return { exito: false, mensaje: "Ticket no encontrado" };
    const tz = Session.getScriptTimeZone();
    const ticket = mapearTicketFila_(fila.data, tz);

    // Líneas
    const lineasSheet = asegurarHojaTicketsLineas_();
    const lineas = obtenerLineasDeTicket_(lineasSheet, ticketId);

    return { exito: true, ticket: ticket, lineas: lineas };
  });
}

/**
 * Auxiliar toma un ticket. Solo si está ABIERTO.
 */
function tomarTicket(token, ticketId) {
  return conSesion_(token, ROLES.AUXILIAR, function(sesion) {
    const sheet = asegurarHojaTickets_();
    const lock = LockService.getDocumentLock();
    try {
      lock.waitLock(15000);
      const fila = buscarTicketRow_(sheet, ticketId);
      if (!fila) return { exito: false, mensaje: "Ticket no encontrado" };
      const estado = String(fila.data[TICKETS_COLS.ESTADO - 1] || "");
      if (estado !== ESTADOS_TICKET.ABIERTO) {
        const auxActual = String(fila.data[TICKETS_COLS.AUXILIAR - 1] || "(otro)");
        return { exito: false, mensaje: "Ya fue tomado por " + auxActual };
      }
      sheet.getRange(fila.row, TICKETS_COLS.ESTADO).setValue(ESTADOS_TICKET.EN_PREP);
      sheet.getRange(fila.row, TICKETS_COLS.AUXILIAR).setValue(sesion.nombre || sesion.usuario);
      sheet.getRange(fila.row, TICKETS_COLS.FECHA_TOMADO).setValue(new Date());
      cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
      return { exito: true, mensaje: "Pedido tomado" };
    } finally {
      try { lock.releaseLock(); } catch (e) {}
    }
  });
}

/**
 * Marca un item como recogido (o faltante con motivo).
 * Auto-genera movimiento en BASE_OPERATIVA: origen=ubicacion del item, destino=STAGING-DESPACHO.
 */
function marcarItemRecogido(token, ticketId, lineNum, datos) {
  return conSesion_(token, ROLES.AUXILIAR, function(sesion) {
    const lineasSheet = asegurarHojaTicketsLineas_();
    const ticketsSheet = asegurarHojaTickets_();
    const lock = LockService.getDocumentLock();
    try {
      lock.waitLock(15000);

      const filaLinea = buscarLineaRow_(lineasSheet, ticketId, lineNum);
      if (!filaLinea) return { exito: false, mensaje: "Línea no encontrada" };
      const estadoActual = String(filaLinea.data[TICKETS_LINEAS_COLS.ESTADO_LINEA - 1] || "");
      if (estadoActual === ESTADOS_LINEA_TICKET.RECOGIDO) {
        return { exito: false, mensaje: "Línea ya estaba marcada como recogida" };
      }

      const d = datos || {};
      const cantidadPedida = Number(filaLinea.data[TICKETS_LINEAS_COLS.CANTIDAD_PEDIDA - 1] || 0);
      const cantidadRecogida = Number(d.cantidad != null ? d.cantidad : cantidadPedida);
      const itemCode = String(filaLinea.data[TICKETS_LINEAS_COLS.ITEM_CODE - 1] || "");
      const descripcion = String(filaLinea.data[TICKETS_LINEAS_COLS.DESCRIPCION - 1] || "");
      const ubicacion = String(filaLinea.data[TICKETS_LINEAS_COLS.UBICACION - 1] || "");

      let nuevoEstado;
      let motivo = "";
      if (d.faltante === true || cantidadRecogida <= 0) {
        nuevoEstado = ESTADOS_LINEA_TICKET.FALTANTE;
        motivo = normalizarTexto(d.motivo || "sin stock");
      } else {
        nuevoEstado = ESTADOS_LINEA_TICKET.RECOGIDO;
      }

      // Actualiza la línea
      lineasSheet.getRange(filaLinea.row, TICKETS_LINEAS_COLS.CANTIDAD_RECOGIDA).setValue(cantidadRecogida);
      lineasSheet.getRange(filaLinea.row, TICKETS_LINEAS_COLS.ESTADO_LINEA).setValue(nuevoEstado);
      lineasSheet.getRange(filaLinea.row, TICKETS_LINEAS_COLS.MOTIVO_FALTA).setValue(motivo);
      lineasSheet.getRange(filaLinea.row, TICKETS_LINEAS_COLS.RECOGIDO_EN).setValue(new Date());
      lineasSheet.getRange(filaLinea.row, TICKETS_LINEAS_COLS.RECOGIDO_POR).setValue(sesion.nombre || sesion.usuario);

      let movimientoId = "";
      // Si recogido y hay cantidad > 0, genera movimiento auto
      if (nuevoEstado === ESTADOS_LINEA_TICKET.RECOGIDO && cantidadRecogida > 0) {
        const baseSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.BASE_OPERATIVA);
        if (baseSheet) {
          movimientoId = generarID();
          const filaMov = [
            movimientoId,
            new Date(),
            "PEDIDO",
            ticketId,
            itemCode,
            itemCode,
            descripcion,
            cantidadRecogida,
            ubicacion,
            STAGING_DESPACHO,
            sesion.nombre || sesion.usuario,
            ESTADO_MOVIMIENTO.UBICADO,
            false,
            "",
            "",
            ""
          ];
          baseSheet.appendRow(filaMov);
          lineasSheet.getRange(filaLinea.row, TICKETS_LINEAS_COLS.MOVIMIENTO_ID).setValue(movimientoId);
        }
      }

      // Actualiza contador en cabecera del ticket
      const filaT = buscarTicketRow_(ticketsSheet, ticketId);
      if (filaT) {
        const totalLineas = obtenerLineasDeTicket_(lineasSheet, ticketId);
        const recogidos = totalLineas.filter(function(l) {
          return l.estado === ESTADOS_LINEA_TICKET.RECOGIDO || l.estado === ESTADOS_LINEA_TICKET.FALTANTE;
        }).length;
        ticketsSheet.getRange(filaT.row, TICKETS_COLS.ITEMS_RECOGIDOS).setValue(recogidos);
        ticketsSheet.getRange(filaT.row, TICKETS_COLS.ITEMS_TOTAL).setValue(totalLineas.length);
      }

      cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
      return {
        exito: true,
        mensaje: nuevoEstado === ESTADOS_LINEA_TICKET.RECOGIDO ? "Recogido" : "Faltante registrado",
        movimientoId: movimientoId,
        nuevoEstado: nuevoEstado
      };
    } finally {
      try { lock.releaseLock(); } catch (e) {}
    }
  });
}

/**
 * Marca el ticket como LISTO. Sólo si todas las líneas están resueltas (recogidas o faltantes).
 */
function marcarTicketListo(token, ticketId) {
  return conSesion_(token, ROLES.AUXILIAR, function() {
    const sheet = asegurarHojaTickets_();
    const lineasSheet = asegurarHojaTicketsLineas_();
    const lock = LockService.getDocumentLock();
    try {
      lock.waitLock(15000);
      const fila = buscarTicketRow_(sheet, ticketId);
      if (!fila) return { exito: false, mensaje: "Ticket no encontrado" };
      if (fila.data[TICKETS_COLS.ESTADO - 1] !== ESTADOS_TICKET.EN_PREP) {
        return { exito: false, mensaje: "El ticket no está EN_PREP" };
      }
      const lineas = obtenerLineasDeTicket_(lineasSheet, ticketId);
      const pendientes = lineas.filter(function(l) { return l.estado === ESTADOS_LINEA_TICKET.PENDIENTE; });
      if (pendientes.length > 0) {
        return { exito: false, mensaje: "Faltan " + pendientes.length + " líneas por resolver" };
      }
      const fechaTomado = fila.data[TICKETS_COLS.FECHA_TOMADO - 1];
      const tiempoPrep = fechaTomado instanceof Date ? Math.round((Date.now() - fechaTomado.getTime()) / 1000) : 0;
      sheet.getRange(fila.row, TICKETS_COLS.ESTADO).setValue(ESTADOS_TICKET.LISTO);
      sheet.getRange(fila.row, TICKETS_COLS.FECHA_LISTO).setValue(new Date());
      sheet.getRange(fila.row, TICKETS_COLS.TIEMPO_PREP_SEG).setValue(tiempoPrep);
      cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
      return { exito: true, mensaje: "Pedido LISTO para entregar", tiempoPrep: tiempoPrep };
    } finally {
      try { lock.releaseLock(); } catch (e) {}
    }
  });
}

/**
 * Marca el ticket como ENTREGADO (cliente retiró).
 */
function marcarTicketEntregado(token, ticketId) {
  return conSesion_(token, ROLES.AUXILIAR, function() {
    const sheet = asegurarHojaTickets_();
    const lock = LockService.getDocumentLock();
    try {
      lock.waitLock(15000);
      const fila = buscarTicketRow_(sheet, ticketId);
      if (!fila) return { exito: false, mensaje: "Ticket no encontrado" };
      if (fila.data[TICKETS_COLS.ESTADO - 1] !== ESTADOS_TICKET.LISTO) {
        return { exito: false, mensaje: "El ticket no está LISTO" };
      }
      sheet.getRange(fila.row, TICKETS_COLS.ESTADO).setValue(ESTADOS_TICKET.ENTREGADO);
      sheet.getRange(fila.row, TICKETS_COLS.FECHA_ENTREGADO).setValue(new Date());
      cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
      return { exito: true, mensaje: "Entregado al cliente" };
    } finally {
      try { lock.releaseLock(); } catch (e) {}
    }
  });
}

// =====================================================================
// HELPERS INTERNOS
// =====================================================================

function asegurarHojaTickets_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(HOJAS.TICKETS);
  if (!sheet) sheet = ss.insertSheet(HOJAS.TICKETS);
  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, TICKETS_HEADERS.length).setValues([TICKETS_HEADERS]);
    sheet.setFrozenRows(1);
  }
  return sheet;
}

function asegurarHojaTicketsLineas_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(HOJAS.TICKETS_LINEAS);
  if (!sheet) sheet = ss.insertSheet(HOJAS.TICKETS_LINEAS);
  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, TICKETS_LINEAS_HEADERS.length).setValues([TICKETS_LINEAS_HEADERS]);
    sheet.setFrozenRows(1);
  }
  return sheet;
}

function buscarTicketRow_(sheet, ticketId) {
  if (sheet.getLastRow() < 2) return null;
  const id = String(ticketId).trim();
  const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, TICKETS_HEADERS.length).getValues();
  for (let i = 0; i < data.length; i++) {
    if (String(data[i][TICKETS_COLS.TICKET_ID - 1]).trim() === id) {
      return { row: i + 2, data: data[i] };
    }
  }
  return null;
}

function buscarLineaRow_(sheet, ticketId, lineNum) {
  if (sheet.getLastRow() < 2) return null;
  const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, TICKETS_LINEAS_HEADERS.length).getValues();
  const idStr = String(ticketId).trim();
  const lnStr = String(lineNum);
  for (let i = 0; i < data.length; i++) {
    if (String(data[i][TICKETS_LINEAS_COLS.TICKET_ID - 1]).trim() === idStr
        && String(data[i][TICKETS_LINEAS_COLS.LINE_NUM - 1]) === lnStr) {
      return { row: i + 2, data: data[i] };
    }
  }
  return null;
}

function obtenerLineasDeTicket_(sheet, ticketId) {
  if (sheet.getLastRow() < 2) return [];
  const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, TICKETS_LINEAS_HEADERS.length).getValues();
  const idStr = String(ticketId).trim();
  const tz = Session.getScriptTimeZone();
  return data
    .filter(function(r) { return String(r[TICKETS_LINEAS_COLS.TICKET_ID - 1]).trim() === idStr; })
    .map(function(r) {
      return {
        ticketId: r[TICKETS_LINEAS_COLS.TICKET_ID - 1],
        lineNum: r[TICKETS_LINEAS_COLS.LINE_NUM - 1],
        itemCode: r[TICKETS_LINEAS_COLS.ITEM_CODE - 1],
        descripcion: r[TICKETS_LINEAS_COLS.DESCRIPCION - 1],
        cantidadPedida: Number(r[TICKETS_LINEAS_COLS.CANTIDAD_PEDIDA - 1] || 0),
        cantidadRecogida: Number(r[TICKETS_LINEAS_COLS.CANTIDAD_RECOGIDA - 1] || 0),
        ubicacion: r[TICKETS_LINEAS_COLS.UBICACION - 1] || "",
        estado: r[TICKETS_LINEAS_COLS.ESTADO_LINEA - 1] || ESTADOS_LINEA_TICKET.PENDIENTE,
        motivoFalta: r[TICKETS_LINEAS_COLS.MOTIVO_FALTA - 1] || "",
        recogidoEn: formatearFechaSimple_(r[TICKETS_LINEAS_COLS.RECOGIDO_EN - 1], tz),
        recogidoPor: r[TICKETS_LINEAS_COLS.RECOGIDO_POR - 1] || "",
        movimientoId: r[TICKETS_LINEAS_COLS.MOVIMIENTO_ID - 1] || ""
      };
    });
}

function mapearTicketFila_(r, tz) {
  return {
    ticketId: r[TICKETS_COLS.TICKET_ID - 1],
    docNum: r[TICKETS_COLS.DOC_NUM - 1],
    docEntry: r[TICKETS_COLS.DOC_ENTRY - 1],
    docDate: formatearFechaSimple_(r[TICKETS_COLS.DOC_DATE - 1], tz),
    docDateRaw: r[TICKETS_COLS.DOC_DATE - 1] instanceof Date ? r[TICKETS_COLS.DOC_DATE - 1].getTime() : 0,
    cardCode: r[TICKETS_COLS.CARD_CODE - 1],
    cardName: r[TICKETS_COLS.CARD_NAME - 1],
    comentarios: r[TICKETS_COLS.COMENTARIOS - 1],
    numAtCard: r[TICKETS_COLS.NUM_AT_CARD - 1],
    salesPerson: r[TICKETS_COLS.SALES_PERSON - 1],
    estado: r[TICKETS_COLS.ESTADO - 1] || ESTADOS_TICKET.ABIERTO,
    auxiliar: r[TICKETS_COLS.AUXILIAR - 1] || "",
    fechaTomado: formatearFechaSimple_(r[TICKETS_COLS.FECHA_TOMADO - 1], tz),
    fechaTomadoMs: r[TICKETS_COLS.FECHA_TOMADO - 1] instanceof Date ? r[TICKETS_COLS.FECHA_TOMADO - 1].getTime() : 0,
    fechaListo: formatearFechaSimple_(r[TICKETS_COLS.FECHA_LISTO - 1], tz),
    fechaEntregado: formatearFechaSimple_(r[TICKETS_COLS.FECHA_ENTREGADO - 1], tz),
    tiempoPrepSeg: Number(r[TICKETS_COLS.TIEMPO_PREP_SEG - 1] || 0),
    itemsTotal: Number(r[TICKETS_COLS.ITEMS_TOTAL - 1] || 0),
    itemsRecogidos: Number(r[TICKETS_COLS.ITEMS_RECOGIDOS - 1] || 0)
  };
}

function formatearFechaSimple_(valor, tz) {
  if (Object.prototype.toString.call(valor) === "[object Date]" && !isNaN(valor)) {
    return Utilities.formatDate(valor, tz, "dd/MM HH:mm");
  }
  return "";
}
