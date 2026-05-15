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
function sincronizarTicketsDesdeSap(opts) {
  try {
    const o = opts || {};
    const t0 = Date.now();
    const filtros = { status: o.status || "open", limit: 200 };
    if (o.dateFrom) filtros.dateFrom = o.dateFrom;
    const resp = sapListarCotizaciones_(filtros);
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
          // ACTUALIZAR sólo si está PENDIENTE_REVISION o ABIERTO (nadie tomó aún)
          if (existente.estado === ESTADOS_TICKET.PENDIENTE_REVISION || existente.estado === ESTADOS_TICKET.ABIERTO) {
            actualizarTicketAbierto_(ticketsSheet, lineasSheet, existente.row, ticketId, q, lineasSp);
            actualizados++;
          } else {
            ticketsSheet.getRange(existente.row, TICKETS_COLS.FECHA_SYNC).setValue(new Date());
          }
        }
      }

      // Detecta tickets locales aún no tomados que ya no vienen de SAP → marcar CANCELADO
      // SCOPE: sólo cancela OV (este es el sync de cotizaciones, no debe tocar OTs)
      let cancelados = 0;
      Object.keys(indiceLocal).forEach(function(tid) {
        if (docNumsRemotos[tid]) return;
        if (obtenerDocTypeDeTicketId_(tid) !== "OV") return;
        const est = indiceLocal[tid].estado;
        if (est === ESTADOS_TICKET.PENDIENTE_REVISION || est === ESTADOS_TICKET.ABIERTO) {
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

/**
 * Sincroniza Órdenes de Trabajo (OT) abiertas desde SAP — paralelo a sincronizarTicketsDesdeSap.
 * Usa ticketId = "OT-" + docNum para evitar colisión con cotizaciones.
 * Si el endpoint no está publicado por Andre todavía, devuelve un mensaje amigable
 * apuntando a la solicitud documentada en docs/solicitud-endpoint-OT-andre.pdf
 */
function sincronizarOTDesdeSap(opts) {
  try {
    // Feature flag: si OT no está habilitado, no llamamos al endpoint todavía
    if (!sapOTHabilitado_()) {
      return {
        exito: false,
        pendienteAndre: true,
        mensaje: "OT aún no habilitado. Andre confirmará vía sapOTHabilitado_ cuando esté vivo (calendario: 20-22/05/2026)."
      };
    }
    const o = opts || {};
    const t0 = Date.now();
    const filtros = { status: o.status || "open", limit: 200 };
    if (o.dateFrom) filtros.dateFrom = o.dateFrom;
    if (o.dateTo) filtros.dateTo = o.dateTo;
    const resp = sapListarOT_(filtros);
    const remoto = (resp && resp.data) || [];

    const ticketsSheet = asegurarHojaTickets_();
    const lineasSheet = asegurarHojaTicketsLineas_();
    const lock = LockService.getDocumentLock();
    let creados = 0, actualizados = 0, saltados = 0;

    try {
      lock.waitLock(30000);
      const indiceLocal = indiceTicketsExistentes_(ticketsSheet);
      const ticketIdsRemotos = {};

      for (let i = 0; i < remoto.length; i++) {
        const q = remoto[i];
        if (!q || !q.docNum) continue;
        const lineasSp = (q.lines || []).filter(function(l) {
          return l && l.warehouseCode === WAREHOUSE_SP;
        });
        if (lineasSp.length === 0) { saltados++; continue; }

        const ticketId = "OT-" + String(q.docNum);
        ticketIdsRemotos[ticketId] = true;
        const existente = indiceLocal[ticketId];

        if (!existente) {
          crearTicket_(ticketsSheet, lineasSheet, ticketId, q, lineasSp);
          creados++;
        } else {
          if (existente.estado === ESTADOS_TICKET.PENDIENTE_REVISION || existente.estado === ESTADOS_TICKET.ABIERTO) {
            actualizarTicketAbierto_(ticketsSheet, lineasSheet, existente.row, ticketId, q, lineasSp);
            actualizados++;
          } else {
            ticketsSheet.getRange(existente.row, TICKETS_COLS.FECHA_SYNC).setValue(new Date());
          }
        }
      }

      // Scope: este sync sólo cancela OT (no toca OV)
      let cancelados = 0;
      Object.keys(indiceLocal).forEach(function(tid) {
        if (ticketIdsRemotos[tid]) return;
        if (obtenerDocTypeDeTicketId_(tid) !== "OT") return;
        const est = indiceLocal[tid].estado;
        if (est === ESTADOS_TICKET.PENDIENTE_REVISION || est === ESTADOS_TICKET.ABIERTO) {
          ticketsSheet.getRange(indiceLocal[tid].row, TICKETS_COLS.ESTADO).setValue(ESTADOS_TICKET.CANCELADO);
          cancelados++;
        }
      });

      cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
      const ms = Date.now() - t0;
      console.log("[OT sync] " + ms + "ms · creados=" + creados + " actualizados=" + actualizados + " cancelados=" + cancelados + " saltados=" + saltados);
      return { exito: true, creados: creados, actualizados: actualizados, cancelados: cancelados, saltados: saltados, ms: ms };
    } finally {
      try { lock.releaseLock(); } catch (e) {}
    }
  } catch (e) {
    const msg = String(e && e.message ? e.message : e);
    if (/HTTP 404|work-orders|endpoint/i.test(msg)) {
      return {
        exito: false,
        pendienteAndre: true,
        mensaje: "El endpoint /api/ubyguard/work-orders aún no está publicado por Andre. Solicitud técnica en docs/solicitud-endpoint-OT-andre.pdf"
      };
    }
    console.error("[OT sync] Error:", msg);
    return { exito: false, mensaje: msg };
  }
}

/**
 * Determina el tipo de documento (OT vs OV) por convención del ticketId:
 * - "OT-..." → OT
 * - cualquier otra cosa → OV (incluye data legacy con ticketId = docNum sin prefijo)
 */
function obtenerDocTypeDeTicketId_(ticketId) {
  return /^OT-/.test(String(ticketId || "")) ? "OT" : "OV";
}

function crearTicket_(ticketsSheet, lineasSheet, ticketId, q, lineas) {
  // Auto-clasificación: cuenta cuántas líneas son para inventario
  const lineasParaPrep = lineas.filter(function(l) { return esLineaInventario_(l.itemCode); });

  const fila = new Array(TICKETS_HEADERS.length).fill("");
  fila[TICKETS_COLS.TICKET_ID - 1] = escaparFormula_(ticketId);
  fila[TICKETS_COLS.DOC_NUM - 1] = escaparFormula_(q.docNum);
  fila[TICKETS_COLS.DOC_ENTRY - 1] = q.docEntry || "";
  fila[TICKETS_COLS.DOC_DATE - 1] = q.docDate ? new Date(q.docDate) : "";
  fila[TICKETS_COLS.CARD_CODE - 1] = escaparFormula_(q.cardCode || "");
  fila[TICKETS_COLS.CARD_NAME - 1] = escaparFormula_(q.cardName || "");
  fila[TICKETS_COLS.COMENTARIOS - 1] = escaparFormula_(q.comments || "");
  fila[TICKETS_COLS.NUM_AT_CARD - 1] = escaparFormula_(q.numAtCard || "");
  fila[TICKETS_COLS.SALES_PERSON - 1] = escaparFormula_(q.salesPersonName || "");
  fila[TICKETS_COLS.ESTADO - 1] = ESTADOS_TICKET.PENDIENTE_REVISION;
  fila[TICKETS_COLS.ITEMS_TOTAL - 1] = lineasParaPrep.length; // solo cuenta items de inventario
  fila[TICKETS_COLS.ITEMS_RECOGIDOS - 1] = 0;
  fila[TICKETS_COLS.FECHA_SYNC - 1] = new Date();
  ticketsSheet.appendRow(fila);

  // Líneas: todas se guardan, pero las SER* (servicios) van con INCLUIDA_PREPARACION = false
  const filasLineas = lineas.map(function(l, idx) {
    const f = new Array(TICKETS_LINEAS_HEADERS.length).fill("");
    const incluida = esLineaInventario_(l.itemCode);
    f[TICKETS_LINEAS_COLS.TICKET_ID - 1] = escaparFormula_(ticketId);
    f[TICKETS_LINEAS_COLS.LINE_NUM - 1] = l.lineNum != null ? l.lineNum : idx;
    f[TICKETS_LINEAS_COLS.ITEM_CODE - 1] = escaparFormula_(l.itemCode || "");
    f[TICKETS_LINEAS_COLS.DESCRIPCION - 1] = escaparFormula_(l.itemDescription || "");
    f[TICKETS_LINEAS_COLS.CANTIDAD_PEDIDA - 1] = Number(l.quantity || 0);
    f[TICKETS_LINEAS_COLS.CANTIDAD_RECOGIDA - 1] = 0;
    f[TICKETS_LINEAS_COLS.UBICACION - 1] = escaparFormula_(l.binCode || "");
    f[TICKETS_LINEAS_COLS.ESTADO_LINEA - 1] = ESTADOS_LINEA_TICKET.PENDIENTE;
    f[TICKETS_LINEAS_COLS.INCLUIDA_PREPARACION - 1] = incluida;
    f[TICKETS_LINEAS_COLS.EXCLUIDA_POR - 1] = incluida ? "" : "sistema";
    f[TICKETS_LINEAS_COLS.MOTIVO_EXCLUSION - 1] = incluida ? "" : "servicio (no inventario)";
    return f;
  });
  if (filasLineas.length > 0) {
    const fi = lineasSheet.getLastRow() + 1;
    lineasSheet.getRange(fi, 1, filasLineas.length, TICKETS_LINEAS_HEADERS.length).setValues(filasLineas);
  }
}

/**
 * Detecta si un itemCode representa inventario físico (a preparar) o servicio.
 * Hoy: cualquier prefijo en PREFIJOS_NO_INVENTARIO = NO. Otros = SÍ.
 */
function esLineaInventario_(itemCode) {
  const code = String(itemCode || "").trim().toUpperCase();
  if (!code) return false;
  for (let i = 0; i < PREFIJOS_NO_INVENTARIO.length; i++) {
    if (code.indexOf(PREFIJOS_NO_INVENTARIO[i]) === 0) return false;
  }
  return true;
}

function actualizarTicketAbierto_(ticketsSheet, lineasSheet, row, ticketId, q, lineas) {
  // Solo actualiza campos "informativos" (comentarios, cliente, total items)
  // No toca estado ni auxiliar.
  ticketsSheet.getRange(row, TICKETS_COLS.COMENTARIOS).setValue(escaparFormula_(q.comments || ""));
  ticketsSheet.getRange(row, TICKETS_COLS.CARD_NAME).setValue(escaparFormula_(q.cardName || ""));
  ticketsSheet.getRange(row, TICKETS_COLS.NUM_AT_CARD).setValue(escaparFormula_(q.numAtCard || ""));
  ticketsSheet.getRange(row, TICKETS_COLS.SALES_PERSON).setValue(escaparFormula_(q.salesPersonName || ""));
  const incluidas = lineas.filter(function(l) { return esLineaInventario_(l.itemCode); });
  ticketsSheet.getRange(row, TICKETS_COLS.ITEMS_TOTAL).setValue(incluidas.length);
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
 * Vista PICKUP — sólo tickets que el agente envió a pickup (ABIERTO / EN_PREP / LISTO / ENTREGADO).
 * Excluye explícitamente PENDIENTE_REVISION (esos van por obtenerPedidosRevision).
 * opts = { dias?: number, fecha?: "YYYY-MM-DD" }
 */
function obtenerTickets(token, opts) {
  return conSesion_(token, ROLES.AUXILIAR, function(sesion) {
    const sheet = asegurarHojaTickets_();
    if (sheet.getLastRow() < 2) return { exito: true, tickets: [] };
    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, TICKETS_HEADERS.length).getValues();
    const tz = Session.getScriptTimeZone();
    const opciones = opts || {};

    const rangoFecha = construirRangoFecha_(opciones, tz);

    const tickets = data
      .filter(function(r) {
        const id = String(r[TICKETS_COLS.TICKET_ID - 1]).trim();
        if (!id) return false;
        const estado = String(r[TICKETS_COLS.ESTADO - 1] || "");
        // Pickup excluye PENDIENTE_REVISION
        if (estado === ESTADOS_TICKET.PENDIENTE_REVISION) return false;
        return ticketEnRango_(r, rangoFecha);
      })
      .map(function(r) { return mapearTicketFila_(r, tz); })
      .sort(ordenTicketsPickup_);

    return { exito: true, tickets: tickets };
  });
}

/**
 * Vista PEDIDOS (solo AGENTE) — pedidos pendientes de clasificar para una fecha.
 * Retorna también los ya enviados a pickup para que el agente vea el avance.
 * opts = { fecha?: "YYYY-MM-DD" (default: hoy), incluirEnviados?: boolean (default true) }
 */
function obtenerPedidosRevision(token, opts) {
  return conSesion_(token, ROLES.AGENTE, function() {
    const sheet = asegurarHojaTickets_();
    if (sheet.getLastRow() < 2) return { exito: true, pendientes: [], enviados: [] };
    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, TICKETS_HEADERS.length).getValues();
    const tz = Session.getScriptTimeZone();
    const o = opts || {};
    const rangoFecha = construirRangoFecha_({ fecha: o.fecha || fechaHoyISO_(tz) }, tz);

    const pendientes = [];
    const enviados = [];

    data.forEach(function(r) {
      const id = String(r[TICKETS_COLS.TICKET_ID - 1]).trim();
      if (!id) return;
      if (!ticketEnRango_(r, rangoFecha)) return;
      const estado = String(r[TICKETS_COLS.ESTADO - 1] || "");
      const mapeado = mapearTicketFila_(r, tz);
      if (estado === ESTADOS_TICKET.PENDIENTE_REVISION) {
        pendientes.push(mapeado);
      } else if (o.incluirEnviados !== false &&
                 (estado === ESTADOS_TICKET.ABIERTO || estado === ESTADOS_TICKET.EN_PREP || estado === ESTADOS_TICKET.LISTO)) {
        enviados.push(mapeado);
      }
    });

    pendientes.sort(function(a, b) { return Number(b.docNum || 0) - Number(a.docNum || 0); });
    enviados.sort(function(a, b) {
      // Primero por estado (ABIERTO > EN_PREP > LISTO), luego docNum desc
      const ordenEst = { ABIERTO: 1, EN_PREP: 2, LISTO: 3 };
      const oa = ordenEst[a.estado] || 99;
      const ob = ordenEst[b.estado] || 99;
      if (oa !== ob) return oa - ob;
      return Number(b.docNum || 0) - Number(a.docNum || 0);
    });
    return { exito: true, pendientes: pendientes, enviados: enviados, fecha: rangoFecha.fechaISO };
  });
}

/**
 * AGENTE envía un ticket a pickup (PENDIENTE_REVISION → ABIERTO).
 * Requiere que el agente haya clasificado las líneas (al menos una incluida).
 */
function enviarAPickup(token, ticketId) {
  return conSesion_(token, ROLES.AGENTE, function() {
    const sheet = asegurarHojaTickets_();
    const lineasSheet = asegurarHojaTicketsLineas_();
    const lock = LockService.getDocumentLock();
    try {
      lock.waitLock(15000);
      const fila = buscarTicketRow_(sheet, ticketId);
      if (!fila) return { exito: false, mensaje: "Ticket no encontrado" };
      const estado = String(fila.data[TICKETS_COLS.ESTADO - 1] || "");
      if (estado !== ESTADOS_TICKET.PENDIENTE_REVISION) {
        return { exito: false, mensaje: "La orden ya fue enviada a Picking o no aplica" };
      }
      const todas = obtenerLineasDeTicket_(lineasSheet, ticketId);
      const incluidas = todas.filter(function(l) { return l.incluida; });
      if (incluidas.length === 0) {
        return { exito: false, mensaje: "Debes incluir al menos una línea antes de enviar" };
      }
      sheet.getRange(fila.row, TICKETS_COLS.ESTADO).setValue(ESTADOS_TICKET.ABIERTO);
      sheet.getRange(fila.row, TICKETS_COLS.ITEMS_TOTAL).setValue(incluidas.length);
      cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
      return { exito: true, mensaje: "Enviado a Picking", items: incluidas.length };
    } finally {
      try { lock.releaseLock(); } catch (e) {}
    }
  });
}

/**
 * AGENTE devuelve un ticket de ABIERTO → PENDIENTE_REVISION para volver a clasificarlo.
 * Sólo permitido si ningún auxiliar lo tomó (AUXILIAR vacío y ninguna línea trabajada).
 */
function devolverARevision(token, ticketId) {
  return conSesion_(token, ROLES.AGENTE, function() {
    const sheet = asegurarHojaTickets_();
    const lineasSheet = asegurarHojaTicketsLineas_();
    const lock = LockService.getDocumentLock();
    try {
      lock.waitLock(15000);
      const fila = buscarTicketRow_(sheet, ticketId);
      if (!fila) return { exito: false, mensaje: "Ticket no encontrado" };
      const estado = String(fila.data[TICKETS_COLS.ESTADO - 1] || "");
      if (estado !== ESTADOS_TICKET.ABIERTO) {
        return { exito: false, mensaje: "Solo se pueden devolver órdenes ABIERTAS sin tomar" };
      }
      const aux = String(fila.data[TICKETS_COLS.AUXILIAR - 1] || "").trim();
      if (aux) {
        return { exito: false, mensaje: "Ya fue tomado por " + aux + " · no se puede devolver" };
      }
      // Chequeo extra: ninguna línea trabajada
      const lineas = obtenerLineasDeTicket_(lineasSheet, ticketId);
      const trabajadas = lineas.filter(function(l) {
        return l.estado === ESTADOS_LINEA_TICKET.RECOGIDO || l.estado === ESTADOS_LINEA_TICKET.FALTANTE;
      });
      if (trabajadas.length > 0) {
        return { exito: false, mensaje: "Hay líneas trabajadas · no se puede devolver" };
      }
      sheet.getRange(fila.row, TICKETS_COLS.ESTADO).setValue(ESTADOS_TICKET.PENDIENTE_REVISION);
      cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
      return { exito: true, mensaje: "Devuelto a revisión" };
    } finally {
      try { lock.releaseLock(); } catch (e) {}
    }
  });
}

/**
 * Migración masiva (one-shot): pasa todos los ABIERTOS sin auxiliar a PENDIENTE_REVISION.
 * Útil para data heredada que se sincronizó antes del cambio de estado.
 * @param {Object} opts { fecha?: "YYYY-MM-DD" } — si se da, solo afecta esa fecha
 */
function devolverTodosARevision(token, opts) {
  return conSesion_(token, ROLES.AGENTE, function() {
    const o = opts || {};
    const sheet = asegurarHojaTickets_();
    const lineasSheet = asegurarHojaTicketsLineas_();
    if (sheet.getLastRow() < 2) return { exito: true, devueltos: 0 };
    const tz = Session.getScriptTimeZone();
    const rango = construirRangoFecha_({ fecha: o.fecha || "" }, tz);

    const lock = LockService.getDocumentLock();
    try {
      lock.waitLock(30000);
      const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, TICKETS_HEADERS.length).getValues();
      const idsConTrabajadas = trabajadasPorTicket_(lineasSheet);
      let devueltos = 0;
      for (let i = 0; i < data.length; i++) {
        const r = data[i];
        const estado = String(r[TICKETS_COLS.ESTADO - 1] || "");
        if (estado !== ESTADOS_TICKET.ABIERTO) continue;
        const aux = String(r[TICKETS_COLS.AUXILIAR - 1] || "").trim();
        if (aux) continue;
        const tid = String(r[TICKETS_COLS.TICKET_ID - 1] || "").trim();
        if (idsConTrabajadas[tid]) continue;
        if (o.fecha && !ticketEnRango_(r, rango)) continue;
        sheet.getRange(i + 2, TICKETS_COLS.ESTADO).setValue(ESTADOS_TICKET.PENDIENTE_REVISION);
        devueltos++;
      }
      cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
      return { exito: true, devueltos: devueltos };
    } finally {
      try { lock.releaseLock(); } catch (e) {}
    }
  });
}

/**
 * Cuenta tickets ABIERTOS sin auxiliar tomado (legacy candidates) en TODA la hoja,
 * sin filtrar por fecha. Lo usa el frontend para mostrar el botón "Devolver todos
 * a revisión" cuando hay legacy de cualquier fecha.
 */
function contarAbiertosSinTomar(token) {
  return conSesion_(token, ROLES.AGENTE, function() {
    const sheet = asegurarHojaTickets_();
    const lineasSheet = asegurarHojaTicketsLineas_();
    if (sheet.getLastRow() < 2) return { exito: true, count: 0 };
    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, TICKETS_HEADERS.length).getValues();
    const idsConTrabajadas = trabajadasPorTicket_(lineasSheet);
    let n = 0;
    for (let i = 0; i < data.length; i++) {
      const r = data[i];
      if (String(r[TICKETS_COLS.ESTADO - 1] || "") !== ESTADOS_TICKET.ABIERTO) continue;
      if (String(r[TICKETS_COLS.AUXILIAR - 1] || "").trim()) continue;
      const tid = String(r[TICKETS_COLS.TICKET_ID - 1] || "").trim();
      if (idsConTrabajadas[tid]) continue;
      n++;
    }
    return { exito: true, count: n };
  });
}

function trabajadasPorTicket_(lineasSheet) {
  const out = {};
  if (lineasSheet.getLastRow() < 2) return out;
  const data = lineasSheet.getRange(2, 1, lineasSheet.getLastRow() - 1, TICKETS_LINEAS_HEADERS.length).getValues();
  for (let i = 0; i < data.length; i++) {
    const estado = String(data[i][TICKETS_LINEAS_COLS.ESTADO_LINEA - 1] || "");
    if (estado === ESTADOS_LINEA_TICKET.RECOGIDO || estado === ESTADOS_LINEA_TICKET.FALTANTE) {
      out[String(data[i][TICKETS_LINEAS_COLS.TICKET_ID - 1]).trim()] = true;
    }
  }
  return out;
}

/**
 * Indicadores de pickup para el home.
 * opts = { dias?: number (default 1) }
 * Devuelve: tiempoPromedioMin, despachados, precisionPct, enPrepActuales, listosActuales, pendientesRevision
 */
function obtenerIndicadoresPickup(token, opts) {
  return conSesion_(token, ROLES.AUXILIAR, function() {
    const o = opts || {};
    const dias = Math.max(1, Number(o.dias || 1));
    const sheet = asegurarHojaTickets_();
    const lineasSheet = asegurarHojaTicketsLineas_();
    if (sheet.getLastRow() < 2) return { exito: true, tiempoPromedioMin: 0, despachados: 0, precisionPct: 0, enPrepActuales: 0, listosActuales: 0, pendientesRevision: 0 };

    const corte = new Date();
    corte.setHours(0, 0, 0, 0);
    corte.setDate(corte.getDate() - (dias - 1));

    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, TICKETS_HEADERS.length).getValues();
    let sumaTiempo = 0, countTiempo = 0;
    let despachados = 0, enPrep = 0, listos = 0, pendientesRev = 0;
    const ticketIdsVentana = {};

    for (let i = 0; i < data.length; i++) {
      const r = data[i];
      const estado = String(r[TICKETS_COLS.ESTADO - 1] || "");
      const fechaRef = r[TICKETS_COLS.FECHA_TOMADO - 1] || r[TICKETS_COLS.DOC_DATE - 1];
      const enVentana = fechaRef instanceof Date && fechaRef >= corte;

      if (estado === ESTADOS_TICKET.PENDIENTE_REVISION) pendientesRev++;
      if (estado === ESTADOS_TICKET.EN_PREP) enPrep++;
      if (estado === ESTADOS_TICKET.LISTO) listos++;

      if (enVentana && estado === ESTADOS_TICKET.ENTREGADO) {
        despachados++;
        const tSeg = Number(r[TICKETS_COLS.TIEMPO_PREP_SEG - 1] || 0);
        if (tSeg > 0) { sumaTiempo += tSeg; countTiempo++; }
        ticketIdsVentana[String(r[TICKETS_COLS.TICKET_ID - 1]).trim()] = true;
      }
    }

    // Precisión de inventario: de las líneas trabajadas en la ventana,
    // % de RECOGIDO con ubicación no vacía
    let precisionPct = 0;
    if (Object.keys(ticketIdsVentana).length > 0 && lineasSheet.getLastRow() > 1) {
      const ld = lineasSheet.getRange(2, 1, lineasSheet.getLastRow() - 1, TICKETS_LINEAS_HEADERS.length).getValues();
      let trabajadas = 0, conUbicacion = 0;
      for (let i = 0; i < ld.length; i++) {
        const tid = String(ld[i][TICKETS_LINEAS_COLS.TICKET_ID - 1]).trim();
        if (!ticketIdsVentana[tid]) continue;
        const estLin = String(ld[i][TICKETS_LINEAS_COLS.ESTADO_LINEA - 1] || "");
        if (estLin !== ESTADOS_LINEA_TICKET.RECOGIDO && estLin !== ESTADOS_LINEA_TICKET.FALTANTE) continue;
        trabajadas++;
        if (estLin === ESTADOS_LINEA_TICKET.RECOGIDO && String(ld[i][TICKETS_LINEAS_COLS.UBICACION - 1] || "").trim()) {
          conUbicacion++;
        }
      }
      if (trabajadas > 0) precisionPct = Math.round((conUbicacion / trabajadas) * 100);
    }

    return {
      exito: true,
      tiempoPromedioMin: countTiempo > 0 ? Math.round((sumaTiempo / countTiempo) / 60 * 10) / 10 : 0,
      despachados: despachados,
      precisionPct: precisionPct,
      enPrepActuales: enPrep,
      listosActuales: listos,
      pendientesRevision: pendientesRev
    };
  });
}

// ---- Helpers de filtro por fecha ----

function fechaHoyISO_(tz) {
  return Utilities.formatDate(new Date(), tz, "yyyy-MM-dd");
}

function construirRangoFecha_(opciones, tz) {
  // Semántica: "DESDE esa fecha en adelante" (sin límite superior).
  // Una OV/OT creada hace varios días pero todavía abierta sigue siendo relevante,
  // así que no tiene sentido filtrar por día exacto. Si el frontend manda fecha,
  // se interpreta como "ver todo desde esa fecha hasta hoy".
  if (opciones.fecha) {
    const partes = String(opciones.fecha).split("-");
    if (partes.length === 3) {
      const desde = new Date(Number(partes[0]), Number(partes[1]) - 1, Number(partes[2]), 0, 0, 0, 0);
      return { desde: desde, hasta: null, fechaISO: opciones.fecha };
    }
  }
  const dias = Math.max(1, Number(opciones.dias || 1));
  const corte = new Date();
  corte.setHours(0, 0, 0, 0);
  corte.setDate(corte.getDate() - (dias - 1));
  return { desde: corte, hasta: null, fechaISO: null };
}

function ticketEnRango_(r, rango) {
  const fecha = r[TICKETS_COLS.DOC_DATE - 1] || r[TICKETS_COLS.FECHA_SYNC - 1];
  if (!(fecha instanceof Date) || isNaN(fecha)) return true;
  if (rango.hasta) return fecha >= rango.desde && fecha <= rango.hasta;
  return fecha >= rango.desde;
}

function ordenTicketsPickup_(a, b) {
  const orden = { ABIERTO: 1, EN_PREP: 2, LISTO: 3, ENTREGADO: 4, CANCELADO: 5 };
  const oa = orden[a.estado] || 99;
  const ob = orden[b.estado] || 99;
  if (oa !== ob) return oa - ob;
  return new Date(b.docDate || 0) - new Date(a.docDate || 0);
}

/**
 * Detalle de un ticket: cabecera + líneas enriquecidas con stock/ubicación/equivalentes.
 */
function obtenerTicketDetalle(token, ticketId) {
  return conSesion_(token, ROLES.AUXILIAR, function() {
    const sheet = asegurarHojaTickets_();
    const fila = buscarTicketRow_(sheet, ticketId);
    if (!fila) return { exito: false, mensaje: "Ticket no encontrado" };
    const tz = Session.getScriptTimeZone();
    const ticket = mapearTicketFila_(fila.data, tz);

    const lineasSheet = asegurarHojaTicketsLineas_();
    const lineas = obtenerLineasDeTicket_(lineasSheet, ticketId);

    // Enriquece cada línea con info del índice local (stock, ubicación canónica, equivalentes)
    try {
      lineas.forEach(function(l) {
        const info = obtenerArticuloPorIdentificador_(l.itemCode);
        if (info) {
          l.stockSap = Number(info.stock || 0);
          l.ubicacionSap = info.ubicacion || "";
          l.parteSap = info.parte || "";
          l.equivalentes = info.equivalentes || [];
        } else {
          l.stockSap = null;
          l.ubicacionSap = "";
          l.parteSap = "";
          l.equivalentes = [];
        }
      });
    } catch (e) {
      // Si el índice falla, el detalle sigue funcionando sin el enriquecimiento
    }

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
      sheet.getRange(fila.row, TICKETS_COLS.AUXILIAR).setValue(escaparFormula_(sesion.nombre || sesion.usuario));
      sheet.getRange(fila.row, TICKETS_COLS.FECHA_TOMADO).setValue(new Date());
      cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
      return { exito: true, mensaje: "Orden tomada" };
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
      const incluidaLinea = parseBoolIncluida_(
        filaLinea.data[TICKETS_LINEAS_COLS.INCLUIDA_PREPARACION - 1],
        filaLinea.data[TICKETS_LINEAS_COLS.ITEM_CODE - 1]
      );
      if (!incluidaLinea) {
        return { exito: false, mensaje: "Línea excluida de la preparación por el agente" };
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
      lineasSheet.getRange(filaLinea.row, TICKETS_LINEAS_COLS.MOTIVO_FALTA).setValue(escaparFormula_(motivo));
      lineasSheet.getRange(filaLinea.row, TICKETS_LINEAS_COLS.RECOGIDO_EN).setValue(new Date());
      lineasSheet.getRange(filaLinea.row, TICKETS_LINEAS_COLS.RECOGIDO_POR).setValue(escaparFormula_(sesion.nombre || sesion.usuario));

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
            escaparFormula_(ticketId),
            escaparFormula_(itemCode),
            escaparFormula_(itemCode),
            escaparFormula_(descripcion),
            cantidadRecogida,
            escaparFormula_(ubicacion),
            STAGING_DESPACHO,
            escaparFormula_(sesion.nombre || sesion.usuario),
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

      // Actualiza contador en cabecera del ticket (sólo cuenta líneas incluidas)
      const filaT = buscarTicketRow_(ticketsSheet, ticketId);
      if (filaT) {
        const totalLineas = obtenerLineasDeTicket_(lineasSheet, ticketId);
        const incluidas = totalLineas.filter(function(l) { return l.incluida; });
        const recogidos = incluidas.filter(function(l) {
          return l.estado === ESTADOS_LINEA_TICKET.RECOGIDO || l.estado === ESTADOS_LINEA_TICKET.FALTANTE;
        }).length;
        ticketsSheet.getRange(filaT.row, TICKETS_COLS.ITEMS_RECOGIDOS).setValue(recogidos);
        ticketsSheet.getRange(filaT.row, TICKETS_COLS.ITEMS_TOTAL).setValue(incluidas.length);
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
 * AGENTE toggle: incluir / excluir una línea del ticket de la preparación.
 * No se permite si la línea ya fue trabajada (RECOGIDO o FALTANTE).
 * Recalcula contadores en la cabecera.
 */
function toggleLineaIncluida(token, ticketId, lineNum, incluir, motivo) {
  return conSesion_(token, ROLES.AGENTE, function(sesion) {
    const lineasSheet = asegurarHojaTicketsLineas_();
    const ticketsSheet = asegurarHojaTickets_();
    const lock = LockService.getDocumentLock();
    try {
      lock.waitLock(15000);

      const filaLinea = buscarLineaRow_(lineasSheet, ticketId, lineNum);
      if (!filaLinea) return { exito: false, mensaje: "Línea no encontrada" };

      const estadoActual = String(filaLinea.data[TICKETS_LINEAS_COLS.ESTADO_LINEA - 1] || "");
      if (estadoActual === ESTADOS_LINEA_TICKET.RECOGIDO || estadoActual === ESTADOS_LINEA_TICKET.FALTANTE) {
        return { exito: false, mensaje: "La línea ya fue trabajada, no se puede cambiar" };
      }

      const nuevaIncluida = (incluir === true);
      lineasSheet.getRange(filaLinea.row, TICKETS_LINEAS_COLS.INCLUIDA_PREPARACION).setValue(nuevaIncluida);
      lineasSheet.getRange(filaLinea.row, TICKETS_LINEAS_COLS.EXCLUIDA_POR).setValue(
        nuevaIncluida ? "" : escaparFormula_(sesion.nombre || sesion.usuario)
      );
      lineasSheet.getRange(filaLinea.row, TICKETS_LINEAS_COLS.MOTIVO_EXCLUSION).setValue(
        nuevaIncluida ? "" : escaparFormula_(normalizarTexto(motivo || "excluida por agente"))
      );

      // Recalcula contadores en la cabecera
      const filaT = buscarTicketRow_(ticketsSheet, ticketId);
      if (filaT) {
        const todas = obtenerLineasDeTicket_(lineasSheet, ticketId);
        const incluidas = todas.filter(function(l) { return l.incluida; });
        const recogidos = incluidas.filter(function(l) {
          return l.estado === ESTADOS_LINEA_TICKET.RECOGIDO || l.estado === ESTADOS_LINEA_TICKET.FALTANTE;
        }).length;
        ticketsSheet.getRange(filaT.row, TICKETS_COLS.ITEMS_TOTAL).setValue(incluidas.length);
        ticketsSheet.getRange(filaT.row, TICKETS_COLS.ITEMS_RECOGIDOS).setValue(recogidos);
      }

      cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
      return { exito: true, mensaje: nuevaIncluida ? "Línea incluida" : "Línea excluida", incluida: nuevaIncluida };
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
      const pendientes = lineas.filter(function(l) {
        return l.incluida && l.estado === ESTADOS_LINEA_TICKET.PENDIENTE;
      });
      if (pendientes.length > 0) {
        return { exito: false, mensaje: "Faltan " + pendientes.length + " líneas por resolver" };
      }
      const fechaTomado = fila.data[TICKETS_COLS.FECHA_TOMADO - 1];
      const tiempoPrep = fechaTomado instanceof Date ? Math.round((Date.now() - fechaTomado.getTime()) / 1000) : 0;
      sheet.getRange(fila.row, TICKETS_COLS.ESTADO).setValue(ESTADOS_TICKET.LISTO);
      sheet.getRange(fila.row, TICKETS_COLS.FECHA_LISTO).setValue(new Date());
      sheet.getRange(fila.row, TICKETS_COLS.TIEMPO_PREP_SEG).setValue(tiempoPrep);
      cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
      return { exito: true, mensaje: "Orden LISTA para entregar", tiempoPrep: tiempoPrep };
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
      // Lecturas tolerantes para filas antiguas sin estas columnas (vienen undefined o "")
      const rawInc = r[TICKETS_LINEAS_COLS.INCLUIDA_PREPARACION - 1];
      const incluida = parseBoolIncluida_(rawInc, r[TICKETS_LINEAS_COLS.ITEM_CODE - 1]);
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
        movimientoId: r[TICKETS_LINEAS_COLS.MOVIMIENTO_ID - 1] || "",
        incluida: incluida,
        excluidaPor: r[TICKETS_LINEAS_COLS.EXCLUIDA_POR - 1] || "",
        motivoExclusion: r[TICKETS_LINEAS_COLS.MOTIVO_EXCLUSION - 1] || ""
      };
    });
}

/**
 * Filas viejas no tienen INCLUIDA_PREPARACION: se infiere por el prefijo (SER → false, resto → true).
 * Si la celda ya tiene un booleano explícito, se respeta.
 */
function parseBoolIncluida_(raw, itemCode) {
  if (raw === true || raw === false) return raw;
  const s = String(raw == null ? "" : raw).trim().toUpperCase();
  if (s === "TRUE" || s === "VERDADERO" || s === "SI" || s === "SÍ") return true;
  if (s === "FALSE" || s === "FALSO" || s === "NO") return false;
  // celda vacía → fallback por prefijo
  return esLineaInventario_(itemCode);
}

function mapearTicketFila_(r, tz) {
  const tid = r[TICKETS_COLS.TICKET_ID - 1];
  return {
    ticketId: tid,
    docType: obtenerDocTypeDeTicketId_(tid),
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
