/**
 * UBYGUARD - Índice en memoria/caché de DATA_SAP.
 *
 * Formato compacto por registro: [parte, codigo, descripcion, existencia, ubicacion]
 * (array en vez de objeto → ~40% menos JSON en caché con 15k filas).
 *
 * Flujo de lectura:
 *   memo (misma invocación) → CacheService (chunked) → Sheets (rebuild)
 *
 * Invalidación: fechaejecutado.onEdit() al detectar edición en DATA_SAP.
 */

var _SAP_INDEX_MEMO_ = null;

function obtenerIndiceSap_(forzarRebuild) {
  if (!forzarRebuild && _SAP_INDEX_MEMO_) return _SAP_INDEX_MEMO_;

  if (!forzarRebuild) {
    const cached = cacheObtenerJSON_(CACHE_KEYS.SAP_INDEX);
    if (cached && cached.filas && cached.porParte) {
      _SAP_INDEX_MEMO_ = cached;
      return cached;
    }
  }

  const fresh = construirIndiceSapDesdeSheet_();
  cachePonerJSON_(CACHE_KEYS.SAP_INDEX, fresh, CACHE_TTL.SAP_INDEX);
  _SAP_INDEX_MEMO_ = fresh;
  return fresh;
}

function construirIndiceSapDesdeSheet_() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.DATA_SAP);
  if (!sheet || sheet.getLastRow() < 2) {
    return { filas: [], porParte: {}, porCodigo: {}, porReferencia: {}, refMeta: {},
             huerfanos: [], totalRefs: 0, ts: Date.now(), total: 0 };
  }
  const lastRow = sheet.getLastRow();
  const ancho = 8;
  const data = sheet.getRange(2, 1, lastRow - 1, ancho).getValues();
  const filas = [];
  const porParte = {};
  const porCodigo = {};
  for (let i = 0; i < data.length; i++) {
    const parte = (data[i][SAP_COL.NUMERO_PARTE] == null ? "" : String(data[i][SAP_COL.NUMERO_PARTE])).trim();
    const codigo = (data[i][SAP_COL.CODIGO] == null ? "" : String(data[i][SAP_COL.CODIGO])).trim();
    const descripcion = (data[i][SAP_COL.DESCRIPCION] == null ? "" : String(data[i][SAP_COL.DESCRIPCION])).trim();
    const ubicacion = (data[i][SAP_COL.UBICACION] == null ? "" : String(data[i][SAP_COL.UBICACION])).trim();

    if (!parte && !codigo && !descripcion) continue;

    const registro = [
      parte,
      codigo,
      descripcion,
      Number(data[i][SAP_COL.EXISTENCIA] || 0),
      ubicacion
    ];
    filas.push(registro);

    const claveParte = parte.toUpperCase();
    if (claveParte && !porParte[claveParte]) porParte[claveParte] = registro;

    const claveCodigo = codigo.toUpperCase();
    if (claveCodigo && !porCodigo[claveCodigo]) porCodigo[claveCodigo] = registro;
  }

  // ── Capa de referencias cruzadas ──────────────────────────────────
  // Carga la hoja REFERENCIAS_CRUZADAS y conecta los grupos al índice SAP.
  // Para cada grupo: si algún código del grupo está en DATA_SAP, todas las
  // demás referencias del grupo apuntan a ese mismo registro.
  // Si el grupo no tiene match en DATA_SAP → huérfano (se reporta).
  const refsLoaded = cargarReferenciasCruzadas_();
  const porReferencia = {};
  const refMeta = {}; // código_ref → { grupoId, codigosDelGrupo[], descripcionGrupo }
  const huerfanos = [];

  Object.keys(refsLoaded.porGrupo).forEach(function(grupoId) {
    const codigosGrupo = refsLoaded.porGrupo[grupoId];
    let registroRepresentante = null;
    let codigoRepresentante = "";

    // Busca el primer código del grupo que exista en DATA_SAP
    for (let j = 0; j < codigosGrupo.length; j++) {
      const c = codigosGrupo[j];
      if (porParte[c]) {
        registroRepresentante = porParte[c];
        codigoRepresentante = c;
        break;
      }
      if (porCodigo[c]) {
        registroRepresentante = porCodigo[c];
        codigoRepresentante = c;
        break;
      }
    }

    if (registroRepresentante) {
      // Mapear todos los demás códigos del grupo como referencias
      for (let j = 0; j < codigosGrupo.length; j++) {
        const c = codigosGrupo[j];
        if (!porParte[c] && !porCodigo[c]) {
          porReferencia[c] = registroRepresentante;
          refMeta[c] = {
            grupoId: grupoId,
            principal: codigoRepresentante,
            descripcionGrupo: refsLoaded.descGrupo[grupoId] || ""
          };
        }
      }
    } else {
      // Ningún código del grupo está en DATA_SAP
      huerfanos.push({
        grupoId: grupoId,
        codigos: codigosGrupo,
        descripcion: refsLoaded.descGrupo[grupoId] || ""
      });
    }
  });

  return {
    filas: filas,
    porParte: porParte,
    porCodigo: porCodigo,
    porReferencia: porReferencia,
    refMeta: refMeta,
    huerfanos: huerfanos,
    totalRefs: Object.keys(porReferencia).length,
    totalGrupos: Object.keys(refsLoaded.porGrupo).length,
    ts: Date.now(),
    total: filas.length
  };
}

/**
 * Lee la hoja REFERENCIAS_CRUZADAS y devuelve la estructura agrupada.
 * Tolerante: si la hoja no existe o está vacía, devuelve estructura vacía.
 */
function cargarReferenciasCruzadas_() {
  try {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.REFERENCIAS_CRUZADAS);
    if (!sheet || sheet.getLastRow() < 2) {
      return { porGrupo: {}, descGrupo: {} };
    }
    const lastRow = sheet.getLastRow();
    const data = sheet.getRange(2, 1, lastRow - 1, 3).getValues();
    const porGrupo = {};
    const descGrupo = {};
    for (let i = 0; i < data.length; i++) {
      const codigo = String(data[i][REFS_COLS.CODIGO] || "").trim().toUpperCase();
      const grupoId = String(data[i][REFS_COLS.GRUPO_ID] || "").trim();
      const desc = String(data[i][REFS_COLS.DESCRIPCION] || "").trim();
      if (!codigo || !grupoId) continue;
      if (!porGrupo[grupoId]) porGrupo[grupoId] = [];
      porGrupo[grupoId].push(codigo);
      if (desc && !descGrupo[grupoId]) descGrupo[grupoId] = desc;
    }
    return { porGrupo: porGrupo, descGrupo: descGrupo };
  } catch (e) {
    return { porGrupo: {}, descGrupo: {} };
  }
}

function invalidarIndiceSap() {
  _SAP_INDEX_MEMO_ = null;
  cacheInvalidar_(CACHE_KEYS.SAP_INDEX);
  cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);
}

/**
 * Refresca el índice y lo guarda en caché. Útil como trigger horario
 * o invocado por un AGENTE desde el frontend.
 */
function precalentarIndiceSap() {
  invalidarIndiceSap();
  return obtenerIndiceSap_(true);
}

/**
 * Endpoint público (requiere sesión) para reconstruir el índice.
 */
function refrescarIndiceSap(token) {
  return conSesion_(token, ROLES.AUXILIAR, function() {
    const idx = precalentarIndiceSap();
    return { exito: true, total: idx.total || 0, mensaje: "Índice actualizado: " + (idx.total || 0) + " partes" };
  });
}

function buscarEnTodos(token, valor) {
  return conSesion_(token, ROLES.AUXILIAR, function() {
    const v = (valor || "").toString().toUpperCase().trim();
    if (!v) return { exito: true, total: 0, resultados: [] };
    const idx = obtenerIndiceSap_(false);
    const max = LIMITES.RESULTADOS_BUSQUEDA;
    const resultados = buscarGlobal_(idx, v, max);
    return { exito: true, total: resultados.length, resultados: resultados };
  });
}

/**
 * Búsqueda por índice. Exacta O(1) para PARTE cuando valor matchea completo;
 * en el resto aplica substring O(n) en memoria (~15k filas ≈ 10ms en V8).
 *
 * Modo TODOS: busca el valor en parte, código, descripción y ubicación.
 * Devuelve registros únicos (deduplicados por parte+código).
 */
function buscarEnIndice_(tipo, valor, limite) {
  const idx = obtenerIndiceSap_(false);
  const v = (valor || "").toString().toUpperCase().trim();
  if (!v) return [];
  const max = Math.min(limite || LIMITES.RESULTADOS_BUSQUEDA, 500);

  if (tipo === "TODOS" || tipo === "TODO" || tipo === "") {
    return buscarGlobal_(idx, v, max);
  }

  const columnaMap = { PARTE: 0, ARTICULO: 1, DESCRIPCION: 2, UBICACION: 4 };
  const col = columnaMap[tipo];
  if (col == null) return buscarGlobal_(idx, v, max);

  // Atajos O(1) por match exacto
  if (tipo === "PARTE") {
    if (idx.porParte[v]) {
      return [empaquetarResultado_(idx.porParte[v], v, "PARTE", null)];
    }
    // Items sin "parte" en SAP (parte vacía): permitir búsqueda PARTE por código.
    // Patrón típico: FER100266 (sólo código, sin parte) → tratarlo como identificador principal.
    if ((idx.porCodigo || {})[v]) {
      return [empaquetarResultado_(idx.porCodigo[v], v, "CODIGO", null)];
    }
    if ((idx.porReferencia || {})[v]) {
      const meta = (idx.refMeta || {})[v] || null;
      return [empaquetarResultado_(idx.porReferencia[v], v, "REFERENCIA", meta)];
    }
  }
  if (tipo === "ARTICULO") {
    if ((idx.porCodigo || {})[v]) {
      return [empaquetarResultado_(idx.porCodigo[v], v, "CODIGO", null)];
    }
    if ((idx.porReferencia || {})[v]) {
      const meta = (idx.refMeta || {})[v] || null;
      return [empaquetarResultado_(idx.porReferencia[v], v, "REFERENCIA", meta)];
    }
  }

  const filas = idx.filas;
  const resultado = [];
  const yaIncluidos = {};

  for (let i = 0; i < filas.length; i++) {
    const cellPrimaria = String(filas[i][col] || "").toUpperCase();
    let match = cellPrimaria.indexOf(v) > -1;

    // Fallback PARTE: si el item no tiene parte (col 0 vacía), también acepta match por código.
    // Esto resuelve items como CEMENTO (FER100266) que sólo viven en col Código.
    if (!match && tipo === "PARTE" && !cellPrimaria) {
      const codigo = String(filas[i][1] || "").toUpperCase();
      match = codigo.indexOf(v) > -1;
    }

    if (match) {
      const clave = filas[i][0] + "|" + filas[i][1];
      if (yaIncluidos[clave]) continue;
      yaIncluidos[clave] = true;
      resultado.push(empaquetarResultado_(filas[i], v, tipo, null));
      if (resultado.length >= max) return resultado;
    }
  }

  // Para PARTE o ARTICULO también incluir matches en referencias cruzadas
  if ((tipo === "PARTE" || tipo === "ARTICULO") && idx.porReferencia) {
    const refKeys = Object.keys(idx.porReferencia);
    for (let i = 0; i < refKeys.length; i++) {
      const refCode = refKeys[i];
      if (refCode.indexOf(v) > -1) {
        const reg = idx.porReferencia[refCode];
        const clave = reg[0] + "|" + reg[1];
        if (yaIncluidos[clave]) continue;
        yaIncluidos[clave] = true;
        const meta = (idx.refMeta || {})[refCode] || null;
        resultado.push(empaquetarResultado_(reg, refCode, "REFERENCIA", meta));
        if (resultado.length >= max) return resultado;
      }
    }
  }

  return resultado;
}

function buscarGlobal_(idx, valor, max) {
  const filas = idx.filas;
  const resultado = [];
  const yaIncluidos = {};

  for (let i = 0; i < filas.length; i++) {
    const parte = String(filas[i][0] || "").toUpperCase();
    const codigo = String(filas[i][1] || "").toUpperCase();
    const desc = String(filas[i][2] || "").toUpperCase();
    const ubic = String(filas[i][4] || "").toUpperCase();
    if (parte.indexOf(valor) > -1 ||
        codigo.indexOf(valor) > -1 ||
        desc.indexOf(valor) > -1 ||
        ubic.indexOf(valor) > -1) {
      const clave = filas[i][0] + "|" + filas[i][1];
      if (yaIncluidos[clave]) continue;
      yaIncluidos[clave] = true;
      resultado.push(empaquetarResultado_(filas[i], valor, "TODOS", null));
      if (resultado.length >= max) return resultado;
    }
  }

  // Incluir también matches en referencias cruzadas
  if (idx.porReferencia) {
    const refKeys = Object.keys(idx.porReferencia);
    for (let i = 0; i < refKeys.length; i++) {
      const refCode = refKeys[i];
      if (refCode.indexOf(valor) > -1) {
        const reg = idx.porReferencia[refCode];
        const clave = reg[0] + "|" + reg[1];
        if (yaIncluidos[clave]) continue;
        yaIncluidos[clave] = true;
        const meta = (idx.refMeta || {})[refCode] || null;
        resultado.push(empaquetarResultado_(reg, refCode, "REFERENCIA", meta));
        if (resultado.length >= max) return resultado;
      }
    }
  }
  return resultado;
}

/**
 * Convierte un registro [parte, codigo, desc, stock, ubic] a un objeto
 * estructurado que el frontend puede mostrar con badges informativos.
 */
function empaquetarResultado_(reg, codigoEntrada, encontradoPor, refMeta) {
  return {
    parte: reg[0],
    codigo: reg[1],
    descripcion: reg[2],
    stock: reg[3],
    ubicacion: reg[4],
    codigoEntrada: codigoEntrada,
    encontradoPor: encontradoPor,
    referenciaUsada: encontradoPor === "REFERENCIA" ? codigoEntrada : null,
    grupoId: refMeta ? refMeta.grupoId : null
  };
}

function obtenerArticuloPorParte_(numeroParte) {
  return obtenerArticuloPorIdentificador_(numeroParte);
}

/**
 * Busca un artículo por número de parte, código de artículo, o referencia cruzada.
 * Devuelve metadata sobre cómo se encontró (PARTE, CODIGO o REFERENCIA).
 *
 * Si encontradoPor === 'REFERENCIA':
 *   - codigoEntrada: el código que el usuario escribió (FER100266)
 *   - parte/codigo: los códigos canónicos del SAP (60208480)
 *   - grupoId: ID del grupo de equivalencia
 */
function obtenerArticuloPorIdentificador_(id) {
  const idx = obtenerIndiceSap_(false);
  const clave = (id || "").toString().trim().toUpperCase();
  if (!clave) return null;

  // 1) Match directo por parte
  if (idx.porParte[clave]) {
    return mapearArticuloDesdeRegistro_(idx.porParte[clave], clave, "PARTE", null);
  }
  // 2) Match directo por código
  if ((idx.porCodigo || {})[clave]) {
    return mapearArticuloDesdeRegistro_(idx.porCodigo[clave], clave, "CODIGO", null);
  }
  // 3) Match vía referencia cruzada
  if ((idx.porReferencia || {})[clave]) {
    const meta = (idx.refMeta || {})[clave] || null;
    return mapearArticuloDesdeRegistro_(idx.porReferencia[clave], clave, "REFERENCIA", meta);
  }
  return null;
}

function mapearArticuloDesdeRegistro_(reg, codigoEntrada, encontradoPor, refMeta) {
  return {
    parte: reg[0],
    codigo: reg[1],
    descripcion: reg[2],
    stock: reg[3],
    ubicacion: reg[4],
    codigoEntrada: codigoEntrada,
    encontradoPor: encontradoPor,
    grupoId: refMeta ? refMeta.grupoId : null,
    referenciaUsada: encontradoPor === "REFERENCIA" ? codigoEntrada : null
  };
}

/**
 * Autocomplete: matches por prefijo en parte, código y referencias cruzadas.
 * Devuelve top N (default 12) sin duplicar el mismo artículo.
 */
function autocompletarParte(prefijo) {
  const p = (prefijo || "").toString().toUpperCase().trim();
  if (p.length < 2) return [];
  const idx = obtenerIndiceSap_(false);
  const filas = idx.filas;
  const max = LIMITES.RESULTADOS_AUTOCOMPLETE;
  const startsWith = [];
  const contains = [];
  const yaIncluidos = {};

  function pack(reg, encontradoPor, refUsada) {
    return {
      parte: reg[0],
      codigo: reg[1],
      descripcion: reg[2],
      stock: reg[3],
      ubicacion: reg[4],
      encontradoPor: encontradoPor,
      referenciaUsada: refUsada
    };
  }

  // 1) Filas SAP — prefix > contains
  for (let i = 0; i < filas.length; i++) {
    const parte = String(filas[i][0] || "").toUpperCase();
    const codigo = String(filas[i][1] || "").toUpperCase();
    const clave = parte + "|" + codigo;
    if (yaIncluidos[clave]) continue;

    if (parte.startsWith(p) || codigo.startsWith(p)) {
      yaIncluidos[clave] = true;
      startsWith.push(pack(filas[i], parte.startsWith(p) ? "PARTE" : "CODIGO", null));
      if (startsWith.length >= max) break;
    } else if (parte.indexOf(p) > -1 || codigo.indexOf(p) > -1) {
      if (contains.length < max) {
        yaIncluidos[clave] = true;
        contains.push(pack(filas[i], "PARTE", null));
      }
    }
  }

  // 2) Referencias cruzadas
  if (startsWith.length < max && idx.porReferencia) {
    const refKeys = Object.keys(idx.porReferencia);
    for (let i = 0; i < refKeys.length; i++) {
      const refCode = refKeys[i];
      const reg = idx.porReferencia[refCode];
      const clave = reg[0] + "|" + reg[1];
      if (yaIncluidos[clave]) continue;

      if (refCode.startsWith(p)) {
        yaIncluidos[clave] = true;
        startsWith.push(pack(reg, "REFERENCIA", refCode));
        if (startsWith.length >= max) break;
      } else if (contains.length < max && refCode.indexOf(p) > -1) {
        yaIncluidos[clave] = true;
        contains.push(pack(reg, "REFERENCIA", refCode));
      }
    }
  }

  return startsWith.concat(contains).slice(0, max);
}

function obtenerEstadoIndice() {
  const idx = obtenerIndiceSap_(false);
  return {
    total: idx.total || 0,
    totalRefs: idx.totalRefs || 0,
    totalGrupos: idx.totalGrupos || 0,
    huerfanos: (idx.huerfanos || []).length,
    ts: idx.ts || 0,
    antiguedadSegundos: idx.ts ? Math.round((Date.now() - idx.ts) / 1000) : null
  };
}

/**
 * Endpoint AGENTE: lista los grupos de referencias cruzadas que NO tienen
 * ningún código en DATA_SAP. Útil para limpiar datos y/o agregarlos a SAP.
 */
function obtenerReferenciasHuerfanas(token) {
  return conSesion_(token, ROLES.AGENTE, function() {
    const idx = obtenerIndiceSap_(false);
    return {
      exito: true,
      total: (idx.huerfanos || []).length,
      huerfanos: idx.huerfanos || []
    };
  });
}

