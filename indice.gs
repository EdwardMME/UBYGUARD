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
    return { filas: [], porParte: {}, porCodigo: {}, ts: Date.now(), total: 0 };
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

    // Incluir fila si tiene AL MENOS uno de: parte, código o descripción.
    // (Antes: sólo si tenía parte → se perdían items como CEMENTO cuyo número
    //  de parte está vacío pero sí tienen código de artículo).
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
  return { filas: filas, porParte: porParte, porCodigo: porCodigo, ts: Date.now(), total: filas.length };
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
    const filas = idx.filas;
    const max = LIMITES.RESULTADOS_BUSQUEDA;
    const resultados = [];
    for (let i = 0; i < filas.length; i++) {
      const parte = String(filas[i][0] || "").toUpperCase();
      const codigo = String(filas[i][1] || "").toUpperCase();
      const desc = String(filas[i][2] || "").toUpperCase();
      const ubic = String(filas[i][4] || "").toUpperCase();
      const coincidencias = [];
      if (parte.indexOf(v) > -1) coincidencias.push("PARTE");
      if (codigo.indexOf(v) > -1) coincidencias.push("ARTICULO");
      if (desc.indexOf(v) > -1) coincidencias.push("DESCRIPCION");
      if (ubic.indexOf(v) > -1) coincidencias.push("UBICACION");
      if (coincidencias.length > 0) {
        resultados.push({ registro: filas[i], campos: coincidencias });
        if (resultados.length >= max) break;
      }
    }
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

  // Modo global: barre todas las columnas en una sola pasada
  if (tipo === "TODOS" || tipo === "TODO" || tipo === "") {
    return buscarGlobal_(idx, v, max);
  }

  const columnaMap = { PARTE: 0, ARTICULO: 1, DESCRIPCION: 2, UBICACION: 4 };
  const col = columnaMap[tipo];
  if (col == null) return buscarGlobal_(idx, v, max);

  // Atajo O(1): búsqueda exacta por parte
  if (tipo === "PARTE" && idx.porParte[v]) {
    return [idx.porParte[v]];
  }

  const filas = idx.filas;
  const resultado = [];
  for (let i = 0; i < filas.length; i++) {
    const cell = String(filas[i][col] || "").toUpperCase();
    if (cell.indexOf(v) > -1) {
      resultado.push(filas[i]);
      if (resultado.length >= max) break;
    }
  }
  return resultado;
}

function buscarGlobal_(idx, valor, max) {
  const filas = idx.filas;
  const resultado = [];
  for (let i = 0; i < filas.length; i++) {
    const parte = String(filas[i][0] || "").toUpperCase();
    const codigo = String(filas[i][1] || "").toUpperCase();
    const desc = String(filas[i][2] || "").toUpperCase();
    const ubic = String(filas[i][4] || "").toUpperCase();
    if (parte.indexOf(valor) > -1 ||
        codigo.indexOf(valor) > -1 ||
        desc.indexOf(valor) > -1 ||
        ubic.indexOf(valor) > -1) {
      resultado.push(filas[i]);
      if (resultado.length >= max) break;
    }
  }
  return resultado;
}

function obtenerArticuloPorParte_(numeroParte) {
  return obtenerArticuloPorIdentificador_(numeroParte);
}

/**
 * Busca un artículo por número de parte O código de artículo.
 * Permite trabajar con items que no tienen "parte" en DATA_SAP (como CEMENTO,
 * que sólo tiene código FER100266 en columna C).
 */
function obtenerArticuloPorIdentificador_(id) {
  const idx = obtenerIndiceSap_(false);
  const clave = (id || "").toString().trim().toUpperCase();
  if (!clave) return null;
  const reg = idx.porParte[clave] || (idx.porCodigo || {})[clave];
  if (!reg) return null;
  return {
    parte: reg[0],
    codigo: reg[1],
    descripcion: reg[2],
    stock: reg[3],
    ubicacion: reg[4]
  };
}

/**
 * Autocomplete para el frontend. Prioriza matches por prefijo, después
 * completa con substring. Devuelve top N (default 12).
 */
function autocompletarParte(prefijo) {
  const p = (prefijo || "").toString().toUpperCase().trim();
  if (p.length < 2) return [];
  const idx = obtenerIndiceSap_(false);
  const filas = idx.filas;
  const startsWith = [];
  const contains = [];
  const max = LIMITES.RESULTADOS_AUTOCOMPLETE;
  for (let i = 0; i < filas.length; i++) {
    const parte = String(filas[i][0] || "").toUpperCase();
    if (parte.startsWith(p)) {
      startsWith.push({
        parte: filas[i][0],
        codigo: filas[i][1],
        descripcion: filas[i][2],
        stock: filas[i][3],
        ubicacion: filas[i][4]
      });
      if (startsWith.length >= max) break;
    } else if (startsWith.length + contains.length < max * 2 && parte.indexOf(p) > -1) {
      contains.push({
        parte: filas[i][0],
        codigo: filas[i][1],
        descripcion: filas[i][2],
        stock: filas[i][3],
        ubicacion: filas[i][4]
      });
    }
  }
  return startsWith.concat(contains).slice(0, max);
}

function obtenerEstadoIndice() {
  const idx = obtenerIndiceSap_(false);
  return {
    total: idx.total || 0,
    ts: idx.ts || 0,
    antiguedadSegundos: idx.ts ? Math.round((Date.now() - idx.ts) / 1000) : null
  };
}

