/**
 * UBYGUARD - Cliente HTTP para la API SAP B1 (provista por Andre Lopez).
 *
 * Autenticación: header X-API-Key. La key vive en PropertiesService bajo
 * la propiedad UBY_SAP_API_KEY. NUNCA la pongas en código fuente — el
 * repo UBYGUARD es público.
 *
 * Endpoints disponibles (read-only, JSON):
 *   GET /api/ubyguard/quotations               · Cotizaciones abiertas en SPS0002
 *   GET /api/ubyguard/quotations/{docNum}      · Detalle de una cotización
 *   GET /api/ubyguard/items/{itemCode}/stock   · Stock real de un ítem
 *   GET /api/ubyguard/business-partners/{cardCode} · Datos del cliente
 *   GET /api/ubyguard/items                    · Listado masivo (sync diario)
 *
 * Documentación completa: ver PDF de Andre (no en repo, fuera del control de versiones).
 */

const SAP_API_BASE = "https://sap-api.eugeniachat.ai";
const SAP_WAREHOUSE = "SPS0002"; // Almacén Platino Motors SPS — confirmado por Andre
const SAP_PROP_KEY = "UBY_SAP_API_KEY";

// Feature flag: OT (work-orders) está en off hasta que Andre publique el endpoint.
// Cambiar a true ejecutando habilitarOT() desde el editor cuando Andre confirme.
// Calendario: 20/05/2026 build · 22/05/2026 listo para integrar (ref: docs/Respuesta-API-UBYGUARD).
const OT_ENABLED_PROP_KEY = "UBY_OT_ENABLED";

function sapOTHabilitado_() {
  return PropertiesService.getScriptProperties().getProperty(OT_ENABLED_PROP_KEY) === "true";
}

/**
 * Lee la API key desde PropertiesService. Lanza si no está configurada.
 * Para configurarla por primera vez, ejecuta sapConfigurarApiKey_() desde
 * el editor de Apps Script (no desde la webapp).
 */
function sapObtenerApiKey_() {
  const key = PropertiesService.getScriptProperties().getProperty(SAP_PROP_KEY);
  if (!key) {
    throw new Error(
      "API Key SAP no configurada. Ejecuta sapConfigurarApiKey_() " +
      "desde el editor de Apps Script (Run → sapConfigurarApiKey_) y pega la key cuando se solicite."
    );
  }
  return key;
}

/**
 * Wrapper HTTP genérico. Usado internamente por las funciones específicas.
 * - Retorna el JSON parseado
 * - Lanza Error con mensaje legible si la API responde no-2xx
 * - Maneja timeouts (UrlFetchApp default 30s)
 * - Loggea timestamp + endpoint para debug del lado de Andre si pide X-Request-ID
 */
function sapFetch_(path, options) {
  const opts = options || {};
  const url = SAP_API_BASE + path;
  const params = {
    method: opts.method || "get",
    muteHttpExceptions: true,
    headers: Object.assign({
      "X-API-Key": sapObtenerApiKey_(),
      "Accept": "application/json"
    }, opts.headers || {}),
    contentType: "application/json"
  };
  if (opts.payload) {
    params.payload = typeof opts.payload === "string" ? opts.payload : JSON.stringify(opts.payload);
  }

  const t0 = Date.now();
  const resp = UrlFetchApp.fetch(url, params);
  const status = resp.getResponseCode();
  const body = resp.getContentText();
  const elapsed = Date.now() - t0;

  console.log("[SAP] " + (params.method || "get").toUpperCase() + " " + path + " → " + status + " (" + elapsed + "ms)");

  if (status >= 200 && status < 300) {
    if (!body) return null;
    try { return JSON.parse(body); }
    catch (e) { throw new Error("Respuesta no-JSON de SAP API: " + body.substring(0, 200)); }
  }

  // Error → intenta parsear shape estándar { error, code }
  let errMsg = "HTTP " + status;
  try {
    const j = JSON.parse(body);
    if (j && j.error) errMsg = (j.code ? "[" + j.code + "] " : "") + j.error;
  } catch (e) {
    if (body) errMsg += ": " + body.substring(0, 200);
  }
  throw new Error("SAP API: " + errMsg);
}

// ============ Endpoints ============

/**
 * Lista cotizaciones abiertas con líneas en SPS0002.
 * @param {Object} filtros { status, dateFrom, limit, offset }
 */
function sapListarCotizaciones_(filtros) {
  const f = filtros || {};
  const qs = [];
  qs.push("warehouse=" + encodeURIComponent(f.warehouse || SAP_WAREHOUSE));
  if (f.status) qs.push("status=" + encodeURIComponent(f.status));
  if (f.dateFrom) qs.push("dateFrom=" + encodeURIComponent(f.dateFrom));
  if (f.limit) qs.push("limit=" + f.limit);
  if (f.offset) qs.push("offset=" + f.offset);
  return sapFetch_("/api/ubyguard/quotations?" + qs.join("&"));
}

/**
 * Detalle de una cotización por DocNum.
 */
function sapObtenerCotizacion_(docNum) {
  if (!docNum) throw new Error("docNum requerido");
  return sapFetch_("/api/ubyguard/quotations/" + encodeURIComponent(docNum));
}

/**
 * Stock + info de un ítem en SPS0002.
 * Devuelve {itemCode, itemName, partNumber, warehouseCode, onHand, committed, ordered, available, binCode}
 */
function sapObtenerStockItem_(itemCode) {
  if (!itemCode) throw new Error("itemCode requerido");
  return sapFetch_("/api/ubyguard/items/" + encodeURIComponent(itemCode) + "/stock");
}

/**
 * Datos del cliente (BusinessPartner).
 */
function sapObtenerCliente_(cardCode) {
  if (!cardCode) throw new Error("cardCode requerido");
  return sapFetch_("/api/ubyguard/business-partners/" + encodeURIComponent(cardCode));
}

/**
 * Lista Órdenes de Trabajo (OT) abiertas con líneas en SPS0002.
 * Endpoint PENDIENTE DE PUBLICACIÓN por Andre — ver docs/solicitud-endpoint-OT-andre.pdf
 * Mismo shape que /quotations + campos extra (docType, noOT, sucursal, imputacion, concepto).
 * @param {Object} filtros { status, dateFrom, dateTo, warehouse, limit, offset }
 */
function sapListarOT_(filtros) {
  const f = filtros || {};
  const qs = [];
  qs.push("warehouse=" + encodeURIComponent(f.warehouse || SAP_WAREHOUSE));
  if (f.status) qs.push("status=" + encodeURIComponent(f.status));
  if (f.dateFrom) qs.push("dateFrom=" + encodeURIComponent(f.dateFrom));
  if (f.dateTo) qs.push("dateTo=" + encodeURIComponent(f.dateTo));
  if (f.limit) qs.push("limit=" + f.limit);
  if (f.offset) qs.push("offset=" + f.offset);
  return sapFetch_("/api/ubyguard/work-orders?" + qs.join("&"));
}

/**
 * Listado masivo de items para sync diario de DATA_SAP.
 * @param {Object} filtros { limit (max 1000), offset, modifiedSince }
 */
function sapListarItems_(filtros) {
  const f = filtros || {};
  const qs = [];
  qs.push("warehouse=" + encodeURIComponent(f.warehouse || SAP_WAREHOUSE));
  qs.push("limit=" + (f.limit || 500));
  if (f.offset) qs.push("offset=" + f.offset);
  if (f.modifiedSince) qs.push("modifiedSince=" + encodeURIComponent(f.modifiedSince));
  return sapFetch_("/api/ubyguard/items?" + qs.join("&"));
}

// ============ Funciones administrativas (ejecutar desde editor) ============
// Nota: SIN underscore final → aparecen en el dropdown "Run" del editor.

/**
 * Setea la API key en Script Properties.
 * USO:
 *   1. Edita la línea API_KEY_TEMPORAL más abajo y pega tu key
 *   2. Run → sapConfigurarApiKey
 *   3. Borra la línea (volvé a poner "") y Ctrl+S
 */
function sapConfigurarApiKey() {
  // ⚠️ Pega la key acá temporalmente, corre la función, y luego borra la línea
  const API_KEY_TEMPORAL = "";

  if (!API_KEY_TEMPORAL || API_KEY_TEMPORAL.length < 20) {
    throw new Error("Edita esta función y pega la key en API_KEY_TEMPORAL antes de ejecutar.");
  }
  PropertiesService.getScriptProperties().setProperty(SAP_PROP_KEY, API_KEY_TEMPORAL);
  console.log("✅ API key SAP guardada en PropertiesService. Borra el literal de esta función.");
  return "OK — borra el literal API_KEY_TEMPORAL antes de pushear de nuevo.";
}

/**
 * Verifica si la API key está configurada (sin revelarla).
 */
function sapVerificarApiKey() {
  const k = PropertiesService.getScriptProperties().getProperty(SAP_PROP_KEY);
  if (!k) {
    console.log("❌ NO configurada. Corre sapConfigurarApiKey primero.");
    return "❌ NO configurada";
  }
  const msg = "✅ Configurada (longitud: " + k.length + " chars, prefijo: " + k.substring(0, 8) + "...)";
  console.log(msg);
  return msg;
}

/**
 * Habilita el feature OT (botón Sync OT visible en UI + permite llamadas al endpoint).
 * Ejecutar SOLO cuando Andre confirme que /api/ubyguard/work-orders está vivo.
 * Run → habilitarOT desde el editor.
 */
function habilitarOT() {
  PropertiesService.getScriptProperties().setProperty(OT_ENABLED_PROP_KEY, "true");
  console.log("✅ OT habilitado. Refresca la webapp y el botón Sync OT aparecerá.");
  return "OT habilitado.";
}

/**
 * Deshabilita OT (oculta botón Sync OT). Útil si Andre debe pausar el endpoint.
 */
function deshabilitarOT() {
  PropertiesService.getScriptProperties().deleteProperty(OT_ENABLED_PROP_KEY);
  console.log("OT deshabilitado.");
  return "OT deshabilitado.";
}

/**
 * Borra la API key de Script Properties (rotación / emergencia).
 */
function sapBorrarApiKey() {
  PropertiesService.getScriptProperties().deleteProperty(SAP_PROP_KEY);
  console.log("API key borrada.");
  return "API key borrada.";
}

// ============ Smoke tests (ejecutar desde editor) ============

/**
 * Prueba mínima: pide stock de RSA101592 (item conocido del PDF de Andre).
 * Run → sapSmokeTest
 */
function sapSmokeTest() {
  const ITEM_PRUEBA = "RSA101592";
  console.log("→ Probando GET /items/" + ITEM_PRUEBA + "/stock");
  const r = sapObtenerStockItem_(ITEM_PRUEBA);
  console.log("✅ Respuesta:", JSON.stringify(r, null, 2));
  return r;
}

/**
 * Verifica el endpoint de cotizaciones (limit=1).
 * Run → sapSmokeTestCotizaciones
 */
function sapSmokeTestCotizaciones() {
  console.log("→ Probando GET /quotations?status=open&limit=1");
  const r = sapListarCotizaciones_({ status: "open", limit: 1 });
  const total = r && r.meta ? r.meta.total : "?";
  const primera = r && r.data && r.data[0] ? r.data[0].docNum : "(ninguna)";
  console.log("✅ Total cotizaciones abiertas: " + total + " · primera DocNum: " + primera);
  return { total: total, primera: primera, sample: r };
}
