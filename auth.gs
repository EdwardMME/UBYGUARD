/**
 * UBYGUARD - Autenticación (login, PIN, tokens firmados).
 *
 * Modelo: JWT-lite propio (base64 payload + HMAC-SHA256).
 * El secreto vive en Script Properties — ni el cliente ni las hojas lo ven.
 * PIN se guarda como SHA-256(pin + salt) con salt único por usuario.
 * Sesiones de 24h, renovadas silenciosamente en cada RPC vía permisos.gs.
 */

// ============ Helpers de PIN ============

function generarSalt_() {
  return Utilities.getUuid();
}

function hashearPin_(pin, salt) {
  const input = String(pin == null ? "" : pin) + "|" + String(salt || "");
  const bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, input);
  return Utilities.base64EncodeWebSafe(bytes);
}

function validarFormatoPin_(pin) {
  const p = String(pin || "");
  if (!/^\d{4}$/.test(p)) {
    return { ok: false, mensaje: "El PIN debe ser exactamente 4 dígitos" };
  }
  return { ok: true, valor: p };
}

// ============ Helpers de token ============

function getSecretoHMAC_() {
  const props = PropertiesService.getScriptProperties();
  let secret = props.getProperty("UBY_HMAC_SECRET");
  if (!secret) {
    secret = Utilities.getUuid() + "-" + Utilities.getUuid();
    props.setProperty("UBY_HMAC_SECRET", secret);
  }
  return secret;
}

function generarToken_(usuario, rol) {
  const payload = {
    u: usuario,
    r: rol,
    exp: Date.now() + SESION_DURACION_MS
  };
  const payloadJson = JSON.stringify(payload);
  const payloadB64 = Utilities.base64EncodeWebSafe(Utilities.newBlob(payloadJson).getBytes());
  const sigBytes = Utilities.computeHmacSha256Signature(payloadB64, getSecretoHMAC_());
  const sigB64 = Utilities.base64EncodeWebSafe(sigBytes);
  return payloadB64 + "." + sigB64;
}

function verificarToken_(token) {
  if (!token || typeof token !== "string" || token.indexOf(".") < 0) return null;
  const partes = token.split(".");
  if (partes.length !== 2) return null;
  const payloadB64 = partes[0];
  const sigRecibida = partes[1];
  const sigBytes = Utilities.computeHmacSha256Signature(payloadB64, getSecretoHMAC_());
  const sigCalculada = Utilities.base64EncodeWebSafe(sigBytes);
  // Constant-time compare evita timing attacks que permitirían forjar firma byte-a-byte
  if (!compararConstanteTime_(sigRecibida, sigCalculada)) return null;

  let payload;
  try {
    const bytes = Utilities.base64DecodeWebSafe(payloadB64);
    const json = Utilities.newBlob(bytes).getDataAsString();
    payload = JSON.parse(json);
  } catch (e) { return null; }

  if (!payload || !payload.u || !payload.r || !payload.exp) return null;
  if (Date.now() > payload.exp) return null;

  return { usuario: payload.u, rol: payload.r, exp: payload.exp };
}

// ============ Rate limit anti brute-force ============
// Estrategia híbrida: Cache (fast-path TTL 900s) + ScriptProperties (persistente).
// Si Cache se evicta bajo presión, ScriptProperties mantiene la fuente de verdad.
// Counter por usuario normalizado (mismo lowercase+trim que el login).
// Cuenta también fallos de usuarios inexistentes (evita timing-based enumeration
// y bloquea sondeos masivos).

const RATE_LIMIT_MAX_INTENTOS = 5;
const RATE_LIMIT_VENTANA_MS = 15 * 60 * 1000; // 15 min

function rateLimitKey_(prefijo, usuario) {
  const u = normalizarTexto(usuario).toLowerCase();
  return prefijo + ":" + u;
}

/**
 * Lee el estado de rate limit. Retorna { intentos, bloqueadoHasta } o null si no hay registro.
 * Cache primero (rápido), fallback a ScriptProperties (persistente).
 */
function rateLimitEstado_(key) {
  const cache = CacheService.getScriptCache();
  const cached = cache.get(key);
  if (cached) {
    try {
      const obj = JSON.parse(cached);
      if (obj && typeof obj.intentos === "number") return obj;
    } catch (e) {}
  }
  const stored = PropertiesService.getScriptProperties().getProperty(key);
  if (!stored) return null;
  try {
    const obj = JSON.parse(stored);
    // Si ya pasó la ventana, considera limpio
    if (obj && obj.expira && Date.now() > obj.expira) {
      rateLimitLimpiar_(key);
      return null;
    }
    return obj;
  } catch (e) { return null; }
}

function rateLimitRegistrarFallo_(key) {
  const estado = rateLimitEstado_(key) || { intentos: 0 };
  estado.intentos = (estado.intentos || 0) + 1;
  estado.expira = Date.now() + RATE_LIMIT_VENTANA_MS;
  const json = JSON.stringify(estado);
  CacheService.getScriptCache().put(key, json, RATE_LIMIT_VENTANA_MS / 1000);
  PropertiesService.getScriptProperties().setProperty(key, json);
  return estado;
}

function rateLimitLimpiar_(key) {
  try { CacheService.getScriptCache().remove(key); } catch (e) {}
  try { PropertiesService.getScriptProperties().deleteProperty(key); } catch (e) {}
}

function rateLimitMinutosRestantes_(estado) {
  if (!estado || !estado.expira) return 0;
  const ms = estado.expira - Date.now();
  return Math.max(1, Math.ceil(ms / 60000));
}

// ============ Endpoints públicos ============

/**
 * Login con usuario + PIN. Devuelve token + info de sesión.
 * Única función pública (sin requireRol_).
 */
function loginUsuario(usuario, pin) {
  try {
    const u = normalizarTexto(usuario).toLowerCase();
    if (!u) return { exito: false, mensaje: "Ingresa usuario" };
    const p = validarFormatoPin_(pin);
    if (!p.ok) return { exito: false, mensaje: p.mensaje };

    // Rate limit: si ya superó intentos en la ventana, bloquea silenciosamente
    // (contamos también fallos de usuario inexistente para evitar enumeration).
    const rlKey = rateLimitKey_("login_fail", u);
    const rlEstado = rateLimitEstado_(rlKey);
    if (rlEstado && rlEstado.intentos >= RATE_LIMIT_MAX_INTENTOS) {
      const min = rateLimitMinutosRestantes_(rlEstado);
      return { exito: false, mensaje: "Demasiados intentos fallidos. Reintenta en " + min + " min." };
    }

    const sheet = asegurarHojaUsuarios_();
    const fila = buscarFilaUsuario_(sheet, u);

    // Usuario inexistente: contar el fallo igual (evita timing-based enumeration)
    if (!fila) {
      rateLimitRegistrarFallo_(rlKey);
      // Pausa simbólica para igualar timing con path de PIN mal
      hashearPin_(p.valor, "dummy-salt-for-timing");
      return { exito: false, mensaje: "Usuario o PIN incorrecto" };
    }

    const activo = fila.data[USUARIOS_COLS.ACTIVO - 1] === true;
    if (!activo) {
      rateLimitRegistrarFallo_(rlKey);
      return { exito: false, mensaje: "Usuario desactivado. Contacta a un agente." };
    }

    const storedHash = String(fila.data[USUARIOS_COLS.PIN_HASH - 1] || "");
    const salt = String(fila.data[USUARIOS_COLS.SALT - 1] || "");
    const computedHash = hashearPin_(p.valor, salt);
    if (!compararConstanteTime_(storedHash, computedHash)) {
      rateLimitRegistrarFallo_(rlKey);
      return { exito: false, mensaje: "Usuario o PIN incorrecto" };
    }

    // Login exitoso → limpia el contador
    rateLimitLimpiar_(rlKey);

    const nombre = fila.data[USUARIOS_COLS.NOMBRE - 1] || u;
    const rol = fila.data[USUARIOS_COLS.ROL - 1] || ROLES.AUXILIAR;
    const token = generarToken_(u, rol);

    sheet.getRange(fila.rowNumber, USUARIOS_COLS.ULTIMO_ACCESO).setValue(new Date());

    return {
      exito: true,
      token: token,
      usuario: u,
      nombre: nombre,
      rol: rol,
      expira: Date.now() + SESION_DURACION_MS
    };
  } catch (e) {
    return { exito: false, mensaje: "Error interno: " + (e && e.message ? e.message : e) };
  }
}

/**
 * Cambia mi propio PIN. Requiere PIN actual.
 */
function cambiarMiPin(token, pinActual, pinNuevo) {
  return conSesion_(token, ROLES.COMERCIAL, function(sesion) {
    const pa = validarFormatoPin_(pinActual);
    if (!pa.ok) return { exito: false, mensaje: "PIN actual: " + pa.mensaje };
    const pn = validarFormatoPin_(pinNuevo);
    if (!pn.ok) return { exito: false, mensaje: "PIN nuevo: " + pn.mensaje };
    if (pa.valor === pn.valor) return { exito: false, mensaje: "El PIN nuevo debe ser distinto al actual" };

    // Rate limit independiente: una sesión robada NO debe poder brute-forcear el PIN actual
    const rlKey = rateLimitKey_("change_pin_fail", sesion.usuario);
    const rlEstado = rateLimitEstado_(rlKey);
    if (rlEstado && rlEstado.intentos >= RATE_LIMIT_MAX_INTENTOS) {
      const min = rateLimitMinutosRestantes_(rlEstado);
      return { exito: false, mensaje: "Demasiados intentos fallidos. Reintenta en " + min + " min." };
    }

    const sheet = asegurarHojaUsuarios_();
    const fila = buscarFilaUsuario_(sheet, sesion.usuario);
    if (!fila) return { exito: false, mensaje: "Usuario no encontrado" };

    const storedHash = String(fila.data[USUARIOS_COLS.PIN_HASH - 1] || "");
    const salt = String(fila.data[USUARIOS_COLS.SALT - 1] || "");
    if (!compararConstanteTime_(hashearPin_(pa.valor, salt), storedHash)) {
      rateLimitRegistrarFallo_(rlKey);
      return { exito: false, mensaje: "El PIN actual es incorrecto" };
    }

    const nuevoSalt = generarSalt_();
    const nuevoHash = hashearPin_(pn.valor, nuevoSalt);
    sheet.getRange(fila.rowNumber, USUARIOS_COLS.PIN_HASH).setValue(nuevoHash);
    sheet.getRange(fila.rowNumber, USUARIOS_COLS.SALT).setValue(nuevoSalt);
    rateLimitLimpiar_(rlKey);

    return { exito: true, mensaje: "PIN actualizado" };
  });
}

/**
 * Cierra la sesión del cliente. No hacemos nada server-side (stateless),
 * pero permitimos al cliente notificar. Útil para logs futuros.
 */
function logoutUsuario(token) {
  // Noop server-side: el cliente borra su token.
  return { exito: true };
}

/**
 * Lista de usuarios activos para el dropdown de login.
 * Público — no requiere sesión. Solo expone usuario + nombre (no PIN, salt, ni rol).
 */
function obtenerUsuariosLogin() {
  try {
    const sheet = asegurarHojaUsuarios_();
    if (sheet.getLastRow() < 2) return [];
    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, USUARIOS_HEADERS.length).getValues();
    return data
      .filter(r => r[USUARIOS_COLS.ACTIVO - 1] === true)
      .map(r => ({
        usuario: String(r[USUARIOS_COLS.USUARIO - 1] || ""),
        nombre: String(r[USUARIOS_COLS.NOMBRE - 1] || "")
      }))
      .filter(u => u.usuario)
      .sort((a, b) => (a.nombre || a.usuario).localeCompare(b.nombre || b.usuario));
  } catch (e) {
    return [];
  }
}

/**
 * Devuelve los feature flags al frontend para que ajuste la UI.
 * Hoy expone: `ot` (true si Andre habilitó el endpoint /work-orders).
 * No requiere token — los flags son globales y no leak info sensible.
 */
function obtenerFeatureFlags() {
  return {
    ot: sapOTHabilitado_()
  };
}

/**
 * Verifica si el token sigue vivo. Útil en el arranque del frontend.
 * NO renueva el token (eso lo hace conSesion_ por RPC con actividad real).
 * Si renovara aquí, un token filtrado podría sostenerse indefinidamente por polling.
 */
function validarMiSesion(token) {
  const sesion = verificarToken_(token);
  if (!sesion) return { valid: false };
  const sheet = asegurarHojaUsuarios_();
  const fila = buscarFilaUsuario_(sheet, sesion.usuario);
  if (!fila) return { valid: false };
  if (fila.data[USUARIOS_COLS.ACTIVO - 1] !== true) return { valid: false };
  return {
    valid: true,
    usuario: sesion.usuario,
    nombre: fila.data[USUARIOS_COLS.NOMBRE - 1] || sesion.usuario,
    rol: fila.data[USUARIOS_COLS.ROL - 1] || ROLES.AUXILIAR,
    exp: sesion.exp
  };
}
