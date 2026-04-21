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
  if (sigRecibida !== sigCalculada) return null;

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

    const sheet = asegurarHojaUsuarios_();
    const fila = buscarFilaUsuario_(sheet, u);
    if (!fila) return { exito: false, mensaje: "Usuario o PIN incorrecto" };

    const activo = fila.data[USUARIOS_COLS.ACTIVO - 1] === true;
    if (!activo) return { exito: false, mensaje: "Usuario desactivado. Contacta a un agente." };

    const storedHash = String(fila.data[USUARIOS_COLS.PIN_HASH - 1] || "");
    const salt = String(fila.data[USUARIOS_COLS.SALT - 1] || "");
    const computedHash = hashearPin_(p.valor, salt);
    if (storedHash !== computedHash) {
      return { exito: false, mensaje: "Usuario o PIN incorrecto" };
    }

    const nombre = fila.data[USUARIOS_COLS.NOMBRE - 1] || u;
    const rol = fila.data[USUARIOS_COLS.ROL - 1] || ROLES.AUXILIAR;
    const token = generarToken_(u, rol);

    // Actualiza último acceso
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
  return conSesion_(token, ROLES.AUXILIAR, function(sesion) {
    const pa = validarFormatoPin_(pinActual);
    if (!pa.ok) return { exito: false, mensaje: "PIN actual: " + pa.mensaje };
    const pn = validarFormatoPin_(pinNuevo);
    if (!pn.ok) return { exito: false, mensaje: "PIN nuevo: " + pn.mensaje };
    if (pa.valor === pn.valor) return { exito: false, mensaje: "El PIN nuevo debe ser distinto al actual" };

    const sheet = asegurarHojaUsuarios_();
    const fila = buscarFilaUsuario_(sheet, sesion.usuario);
    if (!fila) return { exito: false, mensaje: "Usuario no encontrado" };

    const storedHash = String(fila.data[USUARIOS_COLS.PIN_HASH - 1] || "");
    const salt = String(fila.data[USUARIOS_COLS.SALT - 1] || "");
    if (hashearPin_(pa.valor, salt) !== storedHash) {
      return { exito: false, mensaje: "El PIN actual es incorrecto" };
    }

    const nuevoSalt = generarSalt_();
    const nuevoHash = hashearPin_(pn.valor, nuevoSalt);
    sheet.getRange(fila.rowNumber, USUARIOS_COLS.PIN_HASH).setValue(nuevoHash);
    sheet.getRange(fila.rowNumber, USUARIOS_COLS.SALT).setValue(nuevoSalt);

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
 * Verifica si el token sigue vivo. Útil en el arranque del frontend.
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
    token: generarToken_(sesion.usuario, fila.data[USUARIOS_COLS.ROL - 1] || ROLES.AUXILIAR)
  };
}
