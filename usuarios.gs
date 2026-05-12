/**
 * UBYGUARD - CRUD de usuarios. Solo AGENTE.
 * Crea/mantiene la hoja USUARIOS y siembra el usuario bootstrap
 * (definido en constantes.gs → USUARIO_BOOTSTRAP) la primera vez.
 */

function asegurarHojaUsuarios_() {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = spreadsheet.getSheetByName(HOJAS.USUARIOS);

  if (!sheet) {
    sheet = spreadsheet.insertSheet(HOJAS.USUARIOS);
  }

  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, USUARIOS_HEADERS.length).setValues([USUARIOS_HEADERS]);
    sheet.setFrozenRows(1);
  } else {
    const headers = sheet.getRange(1, 1, 1, USUARIOS_HEADERS.length).getValues()[0];
    if (headers.join("|") !== USUARIOS_HEADERS.join("|")) {
      sheet.getRange(1, 1, 1, USUARIOS_HEADERS.length).setValues([USUARIOS_HEADERS]);
      sheet.setFrozenRows(1);
    }
  }

  // Seed del primer usuario si la hoja está vacía
  if (sheet.getLastRow() === 1) {
    sembrarUsuarioBootstrap_(sheet);
  }

  return sheet;
}

function sembrarUsuarioBootstrap_(sheet) {
  const b = USUARIO_BOOTSTRAP;
  const pinFromProps = PropertiesService.getScriptProperties().getProperty(BOOTSTRAP_PROP_KEY);
  if (!pinFromProps || !/^\d{4}$/.test(pinFromProps)) {
    console.warn(
      "[UBY] Bootstrap NO sembrado: falta Script Property '" + BOOTSTRAP_PROP_KEY + "' (4 dígitos). " +
      "Ejecuta configurarBootstrap_ desde el editor para configurarlo."
    );
    return false;
  }
  const salt = generarSalt_();
  const hash = hashearPin_(pinFromProps, salt);
  sheet.appendRow([
    escaparFormula_(b.usuario),
    escaparFormula_(b.nombre),
    hash,
    salt,
    b.rol,
    true,
    "",
    new Date(),
    "sistema"
  ]);
  return true;
}

/**
 * Función admin · configura el PIN del usuario bootstrap.
 * EJECUTAR SOLO DESDE EL EDITOR DE APPS SCRIPT (no via google.script.run).
 * El underscore final la hace privada — no callable desde la webapp.
 *
 * USO:
 *   1. Edita la línea PIN_TEMPORAL más abajo y pega un PIN de 4 dígitos
 *   2. Run → configurarBootstrap_
 *   3. Borra el literal (volvé a poner "") y Ctrl+S
 */
function configurarBootstrap_() {
  const PIN_TEMPORAL = ""; // ← pega aquí el PIN, corre, y bórralo

  if (!PIN_TEMPORAL || !/^\d{4}$/.test(PIN_TEMPORAL)) {
    throw new Error(
      "Edita configurarBootstrap_ y pega un PIN de 4 dígitos en PIN_TEMPORAL antes de ejecutar."
    );
  }
  PropertiesService.getScriptProperties().setProperty(BOOTSTRAP_PROP_KEY, PIN_TEMPORAL);
  console.log(
    "✅ PIN bootstrap configurado en Script Properties. Borra el literal PIN_TEMPORAL de esta función antes de pushear de nuevo."
  );
  return "OK — borra el literal antes de pushear.";
}

function buscarFilaUsuario_(sheet, usuario) {
  const u = normalizarTexto(usuario).toLowerCase();
  if (!u) return null;
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return null;
  const data = sheet.getRange(2, 1, lastRow - 1, USUARIOS_HEADERS.length).getValues();
  for (let i = 0; i < data.length; i++) {
    const fila = normalizarTexto(data[i][USUARIOS_COLS.USUARIO - 1]).toLowerCase();
    if (fila === u) {
      return { rowNumber: i + 2, data: data[i] };
    }
  }
  return null;
}

function mapearUsuarioFila_(fila, timeZone) {
  return {
    usuario: normalizarTexto(fila[USUARIOS_COLS.USUARIO - 1]),
    nombre: normalizarTexto(fila[USUARIOS_COLS.NOMBRE - 1]),
    rol: normalizarTexto(fila[USUARIOS_COLS.ROL - 1]) || ROLES.AUXILIAR,
    activo: fila[USUARIOS_COLS.ACTIVO - 1] === true,
    ultimoAcceso: formatearFechaUsuario_(fila[USUARIOS_COLS.ULTIMO_ACCESO - 1], timeZone),
    fechaCreacion: formatearFechaUsuario_(fila[USUARIOS_COLS.FECHA_CREACION - 1], timeZone),
    creadoPor: normalizarTexto(fila[USUARIOS_COLS.CREADO_POR - 1])
  };
}

function formatearFechaUsuario_(valor, timeZone) {
  if (Object.prototype.toString.call(valor) === "[object Date]" && !isNaN(valor)) {
    return Utilities.formatDate(valor, timeZone, "dd/MM/yyyy HH:mm");
  }
  return "";
}

// ============ Endpoints públicos (solo AGENTE) ============

function obtenerUsuarios(token) {
  return conSesion_(token, ROLES.AGENTE, function(sesion) {
    const sheet = asegurarHojaUsuarios_();
    if (sheet.getLastRow() < 2) return { exito: true, usuarios: [] };
    const data = sheet
      .getRange(2, 1, sheet.getLastRow() - 1, USUARIOS_HEADERS.length)
      .getValues();
    const tz = Session.getScriptTimeZone();
    const usuarios = data
      .filter(f => normalizarTexto(f[USUARIOS_COLS.USUARIO - 1]))
      .map(f => mapearUsuarioFila_(f, tz));
    return { exito: true, usuarios: usuarios };
  });
}

function crearUsuario(token, datos) {
  return conSesion_(token, ROLES.AGENTE, function(sesion) {
    if (!datos) return { exito: false, mensaje: "Faltan datos" };
    const u = normalizarTexto(datos.usuario).toLowerCase();
    if (!/^[a-z0-9_\-\.]{3,30}$/.test(u)) {
      return { exito: false, mensaje: "Usuario: 3-30 caracteres, solo letras/números/_/-/." };
    }
    const n = normalizarTexto(datos.nombre);
    if (!n) return { exito: false, mensaje: "El nombre es requerido" };
    if (n.length > 60) return { exito: false, mensaje: "Nombre demasiado largo" };

    const p = validarFormatoPin_(datos.pin);
    if (!p.ok) return { exito: false, mensaje: p.mensaje };

    const rol = normalizarTexto(datos.rol).toUpperCase();
    if (rol !== ROLES.AGENTE && rol !== ROLES.AUXILIAR && rol !== ROLES.COMERCIAL) {
      return { exito: false, mensaje: "Rol inválido. Usa AGENTE, AUXILIAR o COMERCIAL." };
    }

    const lock = LockService.getScriptLock();
    try {
      lock.waitLock(10000);
      const sheet = asegurarHojaUsuarios_();
      if (buscarFilaUsuario_(sheet, u)) {
        return { exito: false, mensaje: "Ya existe un usuario con ese identificador" };
      }
      const salt = generarSalt_();
      const hash = hashearPin_(p.valor, salt);
      sheet.appendRow([
        escaparFormula_(u),
        escaparFormula_(n),
        hash,
        salt,
        rol,
        true,
        "",
        new Date(),
        escaparFormula_(sesion.usuario)
      ]);
      return { exito: true, mensaje: "Usuario creado" };
    } finally {
      try { lock.releaseLock(); } catch (e) {}
    }
  });
}

function actualizarUsuario(token, usuarioTarget, cambios) {
  return conSesion_(token, ROLES.AGENTE, function(sesion) {
    if (!cambios) return { exito: false, mensaje: "Sin cambios" };
    const sheet = asegurarHojaUsuarios_();
    const fila = buscarFilaUsuario_(sheet, usuarioTarget);
    if (!fila) return { exito: false, mensaje: "Usuario no encontrado" };

    if (cambios.nombre !== undefined) {
      const n = normalizarTexto(cambios.nombre);
      if (!n || n.length > 60) return { exito: false, mensaje: "Nombre inválido" };
      sheet.getRange(fila.rowNumber, USUARIOS_COLS.NOMBRE).setValue(escaparFormula_(n));
    }
    if (cambios.rol !== undefined) {
      const r = normalizarTexto(cambios.rol).toUpperCase();
      if (r !== ROLES.AGENTE && r !== ROLES.AUXILIAR && r !== ROLES.COMERCIAL) {
        return { exito: false, mensaje: "Rol inválido" };
      }
      // Prevenir auto-degradación del último AGENTE
      if (sesion.usuario === normalizarTexto(usuarioTarget).toLowerCase() && r !== ROLES.AGENTE) {
        if (contarAgentesActivos_(sheet) <= 1) {
          return { exito: false, mensaje: "No puedes degradarte: eres el último AGENTE activo" };
        }
      }
      sheet.getRange(fila.rowNumber, USUARIOS_COLS.ROL).setValue(r);
    }
    if (cambios.activo !== undefined) {
      const a = cambios.activo === true;
      // No permitir desactivarse a uno mismo si es el último AGENTE
      if (!a && sesion.usuario === normalizarTexto(usuarioTarget).toLowerCase()) {
        if (contarAgentesActivos_(sheet) <= 1) {
          return { exito: false, mensaje: "No puedes desactivarte: eres el último AGENTE activo" };
        }
      }
      sheet.getRange(fila.rowNumber, USUARIOS_COLS.ACTIVO).setValue(a);
    }

    return { exito: true, mensaje: "Usuario actualizado" };
  });
}

function resetearPin(token, usuarioTarget, pinNuevo) {
  return conSesion_(token, ROLES.AGENTE, function(sesion) {
    const p = validarFormatoPin_(pinNuevo);
    if (!p.ok) return { exito: false, mensaje: p.mensaje };
    const sheet = asegurarHojaUsuarios_();
    const fila = buscarFilaUsuario_(sheet, usuarioTarget);
    if (!fila) return { exito: false, mensaje: "Usuario no encontrado" };
    const salt = generarSalt_();
    const hash = hashearPin_(p.valor, salt);
    sheet.getRange(fila.rowNumber, USUARIOS_COLS.PIN_HASH).setValue(hash);
    sheet.getRange(fila.rowNumber, USUARIOS_COLS.SALT).setValue(salt);
    return { exito: true, mensaje: "PIN reseteado para " + fila.data[USUARIOS_COLS.NOMBRE - 1] };
  });
}

function contarAgentesActivos_(sheet) {
  if (sheet.getLastRow() < 2) return 0;
  const data = sheet
    .getRange(2, 1, sheet.getLastRow() - 1, USUARIOS_HEADERS.length)
    .getValues();
  let n = 0;
  for (let i = 0; i < data.length; i++) {
    if (data[i][USUARIOS_COLS.ROL - 1] === ROLES.AGENTE && data[i][USUARIOS_COLS.ACTIVO - 1] === true) {
      n++;
    }
  }
  return n;
}
