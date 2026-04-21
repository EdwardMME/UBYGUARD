/**
 * UBYGUARD - Permisos y wrapper de sesión.
 *
 * Uso en cualquier función sensible:
 *   function miFuncion(token, arg1, arg2) {
 *     return conSesion_(token, ROLES.AUXILIAR, function(sesion) {
 *       // lógica. sesion = { usuario, rol, exp }
 *       return { exito: true, ... };
 *     });
 *   }
 *
 * conSesion_ se encarga de:
 *   - Verificar token + rol mínimo
 *   - Renovar token silenciosamente (exp += 24h)
 *   - Devolver `_token` en el resultado para que el frontend actualice su storage
 *   - Marcar `sesion_expirada` / `permiso_denegado` cuando aplique
 */

function requireRol_(token, rolMinimo) {
  const sesion = verificarToken_(token);
  if (!sesion) {
    const e = new Error("SESION_EXPIRADA");
    e._sesionExpirada = true;
    throw e;
  }
  if (rolMinimo) {
    const nivelRequerido = ROLES_JERARQUIA[rolMinimo] || 0;
    const nivelUsuario = ROLES_JERARQUIA[sesion.rol] || 0;
    if (nivelUsuario < nivelRequerido) {
      const e = new Error("PERMISO_DENEGADO");
      e._permisoDenegado = true;
      throw e;
    }
  }
  return sesion;
}

function conSesion_(token, rolMinimo, fn) {
  try {
    const sesion = requireRol_(token, rolMinimo);
    let resultado;
    try {
      resultado = fn(sesion);
    } catch (e) {
      return {
        exito: false,
        mensaje: "Error interno: " + (e && e.message ? e.message : e)
      };
    }

    // Renovar token silenciosamente
    const tokenRenovado = generarToken_(sesion.usuario, sesion.rol);

    // Empaquetar preservando el shape original del resultado
    if (resultado === null || resultado === undefined) {
      return { _token: tokenRenovado, data: null };
    }
    if (Array.isArray(resultado)) {
      return { _token: tokenRenovado, data: resultado };
    }
    if (typeof resultado === "object") {
      // Si ya es objeto, agregamos _token en vez de envolver (evita romper contratos)
      resultado._token = tokenRenovado;
      return resultado;
    }
    // Primitivos
    return { _token: tokenRenovado, data: resultado };
  } catch (e) {
    if (e._sesionExpirada) {
      return { exito: false, mensaje: "Sesión expirada. Inicia sesión de nuevo.", sesion_expirada: true };
    }
    if (e._permisoDenegado) {
      return { exito: false, mensaje: "No tienes permiso para esta acción.", permiso_denegado: true };
    }
    return { exito: false, mensaje: "Error de autenticación: " + (e && e.message ? e.message : e) };
  }
}

function esAgente_(rol) {
  return rol === ROLES.AGENTE;
}
