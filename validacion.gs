/**
 * UBYGUARD - Validación y sanitización de input del frontend.
 * Protege contra: strings vacíos, longitudes excesivas, caracteres raros
 * que podrían romper fórmulas de Sheets o inyectar contenido.
 */

function validarTexto_(valor, regex, campo) {
  const v = (valor == null ? "" : String(valor)).trim();
  if (!v) {
    return { ok: false, mensaje: "El campo '" + campo + "' es requerido", valor: "" };
  }
  if (v.length > LIMITES.TEXTO_MAX) {
    return {
      ok: false,
      mensaje: "El campo '" + campo + "' excede " + LIMITES.TEXTO_MAX + " caracteres",
      valor: ""
    };
  }
  if (regex && !regex.test(v)) {
    return {
      ok: false,
      mensaje: "El campo '" + campo + "' contiene caracteres no permitidos",
      valor: ""
    };
  }
  return { ok: true, valor: v };
}

function validarCantidad_(valor, campo) {
  const n = Number(valor);
  if (!isFinite(n) || n <= 0) {
    return { ok: false, mensaje: "Cantidad inválida en '" + campo + "'", valor: 0 };
  }
  if (n > LIMITES.CANTIDAD_MAX) {
    return { ok: false, mensaje: "Cantidad excede el máximo permitido", valor: 0 };
  }
  return { ok: true, valor: Math.floor(n) };
}

function usuarioActual_() {
  try {
    const email = Session.getActiveUser().getEmail();
    if (email) return email;
    return Session.getEffectiveUser().getEmail() || "anonimo";
  } catch (e) {
    return "anonimo";
  }
}
