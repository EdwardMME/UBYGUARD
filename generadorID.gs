/**
 * UBYGUARD - Generador único de IDs de movimiento.
 * Formato: MOV-YYYYMMDD-HHmmss-RND (6 dígitos random en base36).
 * No depende de getLastRow() → resistente a borrado/inserción de filas
 * y a concurrencia entre usuarios registrando el mismo segundo.
 */
function generarID() {
  const tz = Session.getScriptTimeZone();
  const fecha = Utilities.formatDate(new Date(), tz, "yyyyMMdd-HHmmss");
  const rnd = Math.floor(Math.random() * 0xFFFFFF).toString(36).toUpperCase().padStart(4, "0");
  return "MOV-" + fecha + "-" + rnd;
}
