/**
 * UBYGUARD - Trigger onEdit.
 * Responsabilidades:
 *   1. Rellenar columna N (fecha ejecución) cuando se marca el checkbox SAP.
 *   2. Invalidar el índice cacheado de DATA_SAP cuando esa hoja cambia.
 *   3. Invalidar el resumen del dashboard cuando BASE_OPERATIVA o TRABAJO_MASIVO
 *      se editan manualmente en la hoja.
 */

function onEdit(e) {
  if (!e || !e.source) return;
  const hoja = e.source.getActiveSheet();
  const nombreHoja = hoja.getName();

  // 1) Edición en DATA_SAP → índice desactualizado
  if (nombreHoja === HOJAS.DATA_SAP) {
    try { invalidarIndiceSap(); } catch (err) {}
    return;
  }

  // 2) Cambios que afectan al dashboard
  if (nombreHoja === HOJAS.BASE_OPERATIVA || nombreHoja === HOJAS.TRABAJO_MASIVO) {
    try { cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO); } catch (err) {}
  }

  if (nombreHoja === HOJAS.LISTAS_CONTROL) {
    try { invalidarResponsables(); } catch (err) {}
    return;
  }

  // 3) Lógica checkbox SAP - solo BASE_OPERATIVA
  if (nombreHoja !== HOJAS.BASE_OPERATIVA) return;

  const rango = e.range;
  const col = rango.getColumn();
  const row = rango.getRow();

  if (col !== 13) return;
  if (row < 10) return;

  const celdaFecha = hoja.getRange(row, 14);

  if (e.value === "TRUE") {
    if (!celdaFecha.getValue()) {
      celdaFecha.setValue(new Date());
    }
  }

  if (e.value === "FALSE") {
    celdaFecha.clearContent();
  }
}
