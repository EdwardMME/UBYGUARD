/**
 * UBYGUARD - Movimientos individuales.
 * Busca artículo por índice cacheado, valida input, registra en BASE_OPERATIVA
 * y audita el usuario ejecutor.
 */

function buscarArticulo(numeroParte) {
  try {
    return obtenerArticuloPorParte_(numeroParte);
  } catch (e) {
    return null;
  }
}

function registrarMovimiento(datos) {
  try {
    if (!datos) return { exito: false, mensaje: "Faltan datos" };

    const parte = validarTexto_(datos.parte, REGEX.NUMERO_PARTE, "número de parte");
    if (!parte.ok) return { exito: false, mensaje: parte.mensaje };

    const destino = validarTexto_(datos.destino, REGEX.UBICACION, "ubicación destino");
    if (!destino.ok) return { exito: false, mensaje: destino.mensaje };

    const responsable = validarTexto_(datos.responsable, null, "responsable");
    if (!responsable.ok) return { exito: false, mensaje: responsable.mensaje };

    const cantidad = validarCantidad_(datos.cantidad, "cantidad");
    if (!cantidad.ok) return { exito: false, mensaje: cantidad.mensaje };

    const articulo = obtenerArticuloPorParte_(parte.valor);
    if (!articulo) {
      return { exito: false, mensaje: "El número de parte no existe en DATA_SAP" };
    }

    const documento = normalizarTexto(datos.documento);
    const tipo = normalizarMayus(datos.tipo) || TIPO_MOVIMIENTO.INDIVIDUAL;

    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.BASE_OPERATIVA);
    if (!sheet) return { exito: false, mensaje: "No existe la hoja BASE_OPERATIVA" };

    const idMovimiento = generarID();

    // Ejecutado Por (col O) queda vacío aquí — lo llena el módulo de Ejecución SAP
    // cuando el responsable de SAP marca el movimiento.
    const fila = [
      idMovimiento,
      new Date(),
      tipo,
      documento,
      parte.valor,
      articulo.codigo || "",
      articulo.descripcion || "",
      cantidad.valor,
      articulo.ubicacion || "",
      destino.valor.toUpperCase(),
      responsable.valor,
      ESTADO_MOVIMIENTO.UBICADO,
      false,
      "",
      "",
      ""
    ];

    sheet.appendRow(fila);
    cacheInvalidarSimple_(CACHE_KEYS.RESUMEN_INICIO);

    return {
      exito: true,
      idMovimiento: idMovimiento
    };
  } catch (e) {
    return { exito: false, mensaje: "Error interno: " + (e && e.message ? e.message : e) };
  }
}
