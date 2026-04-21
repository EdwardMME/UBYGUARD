/**
 * UBYGUARD - Movimientos individuales.
 * Busca por parte o código. Requiere sesión (AUXILIAR+).
 */

function buscarArticulo(token, identificador) {
  return conSesion_(token, ROLES.AUXILIAR, function() {
    const art = obtenerArticuloPorIdentificador_(identificador);
    return { exito: true, articulo: art };
  });
}

function registrarMovimiento(token, datos) {
  return conSesion_(token, ROLES.AUXILIAR, function(sesion) {
    if (!datos) return { exito: false, mensaje: "Faltan datos" };

    const parte = validarTexto_(datos.parte, REGEX.NUMERO_PARTE, "número de parte");
    if (!parte.ok) return { exito: false, mensaje: parte.mensaje };

    const destino = validarTexto_(datos.destino, REGEX.UBICACION, "ubicación destino");
    if (!destino.ok) return { exito: false, mensaje: destino.mensaje };

    const responsable = validarTexto_(datos.responsable, null, "responsable");
    if (!responsable.ok) return { exito: false, mensaje: responsable.mensaje };

    const cantidad = validarCantidad_(datos.cantidad, "cantidad");
    if (!cantidad.ok) return { exito: false, mensaje: cantidad.mensaje };

    const articulo = obtenerArticuloPorIdentificador_(parte.valor);
    if (!articulo) {
      return { exito: false, mensaje: "El número de parte o código no existe en DATA_SAP" };
    }

    const documento = normalizarTexto(datos.documento);
    const tipo = normalizarMayus(datos.tipo) || TIPO_MOVIMIENTO.INDIVIDUAL;

    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(HOJAS.BASE_OPERATIVA);
    if (!sheet) return { exito: false, mensaje: "No existe la hoja BASE_OPERATIVA" };

    const idMovimiento = generarID();

    const fila = [
      idMovimiento,
      new Date(),
      tipo,
      documento,
      articulo.parte || parte.valor,
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
      idMovimiento: idMovimiento,
      registradoPor: sesion.usuario
    };
  });
}
