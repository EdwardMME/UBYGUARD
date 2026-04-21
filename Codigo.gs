/**
 * UBYGUARD - Punto de entrada de la webapp.
 */
function doGet() {
  return HtmlService
    .createHtmlOutputFromFile("index")
    .setTitle("UBYGUARD")
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}
