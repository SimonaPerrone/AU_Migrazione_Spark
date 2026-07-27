package it.au.misure.ee_switching.utility

object Constants {
  val INPUT_TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss"
  val FUNZIONALI = "funzionali"
  val STORICI = "storici"
  val FILENAME_TIMESTAMP_PATTERN = "yyyyMMddHHmmss"
  val ITALIAN_DATE_PATTERN = "dd/MM/yyyy"
  val PLACEHOLDER_PROGRESSIVO = "NUMERO-PROGRESSIVO"
  val XML_CHUNK_NAME_FIELD = "nome_chunk_xml"
  val XML_HEADER_CONSTANT = "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
  val OK = "OK"
  val REPORT_TABLE = s"${PropertyUtility.getHiveDb}.${PropertyUtility.getReportTable}"
  val REPORT_TABLE_PARTITIONING_COLUMN: String = "annomese_sw"

//  val FLOWS_2G = List("F2G", "S2G")
//  val FLOWS_NON_2G = List("SOF", "SNF", "SOS", "SNS")
//  val TM2G = "TM2G"
//  val STRING_2G = "2G"
//  val TME = "TME"
//  val ERROR = "ERROR"
//  def getFlowRootInfo(codFlusso: String): (String, String) = {
//    if (FLOWS_2G.contains(codFlusso))
//      (TM2G, STRING_2G)
//    else if (FLOWS_NON_2G.contains(codFlusso))
//      (TME, TME)
//    else
//      (ERROR, ERROR)
//  }
}
