package it.sferanet.au.utilities

import java.text.SimpleDateFormat
import java.util.Date

object Constants {
  val DEBUG: Boolean = false
  val CA_LOG = "CA LOG:"
  val CCG_CA_LOG = "CCG_CA LOG:"

  def STANDARD_FORMAT_DATE: SimpleDateFormat = getFormatter("dd/MM/yyyy")

  def FORMAT_DATE_LOAD: SimpleDateFormat = getFormatter("yyyy-MM-dd'T'HH:mm:ss.ssssss")

  def FORMAT_DATE_CLOUD_FILENAME: SimpleDateFormat = getFormatter("yyyyMMddHHmmss")

  val FILTER_MODE_PDR: String = "pdr"
  val FILTER_MODE_DISTRIBUTORE: String = "distributore"
  val FILTER_MODE_DISTRIBUTORE_UDD: String = "distr-udd"
  val FILTER_MODE_DISTRIBUTORE_UDD_UDB: String = "distr-udd-udb"
  val FILTER_MODE_OGGETTO_VARIAZIONE: String = "oggettoVariazione"
  val FILTER_MODE_CARICAMENTO_TDS: String = "caricamentoTDS"
  val FILTER_MODE_NO_MISURE: String = "noMisure"
  val FILTER_MODE_FORZATURA: String = "forzatura"
  val FILTER_MODE_PERIMETRO_AGG_RIC: String = "perimetroAggRic"
  val FILTER_MODE_ERROR_MESSAGE: String = "Invalid value for property \"filterPdr.mode\" in properties file config.properties:"
  val CALCMOD_FORZATO = "forzato"
  val CALCMOD_PROCEDURA = "procedura"
  val CALCMOD_DEDOTTO = "dedotto"
  val HALF_YEAR_FORMAT_DATE: String = "dd/MM/yy"
  val JULY = 7

  //Cod Porf Prel Srd whitelist definition
  val C1A1 = "C1A1"
  val C1B1 = "C1B1"
  val C1C1 = "C1C1"
  val C1D1 = "C1D1"
  val C1E1 = "C1E1"
  val C1F1 = "C1F1"
  val C2X1 = "C2X1"
  val C3A1 = "C3A1"
  val C3B1 = "C3B1"
  val C3C1 = "C3C1"
  val C3D1 = "C3D1"
  val C3E1 = "C3E1"
  val C3F1 = "C3F1"
  val C4X1 = "C4X1"
  val C5A1 = "C5A1"
  val C5B1 = "C5B1"
  val C5C1 = "C5C1"
  val C5D1 = "C5D1"
  val C5E1 = "C5E1"
  val C5F1 = "C5F1"
  val T1X1 = "T1X1"
  val T1X2 = "T1X2"
  val T1X3 = "T1X3"
  val T2A1 = "T2A1"
  val T2B1 = "T2B1"
  val T2C1 = "T2C1"
  val T2D1 = "T2D1"
  val T2E1 = "T2E1"
  val T2F1 = "T2F1"
  val T2A2 = "T2A2"
  val T2B2 = "T2B2"
  val T2C2 = "T2C2"
  val T2D2 = "T2D2"
  val T2E2 = "T2E2"
  val T2F2 = "T2F2"
  val T2A3 = "T2A3"
  val T2B3 = "T2B3"
  val T2C3 = "T2C3"
  val T2D3 = "T2D3"
  val T2E3 = "T2E3"
  val T2F3 = "T2F3"
  val codProfPrelStdWitheList = List(C1A1, C1B1, C1C1, C1D1, C1E1, C1F1, C2X1, C3A1, C3B1, C3C1, C3D1, C3E1, C3F1, C4X1,
    C5A1, C5B1, C5C1, C5D1, C5E1, C5F1, T1X1, T1X2, T1X3, T2A1, T2B1, T2C1, T2D1, T2E1, T2F1, T2A2, T2B2, T2C2, T2D2,
    T2E2, T2F2, T2A3, T2B3, T2C3, T2D3, T2E3, T2F3)

  val MAX_BC_SIZE_MB: Long = 30

  val BLOCCANTE = "BLOCCANTE"

  val ERROR = "ERROR"

  def getFormatter(format: String): SimpleDateFormat = {
    val retVal = new SimpleDateFormat(format)
    //    retVal.setTimeZone(TimeZone.getTimeZone("UTC"))
    retVal
  }

  def parseDate(formatter: SimpleDateFormat, value: String): Date = {
    val retVal = formatter.parse(value)
    retVal
  }

  def getDate(formatter: SimpleDateFormat, value: String): Option[Date] = {
    try {
      if (value == null || value.isEmpty || value == "")
        None
      else
        Some(formatter.parse(value))
    } catch {
      case ex: Exception =>
        val exec = ex
        println("error parsing %s. Message: %s".format(value, exec.getMessage))
        None
    }
  }
}
