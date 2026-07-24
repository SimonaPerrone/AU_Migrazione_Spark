package it.eng.au.calcoloIndennizzi.utility.constants

object Constants {
  val CALCOLO_APPLICATION_NAME = "[CIG] Calcolo Indennizzi Misure GAS"
  val CIG_LOG = "CIG LOG:"

  val TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
  val TIMESTAMP_MS_FORMAT = "yyyy-MM-dd HH:mm:ss.S"
  val DATE_FORMAT = "yyyy-MM-dd"
  val YEAR_MONTH_FORMAT = "yyyyMM"
  val YEAR_MONTH_DAY_FORMAT = "yyyyMMdd"
  val MONTH_YEAR_FORMAT = "MMyyyy"
  val DATA_COMP_FORMAT = "dd/MM/yyyy"

  val EFFETTIVA = "E"
  val STIMATA = "S"

  val RACCOLTA_P = "P"

  val BLOCCANTE = "BLOCCANTE"
  val VAL_DATO_SI = "SI"

  val TRATTAMENTO_G = "G"

  val admissibleClasseMisuratoreList = Seq(
    "G10",
    "G16",
    "G25",
    "G40",
    "G65",
    "G100",
    "G160",
    "G250",
    "G400",
    "G650",
    "G1000",
    "G1600",
    "G2500",
    "G4000",
    "G6500")
}