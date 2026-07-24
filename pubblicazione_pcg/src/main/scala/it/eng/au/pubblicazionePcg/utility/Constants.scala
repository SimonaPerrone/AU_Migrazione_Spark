package it.eng.au.pubblicazionePcg.utility

object Constants {

  val CSV_SEPARATOR = ";"

  val TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
  val DATE_FORMAT = "yyyy-MM-dd"
  val YEAR_MONTH_FORMAT = "yyyyMM"

  val PATTERNS = Map(
    "Q" -> "0",
    "W" -> "1",
    "E" -> "2",
    "R" -> "3",
    "T" -> "4",
    "Y" -> "5",
    "N" -> "6",
    "S" -> "7",
    "D" -> "8",
    "M" -> "9"
  )
}
