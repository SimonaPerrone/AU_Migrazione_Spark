package it.eng.au.queryReport.utility

import it.eng.au.aggregatoreConsumiCommon.utility.ConstantsTrait

object Constants extends ConstantsTrait {
  val TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.S"
  val TIMESTAMP_FORMAT_WITHOUT_MIL = "yyyy-MM-dd HH:mm:ss"
  val ANNOMESE_FORMAT = "yyyyMM"
  override val LOG = "QUERY_SBG"
}