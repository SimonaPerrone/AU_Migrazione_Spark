package it.eng.cdp_codprofstd_tds.utility

import org.apache.spark.sql.Column

object Utility {
  def notNullorEmpty(column: Column): Column = {
    column.isNotNull && (column =!= "")
  }
}
