package it.eng.au.indennizziMisureGasCommon.utility.dataframe

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, not, trim}

object DataFrameUtils {
  def isNullOrEmpty(c: Column): Column = c.isNull or trim(c) === "" or trim(c) === "null"

  def isNotNullNorEmpty(c: Column): Column = not(isNullOrEmpty(c))
}
