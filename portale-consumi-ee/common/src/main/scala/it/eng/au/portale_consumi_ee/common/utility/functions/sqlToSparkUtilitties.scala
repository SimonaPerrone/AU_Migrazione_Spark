package it.eng.au.portale_consumi_ee.common.utility.functions

import org.apache.spark.sql.functions.udf

object sqlToSparkUtilitties {

  // Define and register the isNumeric function as a UDF
  def isNumericUDF = udf((s: String) => s.forall(_.isDigit))
}
