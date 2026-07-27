package it.eng.au.freezerPreCalcolo.freezer.publication

import it.eng.au.freezerPreCalcolo.schema.RcuGasTechFrozenSchema
import it.eng.au.freezerPreCalcolo.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object RcuGasTechPublication {
  def publication(df: DataFrame): DataFrame = {
    df.withColumn(RcuGasTechFrozenSchema.session, lit(Environment.getSession))
      .selectExpr(RcuGasTechFrozenSchema.getValues: _*)
  }
}
