package it.eng.au.freezerPreCalcolo.freezer.publication

import it.eng.au.freezerPreCalcolo.schema.RcuGasMassivoFrozenSchema
import it.eng.au.freezerPreCalcolo.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object RcuGasMassivoPublication {
  def publication(df: DataFrame): DataFrame = {
    df.withColumn(RcuGasMassivoFrozenSchema.session, lit(Environment.getSession))
      .selectExpr(RcuGasMassivoFrozenSchema.getValues: _*)
  }
}
