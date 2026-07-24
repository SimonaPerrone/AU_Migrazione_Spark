package it.au.misure.calcolo_capacita.component.utility.`implicit`

import it.au.misure.calcolo_capacita.component.schema.CalcoloConsumiSbgSchema
import it.au.misure.calcolo_capacita.component.utility.CalculatedField._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat}
import org.apache.spark.sql.types.IntegerType

object ProgressiveDay {

  implicit class ProgressiveDay(df: DataFrame) {
    def getNumericDay(): DataFrame = {
      df
        .withColumn(numericDay, concat(col(CalcoloConsumiSbgSchema.annomese_rif), col(CalcoloConsumiSbgSchema.giorno)).cast(IntegerType))
    }

  }

}