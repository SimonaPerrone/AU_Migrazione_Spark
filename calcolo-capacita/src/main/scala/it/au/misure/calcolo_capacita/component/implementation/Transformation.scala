package it.au.misure.calcolo_capacita.component.implementation

import it.au.misure.calcolo_capacita.component.contract.HDao
import it.au.misure.calcolo_capacita.component.schema.ClgPdrCapacitaSchema
import it.au.misure.calcolo_capacita.component.utility.CalculatedField._
import it.au.misure.calcolo_capacita.component.utility.property.RunningProperty
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

object Transformation {

  def setPcmFields(df: DataFrame, typeCalculation: String): DataFrame = {
    df
      .withColumnRenamed(caMax, ClgPdrCapacitaSchema.n_pcm)
      .withColumn(ClgPdrCapacitaSchema.t_tipo_calcolo, lit(typeCalculation))

  }

}
