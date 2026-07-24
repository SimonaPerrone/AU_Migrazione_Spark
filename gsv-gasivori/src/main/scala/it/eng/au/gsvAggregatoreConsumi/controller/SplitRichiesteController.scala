package it.eng.au.gsvAggregatoreConsumi.controller

import it.eng.au.gsvAggregatoreConsumi.schema.gsv.GsvConsRichiestaSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, year}
import org.apache.spark.sql.types.IntegerType

class SplitRichiesteController {

  val annoRichiesta = "anno_richiesta"
  val period = "period"

  def getOrdinarie(df: DataFrame): DataFrame = {

    val specificCondition = List(col(annoRichiesta).minus(1), col(annoRichiesta).minus(2), col(annoRichiesta).minus(3))

    df
      .filter(col(GsvConsRichiestaSchema.t_tipo)==="RICHIESTA_CONS_O")
      .withColumn(annoRichiesta, year(col(GsvConsRichiestaSchema.d_data_richiesta)).cast(IntegerType))
      .withColumn(period, concat_ws(",", specificCondition:_*))
      .drop(annoRichiesta)

  }

  def getSuppletive(df: DataFrame): DataFrame = {

    val specificCondition = List(col(annoRichiesta).minus(2), col(annoRichiesta).minus(3), col(annoRichiesta).minus(4))

    df
      .filter(col(GsvConsRichiestaSchema.t_tipo)==="RICHIESTA_CONS_S")
      .withColumn(annoRichiesta, year(col(GsvConsRichiestaSchema.d_data_richiesta)).cast(IntegerType))
      .withColumn(period, concat_ws(",", specificCondition:_*))
      .drop(annoRichiesta)

  }

  def getConsuntive(df: DataFrame): DataFrame = {

    val specificCondition = List(col(annoRichiesta).minus(1))

    df
      .filter(col(GsvConsRichiestaSchema.t_tipo)==="RICHIESTA_CONS_C")
      .withColumn(annoRichiesta, year(col(GsvConsRichiestaSchema.d_data_richiesta)).cast(IntegerType))
      .withColumn(period, concat_ws(",",specificCondition:_*))
      .drop(annoRichiesta)


  }

}
