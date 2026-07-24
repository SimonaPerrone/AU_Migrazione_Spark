package it.eng.au.gsvAggregatoreConsumi.controller

import it.eng.au.gsvAggregatoreConsumi.schema.agg.{DailyConsumptionEsclusiSchema, DailyConsumptionIncoerentiSchema, DailyConsumptionSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}

class SterilizationController {

  private val colsToJoinE = col(DailyConsumptionSchema.pdr).equalTo(col(DailyConsumptionEsclusiSchema.pdrE)) and col(DailyConsumptionSchema.date).equalTo(col(DailyConsumptionEsclusiSchema.dateE))
  private val colsToJoinI = col(DailyConsumptionSchema.pdr).equalTo(col(DailyConsumptionIncoerentiSchema.pdrI)) and col(DailyConsumptionSchema.date).equalTo(col(DailyConsumptionIncoerentiSchema.dateI))

  def sterilizeConsumptions(dailyDF: DataFrame, incoerentiDF: DataFrame, esclusiDF: DataFrame) : DataFrame = {

    dailyDF
      //filtro specifico richiesto per procedura di aggregazione
      .filter(col(DailyConsumptionSchema.errorCode).isin(0,1,5,6,9,10,11,12))
      .join(esclusiDF.withColumn("esclusiFlag", lit(true)), colsToJoinE, "left")
      .join(incoerentiDF.withColumn("incoerentiFlag", lit(true)), colsToJoinI, "left")
      .withColumn(DailyConsumptionSchema.value, when(col("esclusiFlag"), col(DailyConsumptionEsclusiSchema.valueE)).otherwise(col(DailyConsumptionSchema.value)))
      .withColumn(DailyConsumptionSchema.value, when(col("incoerentiFlag"), col(DailyConsumptionIncoerentiSchema.valueI)).otherwise(col(DailyConsumptionSchema.value)))
      .withColumn(DailyConsumptionSchema.valueNotSterilized, when(col("esclusiFlag"), col(DailyConsumptionEsclusiSchema.valueNotSterilizedE)).otherwise(col(DailyConsumptionSchema.valueNotSterilized)))
      .withColumn(DailyConsumptionSchema.valueNotSterilized, when(col("incoerentiFlag"), col(DailyConsumptionIncoerentiSchema.valueNotSterilizedI)).otherwise(col(DailyConsumptionSchema.valueNotSterilized)))
      .selectExpr(DailyConsumptionSchema.getValues:_*)

  }

}
