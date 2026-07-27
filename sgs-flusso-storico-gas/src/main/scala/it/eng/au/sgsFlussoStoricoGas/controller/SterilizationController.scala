package it.eng.au.sgsFlussoStoricoGas.controller

import it.eng.au.sgsFlussoStoricoGas.schema.dailyConsumption.{DailyConsumptionEsclusiSchema, DailyConsumptionIncoerentiSchema, DailyConsumptionSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}

class SterilizationController {

  private val colsToJoinE = col(DailyConsumptionSchema.pdr).equalTo(col(DailyConsumptionEsclusiSchema.pdrE)) and col(DailyConsumptionSchema.date).equalTo(col(DailyConsumptionEsclusiSchema.dateE))
  private val colsToJoinI = col(DailyConsumptionSchema.pdr).equalTo(col(DailyConsumptionIncoerentiSchema.pdrI)) and col(DailyConsumptionSchema.date).equalTo(col(DailyConsumptionIncoerentiSchema.dateI))

  def sterilizeConsumptions(dailyDF: DataFrame, incoerentiDF: DataFrame, esclusiDF: DataFrame): DataFrame = {

    dailyDF
      //filtro specifico richiesto per procedura di aggregazione
      .filter(col("forceExclusion") =!= true
        and col(DailyConsumptionSchema.treatment) =!= "N"
        and col("isValid") === true
        and col("dtg").isNotNull
        and col("codRemi").isNotNull
        and col("codProfStd").isNotNull
        and col("tipoCliente").isNotNull
      )
      .join(esclusiDF.withColumn("esclusiFlag", lit(true)), colsToJoinE, "left")
      .join(incoerentiDF.withColumn("incoerentiFlag", lit(true)), colsToJoinI, "left")
      .withColumn("filterErrorCode",
        when(col("esclusiFlag").isNull and col("errorCode").isin(0, 10, 11, 12), lit(true))
          .when(col("esclusiFlag") and !col("errorCode").isin(0, 7, 10, 11, 12, 13), lit(true))
          .otherwise(lit(false))
      )
      .filter(col("filterErrorCode") === true)
      .withColumn(DailyConsumptionSchema.value, when(col("esclusiFlag"), col(DailyConsumptionEsclusiSchema.valueE)).otherwise(col(DailyConsumptionSchema.value)))
      .withColumn(DailyConsumptionSchema.value, when(col("incoerentiFlag"), col(DailyConsumptionIncoerentiSchema.valueI)).otherwise(col(DailyConsumptionSchema.value)))
      .withColumn(DailyConsumptionSchema.valueNotSterilized, when(col("esclusiFlag"), col(DailyConsumptionEsclusiSchema.valueNotSterilizedE)).otherwise(col(DailyConsumptionSchema.valueNotSterilized)))
      .withColumn(DailyConsumptionSchema.valueNotSterilized, when(col("incoerentiFlag"), col(DailyConsumptionIncoerentiSchema.valueNotSterilizedI)).otherwise(col(DailyConsumptionSchema.valueNotSterilized)))
      .selectExpr(DailyConsumptionSchema.getValues: _*)

  }

}
