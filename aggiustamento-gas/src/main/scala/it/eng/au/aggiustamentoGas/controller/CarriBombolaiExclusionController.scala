package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.model.agg.ErrorEnum
import it.eng.au.aggiustamentoGas.schema.CarriBombolaiFileSchema
import it.eng.au.aggiustamentoGas.schema.agg.DailyConsumptionAGGSBGSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}

class CarriBombolaiExclusionController extends Serializable {

  def excludeCarriBombolai(daily: DataFrame, exclusionFile: DataFrame): DataFrame = {

    val colsToJoin = Seq(DailyConsumptionAGGSBGSchema.codRemi.toString)

    daily
      .join(exclusionFile, colsToJoin, "left")
      .withColumn(DailyConsumptionAGGSBGSchema.errorCode,
        when(col(CarriBombolaiFileSchema.carriBombolaiFlag).and(col(DailyConsumptionAGGSBGSchema.errorCode) =!= lit(ErrorEnum.FURNITURE_INACTIVE_ERROR_CODE.id))
          , lit(ErrorEnum.CARRI_BOMBOLAI_ERROR_CODE.id)).otherwise(col(DailyConsumptionAGGSBGSchema.errorCode))
      )
  }

}
