package it.eng.au.aggregatoreConsumiCommon.dao.agg

import it.eng.au.aggregatoreConsumiCommon.dao.Dao
import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggEsclusiSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

class DailyConsumptionEsclusiDao extends Dao {
  override val tableName: String = Environment.getDailyConsumptionEsclusiTableName
  override val columns: List[String] = DailyConsumptionAggEsclusiSchema.getValues
  val value: String = "value"
  val valueNotSterilized: String = "valueNotSterilized"

  def readPartition(executionid: String): DataFrame = {
    readTable
      .filter(col(DailyConsumptionAggEsclusiSchema.executionid) === executionid)
      .withColumnRenamed(value, DailyConsumptionAggEsclusiSchema.sterilizedValueE)
      .withColumnRenamed(valueNotSterilized, DailyConsumptionAggEsclusiSchema.valueNotSterilizedE)
      .withColumn(DailyConsumptionAggEsclusiSchema.esclusiFlag, lit(true))
      .selectExpr(columns:_*)
  }
}
