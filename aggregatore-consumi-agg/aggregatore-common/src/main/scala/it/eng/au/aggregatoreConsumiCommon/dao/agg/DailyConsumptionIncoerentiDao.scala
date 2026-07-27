package it.eng.au.aggregatoreConsumiCommon.dao.agg

import it.eng.au.aggregatoreConsumiCommon.dao.Dao
import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggIncoerentiSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

class DailyConsumptionIncoerentiDao extends Dao {
  override val tableName: String = Environment.getDailyConsumptionIncoerentiTableName
  override val columns: List[String] = DailyConsumptionAggIncoerentiSchema.getValues
  val value: String = "value"
  val valueNotSterilized: String = "valueNotSterilized"

  def readPartition(executionid: String): DataFrame = {
    readTable
      .filter(col(DailyConsumptionAggIncoerentiSchema.executionid) === executionid)
      .withColumnRenamed(value, DailyConsumptionAggIncoerentiSchema.sterilizedValueI)
      .withColumnRenamed(valueNotSterilized, DailyConsumptionAggIncoerentiSchema.valueNotSterilizedI)
      .withColumn(DailyConsumptionAggIncoerentiSchema.incoerentiFlag, lit(true))
      .selectExpr(columns:_*)
  }
}
