package it.eng.au.aggregatoreConsumiCommon.dao.agg

import it.eng.au.aggregatoreConsumiCommon.dao.Dao
import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DecimalType

/** Data Access Object per la tabella dei consumi AGG/SBG */
class DailyConsumptionDao extends Dao {
  override val tableName: String = Environment.getDailyConsumptionTableName
  override val columns: List[String] = DailyConsumptionAggSchema.getValues

  def readPartition(executionid: String): DataFrame = {
    readTable.filter(col(DailyConsumptionAggSchema.executionid) === executionid)
  }

  def prepare(executionid: String): DataFrame = {
    readPartition(executionid)
      .withColumnRenamed("forceexclusion", DailyConsumptionAggSchema.forcedExclusion)
      .withColumn(DailyConsumptionAggSchema.ca, col(DailyConsumptionAggSchema.ca).cast(DecimalType(12,1)))
  }
}
