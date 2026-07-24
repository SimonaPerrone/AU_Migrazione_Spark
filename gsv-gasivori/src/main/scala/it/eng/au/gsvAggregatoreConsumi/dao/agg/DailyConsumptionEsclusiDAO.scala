package it.eng.au.gsvAggregatoreConsumi.dao.agg

import it.eng.au.gsvAggregatoreConsumi.schema.agg.DailyConsumptionEsclusiSchema
import it.eng.au.gsvAggregatoreConsumi.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class DailyConsumptionEsclusiDAO extends AggDao {
  override val tableName: String = Environment.getDailyConsumptionExclTable
  override val tablePath: String = Environment.getDailyConsumptionExclPath
  override val columns: List[String] = DailyConsumptionEsclusiSchema.getValues

  override def readParquet(): DataFrame = {
    val executionId = getLastExecutionId

    Environment.getSpark.sqlContext.read
      .parquet(tablePath)
      .where(col(EXECUTION_ID) === executionId)
      .withColumnRenamed("pdr", DailyConsumptionEsclusiSchema.pdrE)
      .withColumnRenamed("date", DailyConsumptionEsclusiSchema.dateE)
      .withColumnRenamed("value", DailyConsumptionEsclusiSchema.valueE)
      .withColumnRenamed("valueNotSterilized", DailyConsumptionEsclusiSchema.valueNotSterilizedE)
      .selectExpr(columns:_*)

  }
}
