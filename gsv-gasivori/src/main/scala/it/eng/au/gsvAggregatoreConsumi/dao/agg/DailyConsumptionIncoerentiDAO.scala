package it.eng.au.gsvAggregatoreConsumi.dao.agg

import it.eng.au.gsvAggregatoreConsumi.schema.agg.DailyConsumptionIncoerentiSchema
import it.eng.au.gsvAggregatoreConsumi.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class DailyConsumptionIncoerentiDAO extends AggDao {
  override val tableName: String = Environment.getDailyConsumptionIncGdMTable
  override val tablePath: String = Environment.getDailyConsumptionIncGdMPath
  override val columns: List[String] = DailyConsumptionIncoerentiSchema.getValues

  override def readParquet(): DataFrame = {
    val executionId = getLastExecutionId

    Environment.getSpark.sqlContext.read
      .parquet(tablePath)
      .where(col(EXECUTION_ID) === executionId)
      .withColumnRenamed("pdr", DailyConsumptionIncoerentiSchema.pdrI)
      .withColumnRenamed("date", DailyConsumptionIncoerentiSchema.dateI)
      .withColumnRenamed("value", DailyConsumptionIncoerentiSchema.valueI)
      .withColumnRenamed("valueNotSterilized", DailyConsumptionIncoerentiSchema.valueNotSterilizedI)
      .selectExpr(columns:_*)

  }

}
