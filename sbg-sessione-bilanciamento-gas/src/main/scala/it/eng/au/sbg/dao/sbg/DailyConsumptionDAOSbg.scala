package it.eng.au.sbg.dao.sbg

import it.eng.au.aggiustamentoGas.dao.agg.AggDao
import it.eng.au.aggiustamentoGas.model.agg.DailyConsumption
import it.eng.au.aggiustamentoGas.schema.agg.DailyConsumptionSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.lit

class DailyConsumptionDAOSbg extends AggDao {
  val tableName: String = Environment.getDailyConsumptionTable
  override val parquetPath: String = Environment.getDailyConsumptionPath
  override val columns: List[String] = DailyConsumptionSchema.getValues


  def writeParquet(measures: RDD[DailyConsumption]): Unit = {
    val df = Environment.getSpark.sqlContext.createDataFrame(measures)

    writeParquet(df)
  }

  override def writeParquet(df: DataFrame): Unit = {
    df
      .withColumn(EXECUTION_ID, lit(Environment.executionId))
      .withColumn(SESSION, lit(Environment.getSession))
      .write
      .mode(SaveMode.Append)
      .partitionBy(SESSION, DailyConsumptionSchema.annoMese, EXECUTION_ID)
      .parquet(parquetPath)

    Environment.getSpark.sql(s"MSCK REPAIR TABLE $tableName")
  }
}
