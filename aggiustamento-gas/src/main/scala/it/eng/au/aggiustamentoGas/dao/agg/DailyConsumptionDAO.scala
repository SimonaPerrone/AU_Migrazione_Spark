package it.eng.au.aggiustamentoGas.dao.agg

import it.eng.au.aggiustamentoGas.model.agg.DailyConsumption
import it.eng.au.aggiustamentoGas.schema.agg.{DailyConsumptionAGGSBGSchema, DailyConsumptionSchema}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, lit, when}

/** Tabella di output contenente i consumi giornalieri calcolati */
class DailyConsumptionDAO extends AggDao {
  override val tableName: String = Environment.getDailyConsumptionTable
  override val parquetPath: String = Environment.getDailyConsumptionPath
  override val columns: List[String] = DailyConsumptionSchema.getValues
  private val partitionColumn: String = "executionid"


  /*def writeParquet(measures: RDD[DailyConsumption]): Unit = {
    val df = Environment.getSpark.sqlContext.createDataFrame(measures)

    writeParquet(df)
  }*/

  def readPartition(executionid: Long): DataFrame = {
    Environment.getSpark.sqlContext.table(tableName)
      .filter(col(partitionColumn) === executionid)
      .selectExpr(DailyConsumptionAGGSBGSchema.getValues: _*)
  }


  def readTmp1Table(executionid: Long): DataFrame = {
    Environment.getSpark.read.parquet(parquetPath ++ "_tmp1")
      .filter(col(partitionColumn) === executionid)
  }


  def readTmp2Table(executionid: Long): DataFrame = {
    Environment.getSpark.read.parquet(parquetPath ++ "_tmp2")
      .filter(col(partitionColumn) === executionid)
  }

  def readTmp3Table(executionid: Long): DataFrame = {
    Environment.getSpark.read.parquet(parquetPath ++ "_tmp3")
      .filter(col(partitionColumn) === executionid)
  }

  def writeTemporaryTable1(measures: DataFrame): Unit = {
    measures
      .withColumn(SESSION, lit(Environment.getSession))
      .withColumn(EXECUTION_ID, lit(Environment.executionId))
      .write
      .mode(SaveMode.Append)
      .partitionBy(SESSION, EXECUTION_ID)
      .parquet(parquetPath ++ "_tmp1")
  }

  def writeTemporaryTable2(dailyC: DataFrame): Unit = {
    dailyC
      .write
      .mode(SaveMode.Append)
      .partitionBy(SESSION, EXECUTION_ID)
      .parquet(parquetPath ++ "_tmp2")
  }

  def writeTemporaryTable3(dailyC: DataFrame): Unit = {
    dailyC
      .write
      .mode(SaveMode.Append)
      .partitionBy(SESSION, EXECUTION_ID)
      .parquet(parquetPath ++ "_tmp3")
  }

  def deleteTemporaryTables(): Unit = {
    val fs = FileSystem.get(Environment.getSpark.sparkContext.hadoopConfiguration)

    val paths = Seq(
      parquetPath ++ "_tmp1"/*,
      parquetPath ++ "_tmp2",
      parquetPath ++ "_tmp3"*/
    )

    paths.foreach { p =>
      val dir = new Path(p)
      if (fs.exists(dir)) {
        val files = fs.listStatus(dir)
        files.foreach { f =>
          fs.delete(f.getPath, true) // elimina file o sottodirectory
        }
      }
    }
  }


}
