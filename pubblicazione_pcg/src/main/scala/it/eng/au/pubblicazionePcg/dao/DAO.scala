package it.eng.au.pubblicazionePcg.dao

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import java.util.Properties

trait DAO {
  val tableName: String
  val columns: List[String]
  val partitionColumn: String
  val partitionValue: String
  val hdfsOutput: String

  def readTable(implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.read.table(tableName)
      .selectExpr(columns: _*)
  }

  def writeParquet(df: DataFrame)(implicit prop: Properties): Unit = {
    df.write.partitionBy(partitionColumn).mode(SaveMode.Append).parquet(hdfsOutput)
  }

  def writeParquetDirectlyToPath(df: DataFrame, path: String)(implicit prop: Properties): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(path)
  }
}
