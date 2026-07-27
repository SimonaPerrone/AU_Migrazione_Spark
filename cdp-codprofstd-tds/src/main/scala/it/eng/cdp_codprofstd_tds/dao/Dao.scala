package it.eng.cdp_codprofstd_tds.dao

import it.eng.cdp_codprofstd_tds.utility.Environment
import org.apache.spark.sql.{DataFrame, SQLContext}

trait Dao {
  val tableName: String
  val columns: List[String]

  def readTable: DataFrame = {
    Environment.getSpark.sqlContext.table(tableName)
  }

  def readParquet(parquetPath: String): DataFrame = {
    Environment.getSpark.read.parquet(parquetPath).selectExpr(columns: _*)
  }
}
