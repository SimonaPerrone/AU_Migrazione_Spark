package it.eng.au.freezerPreCalcolo.dao.rcugas

import it.eng.au.freezerPreCalcolo.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SQLContext}

/** Provides an abstract interface for classes which read from/write to HDFS. */
trait DAO {
  /** Path in HDFS where data is stored in parquet format. */
  val parquetPath: String
  /** List of columns to be read from HDFS. */
  val columns: List[String]

  /** Reads data from [[parquetPath]] is HDFS. */
  def readParquet(): DataFrame = {
    Environment.getSpark.sqlContext.read.parquet(parquetPath)
      .selectExpr(columns: _*)
  }
}
