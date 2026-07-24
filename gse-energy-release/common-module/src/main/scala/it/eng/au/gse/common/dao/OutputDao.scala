package it.eng.au.gse.common.dao

import it.eng.au.gse.common.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SaveMode}

trait OutputDao extends Dao {
  val parquetPath: String
  val partitionByColumns: List[String]

  def write(df: DataFrame): Unit = {
    df
      .selectExpr(columns: _*)
      .write
      .mode(SaveMode.Append)
      .partitionBy(partitionByColumns: _*)
      .parquet(parquetPath)

    if (!Environment.isLocal) Environment.sqlContext.sql(s"MSCK REPAIR TABLE $tableName")
  }
}
