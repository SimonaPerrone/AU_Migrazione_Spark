package it.sferanet.au.dal

import it.sferanet.au.utilities.Environment
import org.apache.spark.sql.DataFrame

trait Dao {
  val tableName: String
  val columns: List[String]

  def readTable: DataFrame = {
    Environment.getSqlContext.table(tableName)
      .selectExpr(columns: _*)
  }

  def readParquet: DataFrame = {
    Environment.getSqlContext.read.parquet(tableName)
      .selectExpr(columns: _*)
  }

}
