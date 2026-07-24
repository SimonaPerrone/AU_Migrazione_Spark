package it.eng.au.aggregatoreConsumiCdp.dao

import it.eng.au.aggregatoreConsumiCdp.utility.Environment
import org.apache.spark.sql.DataFrame

trait Dao {
  val tableName: String
  val columns: List[String]

  def readTable: DataFrame = {
    Environment.sqlContext.table(tableName)
      .selectExpr(columns: _*)
  }

  def readParquet: DataFrame = {
    Environment.sqlContext.read.parquet(tableName)
      .selectExpr(columns: _*)
  }

}
