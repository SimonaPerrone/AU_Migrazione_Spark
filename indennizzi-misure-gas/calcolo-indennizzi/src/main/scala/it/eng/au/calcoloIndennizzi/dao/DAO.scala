package it.eng.au.calcoloIndennizzi.dao

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.DataFrame

trait DAO {
  val parquetPath: String
  val columns: List[String]

  def readParquet: DataFrame = {
    Environment.sqlContext.read.parquet(parquetPath)
      .selectExpr(columns: _*)
  }

  def get: DataFrame = readParquet
}

object DAO {
  val daysCount: String = "days_count"
  val daysCountPerPdr: String = "days_count_per_pdr"
  val distinctPivaCount: String = "distinct_piva_count"
}