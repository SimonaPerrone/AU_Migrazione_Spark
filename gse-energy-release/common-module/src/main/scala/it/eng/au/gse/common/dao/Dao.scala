package it.eng.au.gse.common.dao

import it.eng.au.gse.common.utility.environment.Environment
import org.apache.spark.sql.DataFrame

trait Dao {
  val tableName: String
  val columns: List[String]

  def readTable: DataFrame = {
    Environment.spark.read.table(tableName)
      .selectExpr(columns: _*)
  }
}
