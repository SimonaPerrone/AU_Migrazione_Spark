package it.eng.au.aggregatoreConsumiCommon.dao

import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.DataFrame

trait Dao {
  val tableName: String
  val columns: List[String]

  def readTable: DataFrame = {
    Environment.sqlContext.table(tableName)
  }

}
