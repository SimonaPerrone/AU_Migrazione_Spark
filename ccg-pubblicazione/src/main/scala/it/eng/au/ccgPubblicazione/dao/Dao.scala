package it.eng.au.ccgPubblicazione.dao

import it.eng.au.ccgPubblicazione.utility.Environment
import org.apache.spark.sql.DataFrame

trait Dao {

  val tableName: String
  val fields: List[String]

  def readTable: DataFrame = {
    Environment.spark.read.table(tableName)
      .selectExpr(fields: _*)
  }

}
