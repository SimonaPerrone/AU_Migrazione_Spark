package it.eng.au.scambioDatiGasivori.dao.traits

import it.eng.au.scambioDatiGasivori.utility.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

trait Dao extends Serializable {
  val tableName: String
  val columns: List[String]
  val partitionColumn: Option[String] = None

  def readTable: DataFrame = {
    Environment.sqlContext.table(tableName).selectExpr(columns: _*)
  }

  def readPartition(partitionValue: String): DataFrame = {
    readTable.where(col(partitionColumn.get) === partitionValue)
  }
}