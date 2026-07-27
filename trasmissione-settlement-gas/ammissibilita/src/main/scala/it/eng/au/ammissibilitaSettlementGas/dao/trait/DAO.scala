package it.eng.au.ammissibilitaSettlementGas.dao.`trait`

import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


trait DAO {
  val tableName: String
  val columns: List[String]
  val partitionColumn: String

  def readTable: DataFrame = {
    Environment.sqlContext.table(tableName).selectExpr(columns: _*)
  }

  def readTablePartiton(partition: String): DataFrame = {
    readTable.filter(col(partitionColumn) === partition)
  }
}
