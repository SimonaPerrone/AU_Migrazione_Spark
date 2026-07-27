package it.eng.au.calcoloSettlementGas.dao.`trait`

import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SaveMode}


trait OutputDAO extends DAO {
  val tableName: String
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

  def writeTable(df: DataFrame): Unit = {
    df.write.insertInto(tableName)
  }
}
