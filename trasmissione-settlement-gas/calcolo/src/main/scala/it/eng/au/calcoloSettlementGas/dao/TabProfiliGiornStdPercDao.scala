package it.eng.au.calcoloSettlementGas.dao

import it.eng.au.calcoloSettlementGas.dao.`trait`.OutputDAO
import it.eng.au.calcoloSettlementGas.schema.TabProfiliGiornStdPercSchema
import it.eng.au.calcoloSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SaveMode}

class TabProfiliGiornStdPercDao extends OutputDAO {
  override val tableName: String = Properties.getTSG2TabProfiliGiornStdPercTableName
  override val parquetPath: String = Properties.getTSG2TabProfiliGiornStdPercPath
  override val partitionByColumns: List[String] = List(TabProfiliGiornStdPercSchema.executionid)
  override val columns: List[String] = TabProfiliGiornStdPercSchema.getValues
  override val partitionColumn: String = TabProfiliGiornStdPercSchema.executionid

  override def write(df: DataFrame): Unit = {
    df
      .coalesce(200)
      .selectExpr(columns: _*)
      .write
      .mode(SaveMode.Append)
      .partitionBy(partitionByColumns: _*)
      .parquet(parquetPath)

    if (!Environment.isLocal) Environment.sqlContext.sql(s"MSCK REPAIR TABLE $tableName")
  }
}
