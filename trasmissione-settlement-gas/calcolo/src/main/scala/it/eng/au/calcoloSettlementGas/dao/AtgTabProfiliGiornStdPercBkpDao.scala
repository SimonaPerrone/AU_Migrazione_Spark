package it.eng.au.calcoloSettlementGas.dao

import it.eng.au.calcoloSettlementGas.dao.`trait`.OutputDAO
import it.eng.au.calcoloSettlementGas.schema.AtgTabProfiliGiornStdPercBkpSchema
import it.eng.au.calcoloSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SaveMode}

class AtgTabProfiliGiornStdPercBkpDao extends OutputDAO {
  override val tableName: String = Properties.getTSG2AtgTabProfiliGiornStdPercBkpTableName
  override val parquetPath: String = Properties.getTSG2AtgTabProfiliGiornStdPercBkpPath
  override val partitionByColumns: List[String] = List(AtgTabProfiliGiornStdPercBkpSchema.annomese.toString,
    AtgTabProfiliGiornStdPercBkpSchema.executionid.toString)
  override val columns: List[String] = AtgTabProfiliGiornStdPercBkpSchema.getValues
  override val partitionColumn: String = AtgTabProfiliGiornStdPercBkpSchema.annomese.toString

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
