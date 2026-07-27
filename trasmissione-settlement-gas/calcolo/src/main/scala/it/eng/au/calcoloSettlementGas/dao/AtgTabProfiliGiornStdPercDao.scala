package it.eng.au.calcoloSettlementGas.dao

import it.eng.au.calcoloSettlementGas.dao.`trait`.{DAO, OutputDAO}
import it.eng.au.calcoloSettlementGas.schema.AtgTabProfiliGiornStdPercSchema
import it.eng.au.calcoloSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SaveMode}

class AtgTabProfiliGiornStdPercDao extends OutputDAO {
  override val tableName: String = Properties.getTSG2AtgTabProfiliGiornStdPercTableName
  override val columns: List[String] = AtgTabProfiliGiornStdPercSchema.getValues
  override val partitionColumn: String = AtgTabProfiliGiornStdPercSchema.annomese.toString
  override val parquetPath: String = Properties.getTSG2AtgTabProfiliGiornStdPercPath
  override val partitionByColumns: List[String] = List(AtgTabProfiliGiornStdPercSchema.annomese.toString)


  override def write(df: DataFrame): Unit = {
    df
      .coalesce(200)
      .selectExpr(columns: _*)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy(partitionByColumns: _*)
      .parquet(parquetPath)

    if (!Environment.isLocal) Environment.sqlContext.sql(s"MSCK REPAIR TABLE $tableName")
  }
}
