package it.eng.au.gse.calcoloMensile.dao

import it.eng.au.gse.calcoloMensile.schema.gse.GseAggrMSchema
import it.eng.au.gse.common.dao.OutputDao
import it.eng.au.gse.common.utility.Properties
import it.eng.au.gse.common.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SaveMode}

class GseAggrMDao extends OutputDao {
  override val tableName: String = Properties.gseAggrMTableName
  override val columns: List[String] = GseAggrMSchema.getValues
  override val partitionByColumns: List[String] = List(GseAggrMSchema.n_execution_id)
  override val parquetPath: String = Properties.gseAggrMBasePath
  val exportTableName: String = Properties.gseAggrMExportTableName
  val exportParquetPath: String = Properties.gseAggrMExportBasePath

  def writeExport(df: DataFrame): Unit = {
    df
      .selectExpr(columns: _*)
      .write
      .mode(SaveMode.Append)
      .parquet(exportParquetPath)

    if (!Environment.isLocal) Environment.sqlContext.sql(s"MSCK REPAIR TABLE $tableName")
  }
}
