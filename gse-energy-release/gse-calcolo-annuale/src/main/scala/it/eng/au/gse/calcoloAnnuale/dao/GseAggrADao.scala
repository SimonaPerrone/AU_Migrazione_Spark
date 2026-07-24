package it.eng.au.gse.calcoloAnnuale.dao

import it.eng.au.gse.calcoloAnnuale.schema.GseAggrASchema
import it.eng.au.gse.common.dao.OutputDao
import it.eng.au.gse.common.utility.Properties
import it.eng.au.gse.common.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SaveMode}

class GseAggrADao extends OutputDao {
  override val tableName: String = Properties.gseAggrATableName
  override val columns: List[String] = GseAggrASchema.getValues
  override val partitionByColumns: List[String] = List(GseAggrASchema.n_execution_id)
  override val parquetPath: String = Properties.gseAggrABasePath
  val exportTableName: String = Properties.gseAggrAExportTableName
  val exportParquetPath: String = Properties.gseAggrAExportBasePath

  def writeExport(df: DataFrame): Unit = {
    df
      .selectExpr(columns: _*)
      .write
      .mode(SaveMode.Append)
      .parquet(exportParquetPath)

    if (!Environment.isLocal) Environment.sqlContext.sql(s"MSCK REPAIR TABLE $tableName")
  }
}
