package it.eng.au.pubblicazioneIndennizzi.dao.cig

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import it.eng.au.pubblicazioneIndennizzi.dao.Dao
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

trait WriteDao extends Dao {
  val EXECUTION_ID = "executionid"
  val ANNO_MESE = "annomese"
  val parquetPath: String
  override val partitionColumn: String = ""

  def writeParquet(df: DataFrame): Unit = {
    df
      .withColumn(EXECUTION_ID, lit(Environment.executionId))
      .write
      .mode(SaveMode.Append)
      .partitionBy(ANNO_MESE, EXECUTION_ID)
      .parquet(parquetPath)

    if (!Environment.isLocal) Environment.spark.sql(s"MSCK REPAIR TABLE $tableName")
  }
}


