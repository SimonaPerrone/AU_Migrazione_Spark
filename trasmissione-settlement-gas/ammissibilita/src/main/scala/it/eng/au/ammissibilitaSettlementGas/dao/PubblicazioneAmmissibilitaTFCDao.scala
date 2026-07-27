package it.eng.au.ammissibilitaSettlementGas.dao

import it.eng.au.ammissibilitaSettlementGas.dao.`trait`.OutputDAO
import it.eng.au.ammissibilitaSettlementGas.model.AlreadyComputedFiles
import it.eng.au.ammissibilitaSettlementGas.schema.{PubblicazioneAmmissibilitaTFCSchema, ReportAmmissibilitaTFCSchema}
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

class PubblicazioneAmmissibilitaTFCDao extends OutputDAO {
  override val tableName: String = Properties.getTSG2PubblicazioneAmmissibilitaTFCTableName
  override val parquetPath: String = Properties.getTSG2PubblicazioneAmmissibilitaTFCPath
  override val partitionByColumns: List[String] = List(PubblicazioneAmmissibilitaTFCSchema.ANNOMESE)
  override val columns: List[String] = PubblicazioneAmmissibilitaTFCSchema.getValues
  override val partitionColumn: String = PubblicazioneAmmissibilitaTFCSchema.ANNOMESE.toString

  def getPubblicazioneAmmissibilitaTFC(yearMonthMin: String, yearMonthMax: String): DataFrame = {
    readTable
      .where(
        (col(partitionColumn) >= yearMonthMin && col(partitionColumn) <= yearMonthMax) ||
          col(partitionColumn).isNull // Default partition.
      )
      .select(
        PubblicazioneAmmissibilitaTFCSchema.CARTELLA_CLOUD,
        PubblicazioneAmmissibilitaTFCSchema.CSV_FILE_NAME,
        PubblicazioneAmmissibilitaTFCSchema.LAST_MODIFIED
      )
  }

  def getAlreadyComputedTFCFiles(pubblicazioneAmmissibilitaTFC: DataFrame): RDD[AlreadyComputedFiles] = {
    pubblicazioneAmmissibilitaTFC
      .rdd
      .map(row =>
        AlreadyComputedFiles(
          fileName = row.getAs[String](PubblicazioneAmmissibilitaTFCSchema.CARTELLA_CLOUD) + row.getAs[String](PubblicazioneAmmissibilitaTFCSchema.CSV_FILE_NAME),
          lastModifiedDate = row.getAs[Long](PubblicazioneAmmissibilitaTFCSchema.LAST_MODIFIED)
        ))
      .distinct()
  }

  def getAlreadyTransmittedTFC(reportAmmissibilita: DataFrame): RDD[String] = {
    reportAmmissibilita
      .select(PubblicazioneAmmissibilitaTFCSchema.CSV_FILE_NAME)
      .rdd
      .map(_.getAs[String](PubblicazioneAmmissibilitaTFCSchema.CSV_FILE_NAME))
      .distinct
  }


  def get(dfReportAmmissibilitaTFC: DataFrame, dfReportPubblicazioneAmmTFC: DataFrame): DataFrame = {
    val dfResult = dfReportAmmissibilitaTFC
      .join(dfReportPubblicazioneAmmTFC,
        dfReportAmmissibilitaTFC(ReportAmmissibilitaTFCSchema.nome_file) ===
          dfReportPubblicazioneAmmTFC(PubblicazioneAmmissibilitaTFCSchema.CSV_FILE_NAME), "inner")

    dfResult.withColumnRenamed(ReportAmmissibilitaTFCSchema.nome_file, PubblicazioneAmmissibilitaTFCSchema.CSV_FILE_NAME)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.data_amm, PubblicazioneAmmissibilitaTFCSchema.DATA_AMM)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.executionid, PubblicazioneAmmissibilitaTFCSchema.EXECUTIONID)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.annomese, PubblicazioneAmmissibilitaTFCSchema.ANNOMESE)

    dfResult.selectExpr(PubblicazioneAmmissibilitaTFCSchema.getValues: _*).dropDuplicates()
    // FIXME: Da ottimizzare, serviva questa modifica alla tabella e, per motivi di tempo, abbiamo
    //    dovuto fare in questo modo.

  }

}
