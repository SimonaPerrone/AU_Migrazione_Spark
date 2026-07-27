package it.eng.au.ammissibilitaSettlementGas.dao

import it.eng.au.ammissibilitaSettlementGas.dao.`trait`.OutputDAO
import it.eng.au.ammissibilitaSettlementGas.model.AlreadyComputedFiles
import it.eng.au.ammissibilitaSettlementGas.schema.{PubblicazioneAmmissibilitaVPGSchema, ReportAmmissibilitaVPGSchema}
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class PubblicazioneAmmissibilitaVPGDao extends OutputDAO {
  override val tableName: String = Properties.getTSG2PubblicazioneAmmissibilitaVPGTableName
  override val parquetPath: String = Properties.getTSG2PubblicazioneAmmissibilitaVPGPath
  override val partitionByColumns: List[String] = List(PubblicazioneAmmissibilitaVPGSchema.ANNOTERMICO)
  override val columns: List[String] = PubblicazioneAmmissibilitaVPGSchema.getValues
  override val partitionColumn: String = PubblicazioneAmmissibilitaVPGSchema.ANNOTERMICO

  def getPubblicazioneAmmissibilitaVPG(): DataFrame = {
    readTable
      .select(
        PubblicazioneAmmissibilitaVPGSchema.CARTELLA_CLOUD,
        PubblicazioneAmmissibilitaVPGSchema.CSV_FILE_NAME,
        PubblicazioneAmmissibilitaVPGSchema.LAST_MODIFIED
    )
  }

  def get(dfReportAmmissibilitaVPG: DataFrame, dfReportPubblicazioneAmmVPG: DataFrame): DataFrame = {
    val dfResult = dfReportAmmissibilitaVPG
      .join(dfReportPubblicazioneAmmVPG,
        dfReportAmmissibilitaVPG(ReportAmmissibilitaVPGSchema.nome_file) ===
          dfReportPubblicazioneAmmVPG(PubblicazioneAmmissibilitaVPGSchema.CSV_FILE_NAME), "inner")

    dfResult.withColumnRenamed(ReportAmmissibilitaVPGSchema.nome_file, PubblicazioneAmmissibilitaVPGSchema.CSV_FILE_NAME)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.data_amm, PubblicazioneAmmissibilitaVPGSchema.DATA_AMM)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.executionid, PubblicazioneAmmissibilitaVPGSchema.EXECUTIONID)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.annotermico, PubblicazioneAmmissibilitaVPGSchema.ANNOTERMICO)

    dfResult.selectExpr(PubblicazioneAmmissibilitaVPGSchema.getValues: _*).dropDuplicates()
    // FIXME: Da ottimizzare, serviva questa modifica alla tabella e, per motivi di tempo, abbiamo
    //    dovuto fare in questo modo.
  }

  def getAlreadyComputedVPGFiles(pubblicazioneAmmissibilitaVPG: DataFrame): RDD[AlreadyComputedFiles] = {
    pubblicazioneAmmissibilitaVPG
      .rdd
      .map(row =>
        AlreadyComputedFiles(
          fileName = row.getAs[String](PubblicazioneAmmissibilitaVPGSchema.CARTELLA_CLOUD) + row.getAs[String](PubblicazioneAmmissibilitaVPGSchema.CSV_FILE_NAME),
          lastModifiedDate = row.getAs[Long](PubblicazioneAmmissibilitaVPGSchema.LAST_MODIFIED)
        ))
      .distinct()
  }

  def getAlreadyTransmittedVPG(reportAmmissibilita: DataFrame): RDD[String] = {
    reportAmmissibilita
      .select(PubblicazioneAmmissibilitaVPGSchema.CSV_FILE_NAME)
      .rdd
      .map(_.getAs[String](PubblicazioneAmmissibilitaVPGSchema.CSV_FILE_NAME))
      .distinct
  }
}
