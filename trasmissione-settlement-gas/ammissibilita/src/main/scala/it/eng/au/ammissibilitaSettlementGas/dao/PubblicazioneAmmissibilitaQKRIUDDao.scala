package it.eng.au.ammissibilitaSettlementGas.dao

import it.eng.au.ammissibilitaSettlementGas.dao.`trait`.OutputDAO
import it.eng.au.ammissibilitaSettlementGas.model.AlreadyComputedFiles
import it.eng.au.ammissibilitaSettlementGas.schema.{PubblicazioneAmmissibilitaQKRIUDSchema, ReportAmmissibilitaQKRIUDSchema}
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import java.sql.Timestamp

class PubblicazioneAmmissibilitaQKRIUDDao extends OutputDAO {
  override val tableName: String = Properties.getTSG2PubblicazioneAmmissibilitaQKRIUDTableName
  override val parquetPath: String = Properties.getTSG2PubblicazioneAmmissibilitaQKRIUDPath
  override val partitionByColumns: List[String] = List(PubblicazioneAmmissibilitaQKRIUDSchema.ANNOMESE)
  override val columns: List[String] = PubblicazioneAmmissibilitaQKRIUDSchema.getValues
  override val partitionColumn: String = PubblicazioneAmmissibilitaQKRIUDSchema.ANNOMESE

  def get(dfReportAmmissibilitaQKRIUD: DataFrame, dfReportPubblicazioneAmmQKRIUD: DataFrame): DataFrame = {
    val dfResult = dfReportAmmissibilitaQKRIUD
      .join(dfReportPubblicazioneAmmQKRIUD.drop(PubblicazioneAmmissibilitaQKRIUDSchema.DATA_AMM),
        dfReportAmmissibilitaQKRIUD(ReportAmmissibilitaQKRIUDSchema.nome_file) ===
          dfReportPubblicazioneAmmQKRIUD(PubblicazioneAmmissibilitaQKRIUDSchema.CSV_FILE_NAME), "inner")

    dfResult
      .withColumnRenamed(ReportAmmissibilitaQKRIUDSchema.nome_file, PubblicazioneAmmissibilitaQKRIUDSchema.CSV_FILE_NAME)
      .withColumnRenamed(ReportAmmissibilitaQKRIUDSchema.data_amm, PubblicazioneAmmissibilitaQKRIUDSchema.DATA_AMM)
      .withColumnRenamed(ReportAmmissibilitaQKRIUDSchema.executionid, PubblicazioneAmmissibilitaQKRIUDSchema.EXECUTIONID)
      .withColumnRenamed(ReportAmmissibilitaQKRIUDSchema.annomese, PubblicazioneAmmissibilitaQKRIUDSchema.ANNOMESE)

    dfResult.selectExpr(PubblicazioneAmmissibilitaQKRIUDSchema.getValues: _*).dropDuplicates()
  }

  def getPubblicazioneAmmissibilitaQKRIUD: DataFrame = {
    readTable
      .select(
        PubblicazioneAmmissibilitaQKRIUDSchema.CARTELLA_CLOUD,
        PubblicazioneAmmissibilitaQKRIUDSchema.CSV_FILE_NAME,
        PubblicazioneAmmissibilitaQKRIUDSchema.DATA_AMM
      )
  }

  def getAlreadyComputedQKRIUDFiles(pubblicazioneAmmissibilitaQKRIUD: DataFrame): RDD[AlreadyComputedFiles] = {
    pubblicazioneAmmissibilitaQKRIUD
      .rdd
      .map(row =>
        AlreadyComputedFiles(
          fileName = row.getAs[String](PubblicazioneAmmissibilitaQKRIUDSchema.CARTELLA_CLOUD) + row.getAs[String](PubblicazioneAmmissibilitaQKRIUDSchema.CSV_FILE_NAME),
          lastModifiedDate = row.getAs[Timestamp](PubblicazioneAmmissibilitaQKRIUDSchema.DATA_AMM).getTime
        ))
      .distinct()
  }

  def getAlreadyTransmittedQKRIUD(reportAmmissibilita: DataFrame): RDD[String] = {
    reportAmmissibilita
      .select(PubblicazioneAmmissibilitaQKRIUDSchema.CSV_FILE_NAME)
      .rdd
      .map(_.getAs[String](PubblicazioneAmmissibilitaQKRIUDSchema.CSV_FILE_NAME))
      .distinct
  }
}