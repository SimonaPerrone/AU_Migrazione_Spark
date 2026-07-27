package it.eng.au.ammissibilitaSettlementGas.dao

import it.eng.au.ammissibilitaSettlementGas.dao.`trait`.OutputDAO
import it.eng.au.ammissibilitaSettlementGas.schema.ReportAmmissibilitaTFCSchema
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.schema.TSGTFCSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class TSGTFCDao extends OutputDAO {
  override val tableName: String = Properties.getTSG2TSGTFCTableName
  override val parquetPath: String = Properties.getTSG2TSGTFCPath
  override val partitionByColumns: List[String] = List(TSGTFCSchema.annomese, TSGTFCSchema.executionid, TSGTFCSchema.progressivo)
  override val columns: List[String] = TSGTFCSchema.getValues
  override val partitionColumn: String = TSGTFCSchema.executionid

  def get(dfReportAmmissibilitaTFC: DataFrame): DataFrame = {
    val dfResult = dfReportAmmissibilitaTFC
      //.filter(col(ReportAmmissibilitaTFCSchema.verifica_amm) === true)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.n_id_tsg2_file, TSGTFCSchema.n_id_TSG2_file)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.nome_file, TSGTFCSchema.nome_file)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.data, TSGTFCSchema.data)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.id_reg_clim, TSGTFCSchema.id_reg_clim)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.wkr, TSGTFCSchema.wkr)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.verifica_amm, TSGTFCSchema.verifica_amm)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.cod_causale, TSGTFCSchema.cod_causale)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.motivazione, TSGTFCSchema.motivazione)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.executionid, TSGTFCSchema.executionid)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.annomese, TSGTFCSchema.annomese)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.progressivo, TSGTFCSchema.progressivo)
      //.withColumnRenamed(ReportAmmissibilitaTFCSchema.)
      //.withColumnRenamed(ReportAmmissibilitaTFCSchema.progressivo, TSGTFCSchema.progressivo)

    dfResult.selectExpr(TSGTFCSchema.getValues: _*)
  }
  /*
  def get(dfReportAmmissibilitaTFC: DataFrame, dfReportPubblicazioneAmmTFC: DataFrame): DataFrame = {
    val dfResult = dfReportAmmissibilitaTFC
      .join(dfReportPubblicazioneAmmTFC,
        dfReportAmmissibilitaTFC(ReportAmmissibilitaTFCSchema.nome_file) ===
          dfReportPubblicazioneAmmTFC(PubblicazioneAmmissibilitaTFCSchema.CSV_FILE_NAME), "inner")

    dfResult.withColumnRenamed(ReportAmmissibilitaTFCSchema.nome_file, PubblicazioneAmmissibilitaTFCSchema.CSV_FILE_NAME)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.verifica_amm, PubblicazioneAmmissibilitaTFCSchema.VERIFICA_AMM)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.cod_causale, PubblicazioneAmmissibilitaTFCSchema.COD_CAUSALE)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.motivazione, PubblicazioneAmmissibilitaTFCSchema.MOTIVAZIONE)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.numero_riga, PubblicazioneAmmissibilitaTFCSchema.NUMERO_RIGA)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.data_amm, PubblicazioneAmmissibilitaTFCSchema.DATA_AMM)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.executionid, PubblicazioneAmmissibilitaTFCSchema.EXECUTIONID)
      .withColumnRenamed(ReportAmmissibilitaTFCSchema.annomese, PubblicazioneAmmissibilitaTFCSchema.ANNOMESE)

    dfResult.selectExpr(PubblicazioneAmmissibilitaTFCSchema.getValues: _*)
  }
   */
}
