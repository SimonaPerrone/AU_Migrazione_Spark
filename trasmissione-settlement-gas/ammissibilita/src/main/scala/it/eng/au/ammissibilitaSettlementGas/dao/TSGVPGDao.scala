package it.eng.au.ammissibilitaSettlementGas.dao

import it.eng.au.ammissibilitaSettlementGas.dao.`trait`.OutputDAO
import it.eng.au.ammissibilitaSettlementGas.schema.ReportAmmissibilitaVPGSchema
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.schema.TSGVPGSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_unixtime, lit, unix_timestamp}

class TSGVPGDao extends OutputDAO {
  override val tableName: String = Properties.getTSG2TSGVPGTableName
  override val parquetPath: String = Properties.getTSG2TSGVPGPath
  override val partitionByColumns: List[String] = List(TSGVPGSchema.annotermico, TSGVPGSchema.annomese, TSGVPGSchema.executionid, TSGVPGSchema.progressivo)
  override val columns: List[String] = TSGVPGSchema.getValues
  override val partitionColumn: String = TSGVPGSchema.executionid

  def get(dfReportAmmissibilitaVPG: DataFrame): DataFrame = {
    val dfResult = dfReportAmmissibilitaVPG
      //.filter(col(ReportAmmissibilitaVPGSchema.verifica_amm) === true)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.n_id_tsg2_file, TSGVPGSchema.n_id_TSG2_file)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.giorno_riferimento, TSGVPGSchema.giorno_riferimento)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C1_A1, TSGVPGSchema.C1_A1)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C1_B1, TSGVPGSchema.C1_B1)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C1_C1, TSGVPGSchema.C1_C1)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C1_E1, TSGVPGSchema.C1_E1)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C1_F1, TSGVPGSchema.C1_F1)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C1_A2, TSGVPGSchema.C1_A2)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C1_B2, TSGVPGSchema.C1_B2)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C1_C2, TSGVPGSchema.C1_C2)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C1_E2, TSGVPGSchema.C1_E2)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C1_F2, TSGVPGSchema.C1_F2)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C1_A3, TSGVPGSchema.C1_A3)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C1_B3, TSGVPGSchema.C1_B3)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C1_C3, TSGVPGSchema.C1_C3)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C1_E3, TSGVPGSchema.C1_E3)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C1_F3, TSGVPGSchema.C1_F3)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C2, TSGVPGSchema.C2)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.C4, TSGVPGSchema.C4)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.T1_1, TSGVPGSchema.T1_1)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.T1_2, TSGVPGSchema.T1_2)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.T1_3, TSGVPGSchema.T1_3)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.verifica_amm, TSGVPGSchema.verifica_amm)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.cod_causale, TSGVPGSchema.cod_causale)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.motivazione, TSGVPGSchema.motivazione)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.executionid, TSGVPGSchema.executionid)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.annotermico, TSGVPGSchema.annotermico)
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.progressivo, TSGVPGSchema.progressivo)
      .withColumn(TSGVPGSchema.annomese, from_unixtime(unix_timestamp(col(ReportAmmissibilitaVPGSchema.giorno_riferimento), "dd/MM/yyyy"), "yyyyMM"))
      .withColumnRenamed(ReportAmmissibilitaVPGSchema.nome_file, TSGVPGSchema.nome_file)

    dfResult.selectExpr(TSGVPGSchema.getValues:_*)
  }
}
