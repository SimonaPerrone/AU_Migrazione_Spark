package it.eng.au.ammissibilitaSettlementGas.dao

import it.eng.au.ammissibilitaSettlementGas.dao.`trait`.OutputDAO
import it.eng.au.ammissibilitaSettlementGas.schema.ReportAmmissibilitaQKRIUDSchema
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.schema.TSGQKRIUDSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_unixtime, unix_timestamp}

class TSGQKRIUDDao extends OutputDAO {
  override val tableName: String = Properties.getTSG2TSGQKRIUDTableName
  override val parquetPath: String = Properties.getTSG2TSGQKRIUDPath
  override val partitionByColumns: List[String] = List(TSGQKRIUDSchema.annomese, TSGQKRIUDSchema.executionid, TSGQKRIUDSchema.progressivo)
  override val columns: List[String] = TSGQKRIUDSchema.getValues
  override val partitionColumn: String = TSGQKRIUDSchema.executionid

  def get(dfReportAmmissibilitaQKRIUD: DataFrame): DataFrame = {
    val dfResult = dfReportAmmissibilitaQKRIUD
      //.filter(col(ReportAmmissibilitaQKRIUDSchema.verifica_amm) === true)
      .withColumnRenamed(ReportAmmissibilitaQKRIUDSchema.n_id_tsg2_file, TSGQKRIUDSchema.n_id_TSG2_file)
      .withColumnRenamed(ReportAmmissibilitaQKRIUDSchema.nome_file, TSGQKRIUDSchema.nome_file)
      .withColumnRenamed(ReportAmmissibilitaQKRIUDSchema.data, TSGQKRIUDSchema.data)
      .withColumnRenamed(ReportAmmissibilitaQKRIUDSchema.cod_remi, TSGQKRIUDSchema.cod_remi)
      .withColumnRenamed(ReportAmmissibilitaQKRIUDSchema.qkriud, TSGQKRIUDSchema.qkriud)
      .withColumnRenamed(ReportAmmissibilitaQKRIUDSchema.verifica_amm, TSGQKRIUDSchema.verifica_amm)
      .withColumnRenamed(ReportAmmissibilitaQKRIUDSchema.cod_causale, TSGQKRIUDSchema.cod_causale)
      .withColumnRenamed(ReportAmmissibilitaQKRIUDSchema.motivazione, TSGQKRIUDSchema.motivazione)
      .withColumnRenamed(ReportAmmissibilitaQKRIUDSchema.annomese, TSGQKRIUDSchema.annomese)
      .withColumnRenamed(ReportAmmissibilitaQKRIUDSchema.executionid, TSGQKRIUDSchema.executionid)
      .withColumnRenamed(ReportAmmissibilitaQKRIUDSchema.progressivo, TSGQKRIUDSchema.progressivo)
      .withColumn(TSGQKRIUDSchema.annomese, from_unixtime(unix_timestamp(col(ReportAmmissibilitaQKRIUDSchema.data), "dd/MM/yyyy"), "yyyyMM"))

    dfResult.selectExpr(TSGQKRIUDSchema.getValues: _*)
  }
}
