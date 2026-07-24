package it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa

import it.eng.au.aggregatoreConsumiCdp.dao.cdp.{CaFinalDao, RcuAziendaDao, RcugasDistributoreDao, RcugasUdbDao}
import it.eng.au.aggregatoreConsumiCdp.schema._
import it.eng.au.aggregatoreConsumiCdp.utility.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp

trait FlowCdpDatiPrelievoGas {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def getDateToRun: String = Environment.dateRun
  def getCurrentThermalYear: String = Environment.dateCurrentThermalYear
  def getHdfsOutputBasepathCsv: String = Environment.getHDFSCsvOutputPath
  def getHdfsOutputBasepathCsvExport: String = Environment.getHDFSCsvExportOutputPath
  def getCaExecutionId: String = Environment.getCaFinalExecutionId

  def specificTransform(caFinal: DataFrame): DataFrame

  def runCdpDatiPrelievoGas(): DataFrame = {
    val distributore = new RcugasDistributoreDao().readTable
    val azienda = new RcuAziendaDao().readTable
    val udb = new RcugasUdbDao().readTable
    val prepCa = getCa
    val executionId = Environment.executionId

    run(prepCa, distributore, broadcast(azienda), udb, executionId)
  }

  def run(caFinal: DataFrame, distributore: DataFrame, azienda: DataFrame, udb: DataFrame, executionId: Long): DataFrame = {
    val prepCaFinal = prepare(caFinal)

    val resTransform = transform(prepCaFinal, distributore, broadcast(azienda), udb, executionId)

    val resSpecificTransform = specificTransform(resTransform)

    resSpecificTransform.persist(StorageLevel.MEMORY_AND_DISK)
    writeCaOnHive(resSpecificTransform)
    resSpecificTransform
  }

  def getCa: DataFrame = {
    val caFinal = new CaFinalDao().readPartition(Environment.getCaFinalExecutionId)
    caFinal
  }

  def transform(caFinal: DataFrame, distributore: DataFrame, azienda: DataFrame, udb: DataFrame, executionId: Long): DataFrame = {
    addPiva(caFinal, distributore, azienda, udb)
      .withColumn(OutputHiveSchema.causale, lit(""))
      .withColumn(OutputHiveSchema.execution_id, lit(executionId)
      )
  }

  def addPiva(caFinalWithPiva: DataFrame, distributore: DataFrame, azienda: DataFrame, udb: DataFrame): DataFrame = {
    val udbRecPiva = udb
      .join(azienda, udb(RcugasUdbSchema.n_id_azienda) === azienda(RcugasAziendaSchema.n_id_azienda), "inner")
      .select(udb(RcugasUdbSchema.n_id_udb).alias("n_id_udb_to_drop"), azienda(RcugasAziendaSchema.t_piva))

    val caFinal = caFinalWithPiva
      .drop(col(CaFinalSchema.piva_distr))
      .drop(col(CaFinalSchema.piva_udd))
      .drop(col(CaFinalSchema.piva_udb))

    caFinal
      .join(broadcast(distributore), caFinal(OutputHiveSchema.n_id_distr) === distributore(RcugasDistributoreSchema.n_id_distributore), "left")
      .drop(col(RcugasDistributoreSchema.n_id_distributore))
      .withColumnRenamed(RcugasDistributoreSchema.t_piva, OutputHiveSchema.piva_distr)

      .join(azienda, caFinal(OutputHiveSchema.n_id_udd) === azienda(RcugasAziendaSchema.n_id_azienda), "left")
      .drop(col(RcugasAziendaSchema.n_id_azienda))
      .withColumnRenamed(RcugasAziendaSchema.t_piva, OutputHiveSchema.piva_udd)

      .join(broadcast(udbRecPiva), caFinal(OutputHiveSchema.n_id_udb) === udbRecPiva("n_id_udb_to_drop"), "left")
      .drop(col("n_id_udb_to_drop"))
      .withColumnRenamed(RcugasAziendaSchema.t_piva, OutputHiveSchema.piva_udb)

      .select(
        caFinal("*"),
        col(OutputHiveSchema.piva_distr),
        col(OutputHiveSchema.piva_udd),
        col(OutputHiveSchema.piva_udb)
      )
  }

  def prepare(caFinal: DataFrame): DataFrame = {
    caFinal
      .withColumnRenamed(CaFinalSchema.codice_pdr, OutputHiveSchema.cod_pdr)
      .withColumnRenamed(CaFinalSchema.codice_remi, OutputHiveSchema.cod_remi)
      .withColumnRenamed(CaFinalSchema.n_id_az_udd, OutputHiveSchema.n_id_udd)
      .withColumnRenamed(CaFinalSchema.massivo_freeze_executionid, OutputHiveSchema.massivo_freezer_executiond_id)
      .withColumnRenamed(CaFinalSchema.freeze_date, OutputHiveSchema.d_data_rif)
      .withColumnRenamed(CaFinalSchema.executionid, OutputHiveSchema.calc_executiond_id)

  }

  def writeCaOnHive(df: DataFrame): DataFrame = {
    val hdfsOutputBasepathCsv = getHdfsOutputBasepathCsv
    val hdfsOutputBasepathExport = getHdfsOutputBasepathCsvExport

    val res = df
      .withColumnRenamed(OutputHiveSchema.n_id_udd, CdpDatiPrelievoGasSchema.n_id_az_udd)
      .withColumnRenamed(OutputHiveSchema.cod_pdr, CdpDatiPrelievoGasSchema.codice_pdr)
      .withColumnRenamed(OutputHiveSchema.cod_remi, CdpDatiPrelievoGasSchema.codice_remi)
      .withColumnRenamed(OutputHiveSchema.data_decorrenza, CdpDatiPrelievoGasSchema.d_data_competenza)
      .withColumnRenamed(OutputHiveSchema.execution_id, CdpDatiPrelievoGasSchema.executionid)
      .withColumn(CdpDatiPrelievoGasSchema.calc_executiond_id, col(CdpDatiPrelievoGasSchema.calc_executiond_id).cast(StringType))
      .withColumn(CdpDatiPrelievoGasSchema.massivo_freezer_executiond_id, col(CdpDatiPrelievoGasSchema.massivo_freezer_executiond_id).cast(StringType))
      .withColumn(CdpDatiPrelievoGasSchema.d_data_competenza, col(CdpDatiPrelievoGasSchema.d_data_competenza).cast(TimestampType))
      .selectExpr(CdpDatiPrelievoGasSchema.getValues: _*)

    res
      .write.partitionBy(CdpDatiPrelievoGasSchema.anno_competenza, CdpDatiPrelievoGasSchema.executionid).mode(SaveMode.Append).parquet(hdfsOutputBasepathCsv)

    res
      .write.mode(SaveMode.Overwrite).parquet(hdfsOutputBasepathExport)

    if (!Environment.isLocalMode) Environment.spark.sql(s"MSCK REPAIR TABLE ${Environment.getCdpDatiPrelievoGasTableName}")

    res
  }
}
