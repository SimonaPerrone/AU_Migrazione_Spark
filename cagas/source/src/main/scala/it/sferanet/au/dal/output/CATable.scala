package it.sferanet.au.dal.output

import it.sferanet.au.model._
import it.sferanet.au.model.caFinal.TechInfoCa
import it.sferanet.au.schema.CaOutputSchema
import it.sferanet.au.utilities.Environment
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.Timestamp

class CATable(session: String, executionId: Long) extends Serializable {

  private val outputPath = Environment.getCaDatasetPath
  private val tempOutputPath = Environment.getCaDatasetPath ++ "_tmp"
  private val tempOutputPath2 = Environment.getCaDatasetPath ++ "_tmp2"

  def parseCaToDF(ca: RDD[(String, Iterable[(Consumption, CAErrorCode.Value, CaParameter)], ProfStdMode.Value, Option[TechInfoCa])]): DataFrame = {
    val caMapped = ca.flatMap { case (pdr, caValues, profMode, techInfo) =>
      caValues.map(i => (pdr, i._1, i._2, i._3, profMode, techInfo))
    }.map(c =>
      Ca(
        pdr = c._1,
        startService = c._2.startService,
        endService = c._2.endService,
        startSegment = new Timestamp(c._2.startSegment.getTime),
        endSegment = new Timestamp(c._2.endSegment.getTime),
        startValue = c._2.startvalue,
        endValue = c._2.endvalue,
        idConsumptionErrorState = c._2.idConsumptionErrorState.id,
        idCaErrorCode = c._3.id,
        caMethods = c._4.caMethods.id,
        codiceProfilo = c._4.codiceProfilo,
        id_regClim = c._4.id_regClim,
        t_comune_istat_pdr = c._4.t_comune_istat_pdr,
        next_cod_profilo = c._4.profStdLastRcu,
        profMode = c._5.id,

        start_local_file = c._2.startLocalFile,
        end_local_file = c._2.endLocalFile,
        start_t_misuratore_integrato = c._2.startTMisuratoreIntegrato,
        end_t_misuratore_integrato = c._2.endTTMisuratoreIntegrato,
        start_t_pre_conv = c._2.startTPreConv,
        end_t_pre_conv = c._2.endTPreConv,
        n_coeff_correzione = c._2.nCoeffCorrezione.map(_.toString),
        cod_istat_last_rcu = c._6.map(_.cod_istat_last_rcu),
        zona_climatica_lookup = c._6.flatMap(_.zona_climatica_lookup),
        ce_mean = c._6.flatMap(_.ce_mean),
        session = session,
        executionid = executionId
      )
    )

    Environment.getSqlContext.createDataFrame(caMapped)
  }

  def write(df: DataFrame): Unit = {
    df
      .coalesce(300)
      .selectExpr(CaOutputSchema.getValues: _*)
      .write
      .partitionBy(CaOutputSchema.session, CaOutputSchema.executionid)
      .mode(SaveMode.Append)
      .parquet(outputPath)

    if(!Environment.isLocalMode) Environment.getSpark.sql(s"MSCK REPAIR TABLE ${Environment.getCaTable}")
  }

  def writeTempTable(df: DataFrame): Unit = {
    df
      .coalesce(300)
      .write
      .partitionBy(CaOutputSchema.session, CaOutputSchema.executionid)
      .mode(SaveMode.Append)
      .parquet(tempOutputPath)
  }

  def writeTempTable2(df: DataFrame): Unit = {
    df
      .coalesce(300)
      .write
      .partitionBy(CaOutputSchema.session, CaOutputSchema.executionid)
      .mode(SaveMode.Append)
      .parquet(tempOutputPath2)
  }

  def readTmpTable(): DataFrame = {
    Environment.getSpark.read.parquet(tempOutputPath)
  }

  def readTmpTable2(): DataFrame = {
    Environment.getSpark.read.parquet(tempOutputPath2)
  }

  def readCaTable(): DataFrame = {
    Environment.getSpark.read.parquet(outputPath)
      .filter(col(CaOutputSchema.executionid) === Environment.executionId)
  }

  def deleteTemporaryTableContents(): Unit = {
    val fs = FileSystem.get(Environment.getSpark.sparkContext.hadoopConfiguration)

    def deleteChildren(pathStr: String): Unit = {
      val dir = new Path(pathStr + "/")
      if (fs.exists(dir)) {
        fs.listStatus(dir).foreach(st => fs.delete(st.getPath, true))
      }
    }

    deleteChildren(tempOutputPath)
    deleteChildren(tempOutputPath2)
  }

}