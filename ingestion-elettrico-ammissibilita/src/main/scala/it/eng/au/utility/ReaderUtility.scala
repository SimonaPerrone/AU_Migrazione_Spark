package it.eng.au.utility

import it.eng.au.schema._
import it.eng.au.utility.environment.Environment
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{coalesce, col, lit, upper}
import org.apache.spark.sql.{DataFrame, SQLContext}

import java.time.LocalDateTime

object ReaderUtility {
  val data_tappo_min = "1900-01-01 00:00:00.0"
  val data_tappo_max = "9999-12-31 00:00:00.0"
  def readAndPrepareRcuAzienda: DataFrame = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val rcuAziendaPName: String = PropertyUtility.getRcuAziendaPTable
    SQLContext.table(rcuAziendaPName)
      .select(RcuAziendaPSchema.n_id_azienda, RcuAziendaPSchema.t_piva)
  }

  def readAndPrepareRcuPodStato(rcuPod: DataFrame): DataFrame = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val rcuPodStatoPName: String = PropertyUtility.getRcuPodStatoPTable

    SQLContext.table(rcuPodStatoPName)
      .selectExpr(RcuPodStatoPSchema.getValues: _*)
      .join(rcuPod, Seq("n_id_pod"))
      .select(
        RcuPodPSchema.t_codice_pod,
        RcuPodStatoPSchema.d_attivazione,
        RcuPodStatoPSchema.d_disattivazione,
        RcuPodStatoPSchema.t_stato_attivazione
      )
  }

  def readAndPrepareRcuUddP: DataFrame = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val rcuUddPName: String = PropertyUtility.getRcuUddPTable
    SQLContext.table(rcuUddPName)
      .select(RcuUddPSchema.n_id_udd, RcuUddPSchema.d_inizio, RcuUddPSchema.d_fine, RcuUddPSchema.t_codice_terna)
  }

  def readAndPrepareRcusUddP: DataFrame = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val rcuUddPName: String = PropertyUtility.getRcusUddPTable
    SQLContext.table(rcuUddPName)
      .select(RcusUddPSchema.n_id_udd, RcusUddPSchema.d_inizio, RcusUddPSchema.d_fine, RcusUddPSchema.t_codice_terna)
  }

  def readAndPrepareRcuDistrP: DataFrame = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val rcuDistrPTable: String = PropertyUtility.getRcuDistrPTable
    SQLContext.table(rcuDistrPTable)
      .filter(upper(col(RcuDistrPSchema.t_tipo)).isin("R", "S")) //filter is used by ID02 file rules smis
      .select(RcuDistrPSchema.n_id_distr)
  }

  def readAndPrepareRcuEmtP: DataFrame = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val rcuEmtPTable: String = PropertyUtility.getRcuEmtPTable
    SQLContext.table(rcuEmtPTable)
      .select(RcuEmtPSchema.n_id_emt)
  }

  def readAndPrepareRcusPod: DataFrame = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val rcusPodPTable: String = PropertyUtility.getRcusPodPTable
    SQLContext.table(rcusPodPTable)
      .filter(col(RcusPodPSchema.b_valido) === "N")
      .select(RcusPodPSchema.n_id_pod, RcusPodPSchema.t_codice_pod)
  }

  def readAndPrepareRcuPod: DataFrame = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val rcuPodPTable: String = PropertyUtility.getRcuPodPTable
    SQLContext.table(rcuPodPTable)
      .select(RcuPodPSchema.n_id_pod, RcuPodPSchema.t_codice_pod)
  }

  def readAndPrepareRcuPodDistr: DataFrame = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val rcuPodDistrPTable: String = PropertyUtility.getRcuPodDistrPTable
    SQLContext.table(rcuPodDistrPTable)
      .withColumn(RcuPodDistrPSchema.d_inizio, coalesce(col(RcuPodDistrPSchema.d_inizio), lit(data_tappo_min)))
      .withColumn(RcuPodDistrPSchema.d_fine, lit(data_tappo_max))
      .select(RcuPodDistrPSchema.n_id_pod, RcuPodDistrPSchema.n_id_distr, RcuPodDistrPSchema.d_inizio, RcuPodDistrPSchema.d_fine)
  }

  def readAndPrepareRcusPodDistr: DataFrame = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val rcusPodDistrPTable: String = PropertyUtility.getRcusPodDistrPTable
    SQLContext.table(rcusPodDistrPTable)
      .filter(col(RcusPodDistrPSchema.b_valido) === "N")
      .withColumn(RcusPodDistrPSchema.d_inizio, coalesce(col(RcusPodDistrPSchema.d_inizio), lit(data_tappo_min)))
      .withColumn(RcusPodDistrPSchema.d_fine, coalesce(col(RcusPodDistrPSchema.d_fine), lit(data_tappo_max)))
      .select(RcusPodDistrPSchema.n_id_pod, RcusPodDistrPSchema.n_id_distr, RcuPodDistrPSchema.d_inizio, RcusPodDistrPSchema.d_fine)
  }
  def readAndPrepareRcuPodUdd: DataFrame = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val rcuPodUddPTable: String = PropertyUtility.getRcuPodUddPTable
    SQLContext.table(rcuPodUddPTable)
      .withColumn(RcuPodUddPSchema.d_inizio, coalesce(col(RcuPodUddPSchema.d_inizio), lit(data_tappo_min)))
      .withColumn(RcuPodUddPSchema.d_fine, lit(data_tappo_max))
      .select(RcuPodUddPSchema.n_id_pod, RcuPodUddPSchema.n_id_udd, RcuPodUddPSchema.d_inizio, RcuPodUddPSchema.d_fine)
  }
  def readAndPrepareRcusPodUdd: DataFrame = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val rcusPodUddPTable: String = PropertyUtility.getRcusPodUddPTable
    SQLContext.table(rcusPodUddPTable)
      .filter(col(RcusPodUddPSchema.b_valido) === "N")
      .withColumn(RcusPodUddPSchema.d_inizio, coalesce(col(RcusPodUddPSchema.d_inizio), lit(data_tappo_min)))
      .withColumn(RcusPodUddPSchema.d_fine, coalesce(col(RcusPodUddPSchema.d_fine), lit(data_tappo_max)))
      .select(RcusPodUddPSchema.n_id_pod, RcusPodUddPSchema.n_id_udd, RcusPodUddPSchema.d_inizio, RcusPodUddPSchema.d_fine)
  }
}
