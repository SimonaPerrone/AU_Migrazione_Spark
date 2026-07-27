package it.sferanet.au.controller.caFinal

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.schema._
import it.sferanet.au.utilities.{Environment, HDFSUtils}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.Assert

import java.io.File

class CaFinalControllerTest extends EnvironmentSparkTest {
  private lazy val CaPreFinal: DataFrame = getCaPreFinal().cache()
  private lazy val distributore: DataFrame = getDistributore().cache()
  private lazy val azienda: DataFrame = getAzienda().cache()
  private lazy val udb: DataFrame = getUdb().cache()
  val sqlContext = Environment.getSqlContext

  import sqlContext.implicits._

  def testGet(): Unit = {
    val caFinalController = new CaFinalController
    val caFinal = caFinalController.get(CaPreFinal, distributore, azienda, udb).cache()
    caFinal.selectExpr(CaFinalSchema.getValues: _*).show(false)
    Assert.assertNotEquals(CaPreFinal.count(), caFinal.count())
    Assert.assertEquals(1, caFinal.where(col(CaFinalSchema.codice_pdr) === lit("5.3")).count())
    Assert.assertEquals(1, caFinal.where(col(CaPreFinalSchema.codice_pdr) === "5.3").where(col(CaPreFinalSchema.prelievo_annuo_prev) === lit("10159")).count)
    Assert.assertEquals(1, caFinal.where(col(CaFinalSchema.codice_pdr) === lit("5.2")).count())
    Assert.assertEquals(3, caFinal.where(col(CaFinalSchema.cod_prof_prel_std) === lit("C2X1")).count())
    Assert.assertEquals(1, caFinal.where(col(CaFinalSchema.piva_udd) === lit("pivaudd1")).count())
    Assert.assertEquals(1, caFinal.where(col(CaFinalSchema.piva_distr) === lit("pivadistr1")).count())
    Assert.assertEquals(1, caFinal.where(col(CaFinalSchema.piva_udb) === lit("pivaudb1")).count())

    Assert.assertEquals(1, caFinal.where(col(CaPreFinalSchema.codice_pdr) === "XXX").where(col(CaPreFinalSchema.prelievo_annuo_prev) === lit("620000000")).count)
  }

  def testWrite(): Unit = {
    val writeTestPath = "src/test/resources/AppTest/output/caFinal"
    val outDir = new File(writeTestPath)
    Environment.setProperty("ca_final.basepath", writeTestPath)
    HDFSUtils.deleteIfExist(writeTestPath)

    val caFinalController = new CaFinalController
    val caFinal = caFinalController.get(CaPreFinal, distributore, azienda, udb).cache()
    caFinalController.write(caFinal)

    val session = outDir.listFiles.filter(_.isDirectory)
    Assert.assertEquals(1, session.length)
    Assert.assertEquals("session=CDP", session(0).getName)

    val tipoTrasmissione = session(0).listFiles.filter(_.isDirectory)
    Assert.assertEquals(1, tipoTrasmissione.length)
    Assert.assertEquals("tipo_trasmissione=AGG_RIC", tipoTrasmissione(0).getName)

    val annoCompetenza = tipoTrasmissione(0).listFiles.filter(_.isDirectory)
    Assert.assertEquals(1, annoCompetenza.length)
    Assert.assertEquals("anno_competenza=2021", annoCompetenza(0).getName)

    val executionId = annoCompetenza(0).listFiles.filter(_.isDirectory)
    Assert.assertEquals(1, executionId.length)
    Assert.assertEquals("executionid=1612430890541", executionId(0).getName)

    val caFinalDeser = Environment.getSqlContext.read.parquet(writeTestPath)
    Assert.assertEquals(1, caFinalDeser.where(col(CaPreFinalSchema.codice_pdr) === "5.3").count)
    Assert.assertEquals(1, caFinalDeser.where(col(CaPreFinalSchema.codice_pdr) === "5.2").count)
    Assert.assertEquals(0, caFinalDeser.where(col(CaPreFinalSchema.anno_competenza) =!= "2021").count)
    Assert.assertEquals(caFinal.count, caFinalDeser.count)

  }

  private def getDistributore(): DataFrame = Environment.getSparkContext.parallelize(
    Seq((1L, "pivadistr1"),
      (2L, "pivadistr2"),
      (3L, "pivadistr3")
    )).toDF(RcugasDistributoreSchema.n_id_distributore, RcugasDistributoreSchema.t_piva)

  private def getAzienda(): DataFrame = Environment.getSparkContext.parallelize(
    Seq((11L, "pivaudd1"),
      (22L, "pivaudd2"),
      (33L, "pivaudd3"),
      (44L, "pivaudb1"),
      (55L, "pivaudb2"),
      (66L, "pivaudb3")
    )).toDF(RcuAziendaPSchema.n_id_azienda, RcuAziendaPSchema.t_piva)

  private def getUdb(): DataFrame = Environment.getSparkContext.parallelize(
    Seq((44L, 111L),
      (55L, 222L),
      (66L, 333L)
    )).toDF(RcuGasUdbPSchema.n_id_azienda, RcuGasUdbPSchema.n_id_udb)

  private def getCaPreFinal(): DataFrame = {
    val row = Row(10L, "2021", 1L, 11L, 111L, null, "5.3", "C3", "1", "E", null, "C3E1", null, "10159.5"
      , "2021-02-04", "PRE", null, null, null, null, "dedotto", null, null, null, null, null, null, null, null, null
      , null, null, null, null, "C2X1", null, "1612430890541")

    val row2 = Row(11L, "2021", 2L, 22L, 222L, null, "5.2", "C3", "1", "E", null, "C3E1", null, "10159.0"
      , "2021-02-04", "PRE", null, null, null, null, "dedotto", null, null, null, null, null, null, null, null, null
      , null, null, null, null, "C2X1", null, "1612430890541")

    val row3 = Row(12L, "2021", 3L, 33L, 333L, null, "XXX", "C3", "1", "E", null, "C3E1", null, "620000000"
      , "2021-02-04", "PRE", null, null, null, null, "dedotto", null, null, null, null, null, null, null, null, null
      , null, null, null, null, "C2X1", null, "1612430890541")

    val df = Environment.getSqlContext.createDataFrame(Environment.getSparkContext.parallelize(List(row, row, row, row2, row3)), caPreFinalST)
    df.withColumn("d_ricezione_tmp", col(CaPreFinalSchema.d_ricezione).cast(DateType))
      .drop(CaPreFinalSchema.d_ricezione)
      .withColumnRenamed("d_ricezione_tmp", CaPreFinalSchema.d_ricezione)
      .withColumn(CaPreFinalSchema.cat_uso_forced, lit("1"))
      .withColumn(CaPreFinalSchema.zona_climatica_forced, lit("X"))
      .withColumn(CaPreFinalSchema.classe_prelievo_forced, lit("C2"))
      .withColumn(CaPreFinalSchema.n_coeff_correzione, lit(null))
      .withColumn(CaPreFinalSchema.trattamento_forced, lit(null))
      .withColumn(CaPreFinalSchema.startSegment, lit(null).cast(TimestampType))
      .withColumn(CaPreFinalSchema.endSegment, lit(null).cast(TimestampType))
      .withColumn(CaPreFinalSchema.massivo_freeze_executionid, lit(1234567891234L))
      .withColumn(CaPreFinalSchema.freeze_date, lit("2022-01-01"))
      .withColumn(CaPreFinalSchema.session, lit(Environment.getSession))
      .withColumn(CaPreFinalSchema.tipo_trasmissione, lit(Environment.getTipoTrasmissione))
      .selectExpr(CaPreFinalSchema.getValues: _*)
  }

  private def caPreFinalST: StructType = StructType(
    StructField(CaPreFinalSchema.id_sag_ann, LongType) ::
      StructField(CaPreFinalSchema.anno_competenza, StringType) ::
      StructField(CaPreFinalSchema.n_id_distr, LongType) ::
      StructField(CaPreFinalSchema.n_id_az_udd, LongType) ::
      StructField(CaPreFinalSchema.n_id_udb, LongType) ::
      StructField(CaPreFinalSchema.codice_remi, StringType) ::
      StructField(CaPreFinalSchema.codice_pdr, StringType) ::
      StructField(CaPreFinalSchema.cap_trasp_pdr, StringType) ::
      StructField(CaPreFinalSchema.cat_uso, StringType) ::
      StructField(CaPreFinalSchema.classe_prelievo, StringType) ::
      StructField(CaPreFinalSchema.zona_climatica, StringType) ::
      StructField(CaPreFinalSchema.id_reg_clim, StringType) ::
      StructField(CaPreFinalSchema.cod_prof_prel_std, StringType) ::
      StructField(CaPreFinalSchema.prelievo_annuo_prev, StringType) ::
      StructField(CaPreFinalSchema.trattamento, StringType) ::
      StructField(CaPreFinalSchema.d_ricezione, StringType) ::
      StructField(CaPreFinalSchema.tipo_trasmissione, StringType) ::
      StructField(CaPreFinalSchema.codistat, StringType) ::
      StructField(CaPreFinalSchema.id_ca_error_code, IntegerType) ::
      StructField(CaPreFinalSchema.start_local_file, StringType) ::
      StructField(CaPreFinalSchema.end_local_file, StringType) ::
      StructField(CaPreFinalSchema.calcmode, StringType) ::
      StructField(CaPreFinalSchema.start_t_misuratore_integrato, StringType) ::
      StructField(CaPreFinalSchema.end_t_misuratore_integrato, StringType) ::
      StructField(CaPreFinalSchema.start_t_pre_conv, StringType) ::
      StructField(CaPreFinalSchema.end_t_pre_conv, StringType) ::
      StructField(CaPreFinalSchema.pres_tds, BooleanType) ::
      StructField(CaPreFinalSchema.tipologia_uso, BooleanType) ::
      StructField(CaPreFinalSchema.comp_termica, BooleanType) ::
      StructField(CaPreFinalSchema.cat_uso_tds, StringType) ::
      StructField(CaPreFinalSchema.classe_prelievo_tds, StringType) ::
      StructField(CaPreFinalSchema.cod_istat_last_rcu, StringType) ::
      StructField(CaPreFinalSchema.zona_climatica_lookup, StringType) ::
      StructField(CaPreFinalSchema.prelievo_annuo_prev_forced, StringType) ::
      StructField(CaPreFinalSchema.cod_prof_prel_std_forced, StringType) ::
      StructField(CaPreFinalSchema.is_ca_calculated, BooleanType) ::
      StructField(CaPreFinalSchema.executionid, StringType) ::
      Nil)
}
