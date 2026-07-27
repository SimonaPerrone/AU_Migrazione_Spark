package it.sferanet.au.controller.caFinal

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.schema.{CaPreFinalSchema, CaSchema, PdrMassivoSchema, SettleGasGasTdsSchema}
import it.sferanet.au.utilities.{Constants, Environment, HDFSUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.junit.Assert

import java.io.File
import java.sql.Timestamp
import java.util.Date

class CaPreFinalControllerTest extends EnvironmentSparkTest {
  def testGet(): Unit = {

    val (pdrMassivoDF, caDF, tdsForPresTds, tdsForDedotti) = getDataFrames()

    val caPreFinalController = new CaPreFinalController(Environment.executionId.toString, new Date().getTime)
    val caPreFinal = caPreFinalController.get(pdrMassivoDF, caDF, tdsForPresTds, tdsForDedotti).cache()
    caPreFinal.show(false)

    Assert.assertEquals(3, caPreFinal.where(col(CaPreFinalSchema.is_ca_calculated) === true).count())

    Assert.assertEquals(2, caPreFinal.where(col(CaPreFinalSchema.codice_pdr) === "05260000050549").count())

    Assert.assertEquals(3, caPreFinal.where(col(CaPreFinalSchema.cat_uso) === "YY").count())
    Assert.assertEquals(3, caPreFinal.where(col(CaPreFinalSchema.cat_uso) === "XX").count())

    Assert.assertEquals(3, caPreFinal.where(col(CaPreFinalSchema.classe_prelievo) === "Y").count())
    Assert.assertEquals(3, caPreFinal.where(col(CaPreFinalSchema.classe_prelievo) === "X").count())

    Assert.assertEquals(3, caPreFinal.where(col(CaPreFinalSchema.zona_climatica) === "A").count())
    Assert.assertEquals(3, caPreFinal.where(col(CaPreFinalSchema.zona_climatica) === "Y").count())

    Assert.assertEquals(3, caPreFinal.where(col(CaPreFinalSchema.id_reg_clim) === "12").count())
    Assert.assertEquals(2, caPreFinal.where(col(CaPreFinalSchema.id_reg_clim) === "1").count())
    Assert.assertEquals(0, caPreFinal.where(col(CaPreFinalSchema.id_reg_clim) === "2").count())
    Assert.assertEquals(1, caPreFinal.where(col(CaPreFinalSchema.id_reg_clim) === "5").count())

    Assert.assertEquals(3, caPreFinal.where(col(CaPreFinalSchema.cod_prof_prel_std).startsWith("XXX")).count())
    Assert.assertEquals(3, caPreFinal.where(col(CaPreFinalSchema.cod_prof_prel_std).startsWith("YYY")).count())

    Assert.assertEquals(1, caPreFinal.where(col(CaPreFinalSchema.prelievo_annuo_prev) === "620000000").count())
    Assert.assertEquals(1, caPreFinal.where(col(CaPreFinalSchema.prelievo_annuo_prev) === "200").count())
    Assert.assertEquals(1, caPreFinal.where(col(CaPreFinalSchema.prelievo_annuo_prev) === "101").count())
    Assert.assertEquals(3, caPreFinal.where(col(CaPreFinalSchema.prelievo_annuo_prev) === "0").count())

    //TESTING CaFinal.calcmode
    Assert.assertEquals(2, caPreFinal.where(col(CaPreFinalSchema.calcmode) === Constants.CALCMOD_DEDOTTO).count())
    Assert.assertEquals(2, caPreFinal.where(col(CaPreFinalSchema.calcmode) === Constants.CALCMOD_PROCEDURA).count())

    Environment.setProperty("filterPdr.mode", "forzatura")
    Assert.assertEquals(Constants.FILTER_MODE_FORZATURA, Environment.getFilterPdrMode)
    val caFinalForzatura = caPreFinalController.get(pdrMassivoDF, caDF, tdsForPresTds, tdsForDedotti).cache()
    caFinalForzatura.show(false)
    Assert.assertEquals(4, caFinalForzatura.where(col(CaPreFinalSchema.calcmode) === Constants.CALCMOD_FORZATO).count())

    //TESTING id_ca_error_code
    Assert.assertEquals(caDF.where(col(CaSchema.idCaErrorCode) === 0).count(), caPreFinal.where(col(CaPreFinalSchema.id_ca_error_code) === 0).count())
  }

  def testWrite(): Unit = {
    val writeTestPath = "src/test/resources/AppTest/output/caPreFinal"
    val outDir = new File(writeTestPath)
    val executionId: Long = 160396
    Environment.setProperty("ca_pre_final.basepath", writeTestPath)
    val (pdrMassivoDF, caDF, tdsForPresTds, tdsForDedotti) = getDataFrames()
    val caFinalController = new CaPreFinalController(Environment.getSession, executionId)
    val caFinal = caFinalController.get(pdrMassivoDF, caDF, tdsForPresTds, tdsForDedotti).cache()
    HDFSUtils.deleteIfExist(writeTestPath)
    caFinalController.write(caFinal.withColumn("trattamento_forced", lit("")))
    //Check for correct partitioning
    val subDir = outDir
      .listFiles.filter(_.isDirectory)
      .flatMap(_.listFiles.filter(_.isDirectory))
      .flatMap(_.listFiles.filter(_.isDirectory))

    Assert.assertEquals(1, subDir.length)
    Assert.assertEquals("anno_competenza=2020", subDir(0).getName)

    val subSubDir = subDir(0).listFiles.filter(_.isDirectory)
    Assert.assertEquals(1, subSubDir.length)
    Assert.assertEquals("executionid=160396", subSubDir(0).getName)
    //Check for correct content
    val caPreFinalDeser = Environment.getSqlContext.read.parquet(writeTestPath)
    Assert.assertEquals(1, caPreFinalDeser.where(col(CaPreFinalSchema.codice_pdr) === "05260000050547").count)
    Assert.assertEquals(1, caPreFinalDeser.where(col(CaPreFinalSchema.codice_pdr) === "05260000050548").count)
    Assert.assertEquals(2, caPreFinalDeser.where(col(CaPreFinalSchema.codice_pdr) === "05260000050549").count)
    Assert.assertEquals(1, caPreFinalDeser.where(col(CaPreFinalSchema.codice_pdr) === "05260000050550").count)
    Assert.assertEquals(0, caPreFinalDeser.where(col(CaPreFinalSchema.anno_competenza) =!= "2020").count)
  }

  def testAddForcedColumnsLogic(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    val dummyCaFinal = List(
      ("pdrNull", "10.0000", null,    null,   null,   null, null, null, null, null, null),
      ("pdr1",    "10.0000", "1.000", "C1D1", "C3C1", "C1", "C3", "D",  "C",  "1",  "1"),
      ("pdr1.1",  "10.0000", "1.000", "C1D1", "T1X1", "C1", "T1", "D",  "C",  "1",  "1"),
      ("pdr1.2",  "10.0000", "1.000", "C1D1", "C3C1", "C1", "C1", "D",  "C",  "1",  "1"),
      ("pdr2",    "10.0000", "1.000", null,   null,   null, null, null, null, null, null),
      ("pdr3",    "10.0000", "1.000", "C1D1", "T1X1", "C1", null, "D",  null, "1",  null),
      ("pdr3.1",  "10.0000", "1.000", "C1D1", "T1X1", "C1", null, "D",  "C",  "1",  null),
      ("pdr3.2",  "10.0000", "1.000", "C1D1", "T2E3", "C1", null, "D",  null, "1",  null),
      ("pdr4",    "10.0000", "1.000", "C3B1", null,   "C3", "C1", "B",  null, "1",  null),
      ("pdr4.1",  "10.0000", "1.000", "C3B1", null,   "C3", "C2", "B",  null, "1",  null),
      ("pdr5",    "10.0000", "1.000", "T1X1", null,   "T1", null, "D",  null, "1",  "3"),
      ("pdr6",    "10.0000", "1.000", "C1D1", null,   "C1", "C2", "D",  "C",  "1",  "1"),
      ("pdr6.1",  "10.0000", "1.000", "C2X1", null,   "C2", "C1", "D",  "D",  "1",  "1"),
      ("pdr6.2",  "10.0000", null,    "C1A1", null,   "C1", null, "A",  "B",  "1",  null),
      ("pdr6.3",  "10.0000", null,    "C2X1", null,   "C2", null, "D",  "C",  "1",  null),
      ("pdr6.4",  "10.0000", null,    "C2X1", null,   "C2", null, "D",  "C",  "1",  "3"),
      ("pdr6.5",  "10.0000", null,    "T1X1", null,   "T1", null, "D",  "C",  "1",  "3"),
      ("pdr7.1",  "10.0000", "1.000", "C2X1", null,   "C2", "C6", "D",  "D",  "1",  "1"), //setting cat_uso_forced to a not valid value
      ("pdr7.2",  "10.0000", "1.000", "C2X1", "C6X1", "C2", "C1", "D",  "D",  "1",  "1") //setting prelievo_annuo_prev_forced to a not valid value
    ).toDF(CaPreFinalSchema.codice_pdr,
      CaPreFinalSchema.prelievo_annuo_prev,
      CaPreFinalSchema.prelievo_annuo_prev_forced,
      CaPreFinalSchema.cod_prof_prel_std,
      CaPreFinalSchema.cod_prof_prel_std_forced,
      CaPreFinalSchema.cat_uso,
      CaPreFinalSchema.cat_uso_forced,
      CaPreFinalSchema.zona_climatica,
      CaPreFinalSchema.zona_climatica_forced,
      CaPreFinalSchema.classe_prelievo,
      CaPreFinalSchema.classe_prelievo_forced)

    val caFinalCols = CaPreFinalSchema.getValues.toSet
    val caDummyCols = dummyCaFinal.columns.toSet
    val missingColumns = caFinalCols.diff(caDummyCols)
    var dummyCaFinalWithAllCols = dummyCaFinal
    missingColumns.foreach(col => dummyCaFinalWithAllCols = dummyCaFinalWithAllCols.withColumn(col, lit(null)))

    val dummyCaFinalWithForcedLogic = new CaPreFinalController(Environment.getSession, 1.toLong).addForcedColumnsLogic(dummyCaFinalWithAllCols)
      .cache()

    dummyCaFinalWithForcedLogic.orderBy(CaPreFinalSchema.codice_pdr).show()

    Assert.assertEquals(1,
      dummyCaFinalWithForcedLogic.where(col(CaPreFinalSchema.codice_pdr) === lit("pdr1"))
        .where(col(CaPreFinalSchema.cod_prof_prel_std_forced) === lit("C3C1"))
        .where(col(CaPreFinalSchema.cat_uso_forced) === lit("C3"))
        .where(col(CaPreFinalSchema.zona_climatica_forced) === lit("C"))
        .where(col(CaPreFinalSchema.classe_prelievo_forced) === lit("1"))
        .count()
    )
    Assert.assertEquals(1,
      dummyCaFinalWithForcedLogic.where(col(CaPreFinalSchema.codice_pdr) === lit("pdr1.1"))
        .where(col(CaPreFinalSchema.cod_prof_prel_std_forced) === lit("T1X1"))
        .where(col(CaPreFinalSchema.cat_uso_forced) === lit("T1"))
        .where(col(CaPreFinalSchema.zona_climatica_forced) === lit("C"))
        .where(col(CaPreFinalSchema.classe_prelievo_forced) === lit("1"))
        .count()
    )
    Assert.assertEquals(1,
      dummyCaFinalWithForcedLogic.where(col(CaPreFinalSchema.codice_pdr) === lit("pdr1.2"))
        .where(col(CaPreFinalSchema.cod_prof_prel_std_forced) === lit("C3C1"))
        .where(col(CaPreFinalSchema.cat_uso_forced) === lit("C3"))
        .where(col(CaPreFinalSchema.zona_climatica_forced) === lit("C"))
        .where(col(CaPreFinalSchema.classe_prelievo_forced) === lit("1"))
        .count()
    )
    Assert.assertEquals(2,
      dummyCaFinalWithForcedLogic.where(col(CaPreFinalSchema.codice_pdr) === lit("pdr2"))
        .where(col(CaPreFinalSchema.cod_prof_prel_std_forced).isNull)
        .where(col(CaPreFinalSchema.cat_uso_forced).isNull)
        .where(col(CaPreFinalSchema.zona_climatica_forced).isNull)
        .where(col(CaPreFinalSchema.classe_prelievo_forced).isNull)
        .count()
    )
    Assert.assertEquals(1,
      dummyCaFinalWithForcedLogic.where(col(CaPreFinalSchema.codice_pdr) === lit("pdr3"))
        .where(col(CaPreFinalSchema.cod_prof_prel_std_forced) === lit("T1X1"))
        .where(col(CaPreFinalSchema.cat_uso_forced) === lit("T1"))
        .where(col(CaPreFinalSchema.zona_climatica_forced).isNull)
        .where(col(CaPreFinalSchema.classe_prelievo_forced) === lit("1"))
        .count()
    )
    Assert.assertEquals(1,
      dummyCaFinalWithForcedLogic.where(col(CaPreFinalSchema.codice_pdr) === lit("pdr3.1"))
        .where(col(CaPreFinalSchema.cod_prof_prel_std_forced) === lit("T1X1"))
        .where(col(CaPreFinalSchema.cat_uso_forced) === lit("T1"))
        .where(col(CaPreFinalSchema.zona_climatica_forced) === lit("C"))
        .where(col(CaPreFinalSchema.classe_prelievo_forced) === lit("1"))
        .count()
    )
    Assert.assertEquals(1,
      dummyCaFinalWithForcedLogic.where(col(CaPreFinalSchema.codice_pdr) === lit("pdr3.2"))
        .where(col(CaPreFinalSchema.cod_prof_prel_std_forced) === lit("T2E3"))
        .where(col(CaPreFinalSchema.cat_uso_forced) === lit("T2"))
        .where(col(CaPreFinalSchema.zona_climatica_forced) === lit("E"))
        .where(col(CaPreFinalSchema.classe_prelievo_forced) === lit("3"))
        .count()
    )
    Assert.assertEquals(1,
      dummyCaFinalWithForcedLogic.where(col(CaPreFinalSchema.codice_pdr) === lit("pdr4"))
        .where(col(CaPreFinalSchema.cod_prof_prel_std_forced) === lit("C1B1"))
        .where(col(CaPreFinalSchema.cat_uso_forced) === lit("C1"))
        .where(col(CaPreFinalSchema.zona_climatica_forced).isNull)
        .where(col(CaPreFinalSchema.classe_prelievo_forced).isNull)
        .count()
    )
    Assert.assertEquals(1,
      dummyCaFinalWithForcedLogic.where(col(CaPreFinalSchema.codice_pdr) === lit("pdr4.1"))
        .where(col(CaPreFinalSchema.cod_prof_prel_std_forced) === lit("C2X1"))
        .where(col(CaPreFinalSchema.cat_uso_forced) === lit("C2"))
        .where(col(CaPreFinalSchema.zona_climatica_forced).isNull)
        .where(col(CaPreFinalSchema.classe_prelievo_forced).isNull)
        .count()
    )
    Assert.assertEquals(1,
      dummyCaFinalWithForcedLogic.where(col(CaPreFinalSchema.codice_pdr) === lit("pdr5"))
        .where(col(CaPreFinalSchema.cod_prof_prel_std_forced) === lit("T1X3"))
        .where(col(CaPreFinalSchema.cat_uso_forced).isNull)
        .where(col(CaPreFinalSchema.zona_climatica_forced).isNull)
        .where(col(CaPreFinalSchema.classe_prelievo_forced) === lit("3"))
        .count()
    )
    Assert.assertEquals(1,
      dummyCaFinalWithForcedLogic.where(col(CaPreFinalSchema.codice_pdr) === lit("pdr6"))
        .where(col(CaPreFinalSchema.cod_prof_prel_std_forced) === lit("C2X1"))
        .where(col(CaPreFinalSchema.cat_uso_forced) === lit("C2"))
        .where(col(CaPreFinalSchema.zona_climatica_forced) === lit("C"))
        .where(col(CaPreFinalSchema.classe_prelievo_forced) === lit("1"))
        .count()
    )
    Assert.assertEquals(1,
      dummyCaFinalWithForcedLogic.where(col(CaPreFinalSchema.codice_pdr) === lit("pdr6.1"))
        .where(col(CaPreFinalSchema.cod_prof_prel_std_forced) === lit("C1D1"))
        .where(col(CaPreFinalSchema.cat_uso_forced) === lit("C1"))
        .where(col(CaPreFinalSchema.zona_climatica_forced) === lit("D"))
        .where(col(CaPreFinalSchema.classe_prelievo_forced) === lit("1"))
        .count()
    )
    Assert.assertEquals(1,
      dummyCaFinalWithForcedLogic.where(col(CaPreFinalSchema.codice_pdr) === lit("pdr7.1"))
        .where(col(CaPreFinalSchema.cod_prof_prel_std_forced).isNull)
        .where(col(CaPreFinalSchema.cat_uso_forced).isNull)
        .where(col(CaPreFinalSchema.zona_climatica_forced).isNull)
        .where(col(CaPreFinalSchema.classe_prelievo_forced).isNull)
        .count()
    )
    Assert.assertEquals(1,
      dummyCaFinalWithForcedLogic.where(col(CaPreFinalSchema.codice_pdr) === lit("pdr7.2"))
        .where(col(CaPreFinalSchema.cod_prof_prel_std_forced).isNull)
        .where(col(CaPreFinalSchema.cat_uso_forced).isNull)
        .where(col(CaPreFinalSchema.zona_climatica_forced).isNull)
        .where(col(CaPreFinalSchema.classe_prelievo_forced).isNull)
        .count()
    )
  }

  def getDataFrames(): (DataFrame, DataFrame, DataFrame, DataFrame) = {
    val sqlContext = Environment.getSqlContext
    import sqlContext.implicits._

    val pdrMassivoDF: DataFrame = List(
      ("1", "2020", "id_distr_1", "id_az_udd_1", "id_udb_1", "cd_remi_1", "05260000050547", "trp_pdr_1", "cat_uso_1",
        "trattamento1", "11/11/2020", "tipo_tr_1", "YYYY7", "A", "12", "0.15", "", "", " calcmode1"),
      ("2", "2020", "id_distr_2", "id_az_udd_2", "id_udb_2", "cd_remi_2", "05260000050548", "trp_pdr_2", "cat_uso_2",
        "trattamento2", "11/11/2020", "tipo_tr_1", "YYYY8", "A", "12", "0.15", "", "", ""),
      ("3", "2020", "id_distr_3", "id_az_udd_2", "id_udb_3", "cd_remi_3", "05260000050549", "trp_pdr_3", "cat_uso_3",
        "trattamento3", "11/11/2020", "tipo_tr_1", "YYYY9", "A", "12", "0.15", "", "", ""),
      ("4", "2020", "id_distr_4", "id_az_udd_4", "id_udb_4", "cd_remi_4", "05260000050550", "trp_pdr_4", "cat_uso_4",
        "trattamento4", "11/11/2020", "tipo_tr_1", "YYYY0", "A", "12", "0.15", "", "", ""),
      ("5", "2020", "id_distr_5", "id_az_udd_5", "id_udb_5", "cd_remi_5", "05260000050551", "trp_pdr_5", "cat_uso_5",
        "trattamento5", "05/11/2020", "tipo_tr_1", "YYYY5", "A", "5", "0.55", "", "", " calcmode5")
    ).toDF(PdrMassivoSchema.id_sag_ann,
      PdrMassivoSchema.anno_competenza,
      PdrMassivoSchema.n_id_distr,
      PdrMassivoSchema.n_id_az_udd,
      PdrMassivoSchema.n_id_udb,
      PdrMassivoSchema.codice_remi,
      PdrMassivoSchema.codice_pdr,
      PdrMassivoSchema.cap_trasp_pdr,
      PdrMassivoSchema.cat_uso,
      PdrMassivoSchema.trattamento,
      PdrMassivoSchema.d_ricezione,
      PdrMassivoSchema.tipo_trasmissione,
      PdrMassivoSchema.t_cod_profilo,
      PdrMassivoSchema.t_regione_climatica,
      PdrMassivoSchema.id_regione_climatica,
      PdrMassivoSchema.n_prelievo_annuo,
      PdrMassivoSchema.prelievo_annuo_prev_forced,
      PdrMassivoSchema.cod_prof_prel_std_forced,
      PdrMassivoSchema.calcmode)
      .withColumn(PdrMassivoSchema.cat_uso_forced, lit(null).cast(StringType))
      .withColumn(PdrMassivoSchema.zona_climatica_forced, lit(null).cast(StringType))
      .withColumn(PdrMassivoSchema.classe_prelievo_forced, lit(null).cast(StringType))
      .withColumn(PdrMassivoSchema.trattamento_forced, lit(null).cast(StringType))
      .withColumn(PdrMassivoSchema.freeze_date, lit(Timestamp.valueOf("2021-07-01 00:00:00")))

    val caDF: DataFrame = List(
      ("0", "05260000050547", "XXXX47", "1", "016024", Some(620000000: Long), "startLocalFile1", "endLocalFile1", "startTMisuratoreIntegrato1", "endTTMisuratoreIntegrato1", "startTPreConv1", "endTPreConv1",
        true, true, false, "cat_uso_tds1", "classe_prelievo_tds1", "cod_istat_last_rcu1", "zona_climatica_lookup1"),
      ("0", "05260000050548", "XXXX48", "1", "016024", Some(200: Long), "startLocalFile2", "endLocalFile2", "startTMisuratoreIntegrato2", "endTTMisuratoreIntegrato2", "startTPreConv2", "endTPreConv2",
        true, true, false, "cat_uso_tds2", "classe_prelievo_tds2", "cod_istat_last_rcu2", "zona_climatica_lookup2"),
      ("0", "05260000050549", "XXXX49", "-2", "016025", Some(101: Long), "startLocalFile3", "endLocalFile3", "startTMisuratoreIntegrato3", "endTTMisuratoreIntegrato3", "startTPreConv3", "endTPreConv3",
        true, true, false, "cat_uso_tds3", "classe_prelievo_tds3", "cod_istat_last_rcu3", "zona_climatica_lookup3"),
      ("0", "05260000050549", "", "-2", "016025", None, "startLocalFile4", "endLocalFile4", "startTMisuratoreIntegrato4", "endTTMisuratoreIntegrato4", "startTPreConv4", "endTPreConv4",
        true, true, false, "cat_uso_tds4", "classe_prelievo_tds4", "cod_istat_last_rcu4", "zona_climatica_lookup4"),
      ("1", "05260000050550", "XXXX49", "2", "016025", Some(1: Long), "startLocalFile5", "endLocalFile5", "startTMisuratoreIntegrato5", "endTTMisuratoreIntegrato5", "startTPreConv5", "endTPreConv5",
        true, true, false, "cat_uso_tds5", "classe_prelievo_tds5", "cod_istat_last_rcu5", "zona_climatica_lookup5")
    ).toDF(CaSchema.idCaErrorCode,
      CaSchema.pdr,
      CaSchema.next_cod_profilo,
      CaSchema.id_regClim,
      CaSchema.t_comune_istat_pdr,
      CaSchema.ca_sum,

      CaSchema.start_local_file,
      CaSchema.end_local_file,
      CaSchema.start_t_misuratore_integrato,
      CaSchema.end_t_misuratore_integrato,
      CaSchema.start_t_pre_conv,
      CaSchema.end_t_pre_conv,

      CaSchema.pres_tds,
      CaSchema.tipologia_uso,
      CaSchema.comp_termica,
      CaSchema.cat_uso_tds,
      CaSchema.classe_prelievo_tds,
      CaSchema.cod_istat_last_rcu,
      CaSchema.zona_climatica_lookup
    ).withColumn(CaSchema.n_coeff_correzione, lit(null).cast(StringType))
      .withColumn(CaPreFinalSchema.startSegment, lit(null).cast(TimestampType))
      .withColumn(CaPreFinalSchema.endSegment, lit(null).cast(TimestampType))

    val tdsForPresTds = List(
      "05260000050547",
    "05260000050548",
    "05260000050549",
    "052600000505410"
    ).toDF(SettleGasGasTdsSchema.cod_pdr)

    val tdsForDedotti = List(
      ("05260000050547", "cat_uso_1", "classe_prelievo_1"),
      ("05260000050548", "cat_uso_2", "classe_prelievo_2"),
      ("05260000050549", "cat_uso_3", "classe_prelievo_3"),
      ("052600000505410", "cat_uso_4", "classe_prelievo_4")
    ).toDF(
      SettleGasGasTdsSchema.cod_pdr,
      SettleGasGasTdsSchema.cat_uso,
      SettleGasGasTdsSchema.classe_prelievo
    )

    (pdrMassivoDF, caDF, tdsForPresTds, tdsForDedotti)
  }

}
