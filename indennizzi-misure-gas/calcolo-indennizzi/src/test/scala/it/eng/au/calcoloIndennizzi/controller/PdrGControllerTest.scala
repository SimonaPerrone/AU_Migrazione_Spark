package it.eng.au.calcoloIndennizzi.controller

import it.eng.au.calcoloIndennizzi.EnvironmentSparkTest
import it.eng.au.calcoloIndennizzi.controller.PdrGController.{dataAttivazione, hasIndennizzo}
import it.eng.au.calcoloIndennizzi.schema.cig.{PdrCountSchema, PdrGSchema, PdrGSettimoSchema, PdrTotaleSchema}
import it.eng.au.calcoloIndennizzi.schema.measure.TglSchema
import it.eng.au.calcoloIndennizzi.schema.rcu.RcuAziendaSchema
import it.eng.au.calcoloIndennizzi.schema.rcugas.{RcugasConnessioniDistr2Schema, RcugasMassivoPSchema, RcugasSospensioniPSchema, RcugasVarMisuratoreSchema}
import it.eng.au.indennizziMisureGasCommon.schema.{AggregatoTotaleSchema, DettaglioPdrSchema}
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.Assert

class PdrGControllerTest extends EnvironmentSparkTest {
  def testGetDettagliPdr(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val pdrGSettimo = Environment.sparkContext.parallelize(Seq(
      ("pdr1", "udd1", "azienda1", "distr1", "azienda1", "G10", "2022-10-01", "/mnt/isilonshare_gas/TMG_distr1/DISTRIBUTORE/TMG_distr1_udd1/2022/1001/distr1_udd1_201301_TGL_20221001205941_3515_M.zip", Some(2), Some(6), true, true, true, false, "202210"),
      ("pdr2", "udd2", "azienda2", "distr2", "azienda2", "G10", "2022-10-01", "/mnt/isilonshare_gas/TMG_distr2/DISTRIBUTORE/TMG_distr2_udd2/2022/1001/distr2_udd2_201301_TGL_20221001205941_3515_M.zip", Some(3), Some(5), true, true, true, false, "202210"),
      ("pdr3", "udd3", "azienda3", "distr3", "azienda3", "G10", "2022-10-01", "/mnt/isilonshare_gas/TMG_distr3/DISTRIBUTORE/TMG_distr3_udd3/2022/1001/distr3_udd3_201301_TGL_20221001205941_3515_M.zip", Some(4), Some(4), true, true, false, true, "202210"),
      ("pdr4", "udd4", "azienda4", "distr4", "azienda4", "G10", "2022-10-01", "/mnt/isilonshare_gas/TMG_distr4/DISTRIBUTORE/TMG_distr4_udd4/2022/1001/distr4_udd4_201301_TGL_20221001205941_3515_M.zip", Some(5), Some(3), true, true, false, true, "202210"),
      ("pdr5", "udd5", "azienda5", "distr5", "azienda5", "G10", "2022-10-01", "/mnt/isilonshare_gas/TMG_distr4/DISTRIBUTORE/TMG_distr4_udd4/2022/1001/distr4_udd4_201301_TGL_20221001205941_3515_M.zip", Some(5), Some(3), true, true, false, true, "202210"),
      ("pdr6", "udd6", "azienda6", "distr6", "azienda6", "G10", "2022-10-01", "/mnt/isilonshare_gas/TMG_distr4/DISTRIBUTORE/TMG_distr4_udd4/2022/1001/distr4_udd4_201301_TGL_20221001205941_3515_M.zip", Some(5), Some(3), true, false, false, false, "202210"),
      ("pdr7", "udd7", "azienda7", "distr7", "azienda7", "G10", "2022-10-01", "/mnt/isilonshare_gas/TMG_distr4/DISTRIBUTORE/TMG_distr4_udd4/2022/1001/distr4_udd4_201301_TGL_20221001205941_3515_M.zip", Some(5), Some(3), true, false, false, false, "202210"),
      ("pdr8", "udd8", "azienda8", "distr8", "azienda8", "G10", "2022-10-01", null, None, None, true, false, false, false, "202210"),
      ("pdr9", "udd9", "azienda9", "distr9", "azienda9", "G10", "2022-10-01", null, None, None, true, false, false, false, "202210"),
      ("pdr10", "udd10", "azienda10", "distr10", "azienda10", "G10", "2022-10-01", null, None, None, true, false, false, false, "202210")
    )).toDF(PdrGSettimoSchema.getValues: _*)
      .withColumn(hasIndennizzo, lit(true))

    val result = PdrGController.getDettaglioPdr(pdrGSettimo)
    result.show

    Assert.assertEquals(10, result.count())
    Assert.assertEquals(9, result.columns.length)
    Assert.assertEquals(3, result.where(col(DettaglioPdrSchema.nome_file).isNull).count)
    Assert.assertEquals(10, result.select(col(DettaglioPdrSchema.pdr)).distinct.count)
  }

  def testGetPdrTotale(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val pdrGSettimo = Environment.sparkContext.parallelize(Seq(
      ("pdr1", "udd1", "azienda1", "distr1", "azienda1", "G10", "2022-10-01", "/mnt/isilonshare_gas/TMG_distr1/DISTRIBUTORE/TMG_distr1_udd1/2022/1001/distr1_udd1_201301_TGL_20221001205941_3515_M.zip", Some(2), Some(6), true, true, true, false, "202210"),
      ("pdr2", "udd2", "azienda2", "distr2", "azienda2", "G10", "2022-10-01", "/mnt/isilonshare_gas/TMG_distr2/DISTRIBUTORE/TMG_distr2_udd2/2022/1001/distr2_udd2_201301_TGL_20221001205941_3515_M.zip", Some(3), Some(5), true, true, true, false, "202210"),
      ("pdr3", "udd3", "azienda3", "distr3", "azienda3", "G10", "2022-10-01", "/mnt/isilonshare_gas/TMG_distr3/DISTRIBUTORE/TMG_distr3_udd3/2022/1001/distr3_udd3_201301_TGL_20221001205941_3515_M.zip", Some(4), Some(4), true, true, false, true, "202210"),
      ("pdr4", "udd4", "azienda4", "distr4", "azienda4", "G10", "2022-10-01", "/mnt/isilonshare_gas/TMG_distr4/DISTRIBUTORE/TMG_distr4_udd4/2022/1001/distr4_udd4_201301_TGL_20221001205941_3515_M.zip", Some(5), Some(3), true, true, false, true, "202210"),
      ("pdr5", "udd5", "azienda5", "distr5", "azienda5", "G10", "2022-10-01", "/mnt/isilonshare_gas/TMG_distr4/DISTRIBUTORE/TMG_distr4_udd4/2022/1001/distr4_udd4_201301_TGL_20221001205941_3515_M.zip", Some(5), Some(3), true, true, false, true, "202210"),
      ("pdr6", "udd6", "azienda6", "distr6", "azienda6", "G10", "2022-10-01", "/mnt/isilonshare_gas/TMG_distr4/DISTRIBUTORE/TMG_distr4_udd4/2022/1001/distr4_udd4_201301_TGL_20221001205941_3515_M.zip", Some(5), Some(3), true, false, false, false, "202210"),
      ("pdr7", "udd7", "azienda7", "distr7", "azienda7", "G10", "2022-10-01", "/mnt/isilonshare_gas/TMG_distr4/DISTRIBUTORE/TMG_distr4_udd4/2022/1001/distr4_udd4_201301_TGL_20221001205941_3515_M.zip", Some(5), Some(3), true, false, false, false, "202210"),
      ("pdr8", "udd8", "azienda8", "distr8", "azienda8", "G10", "2022-10-01", null, None, None, true, false, false, false, "202210"),
      ("pdr9", "udd9", "azienda9", "distr9", "azienda9", "G10", "2022-10-01", null, None, None, true, false, false, false, "202210"),
      ("pdr10", "udd10", "azienda10", "distr10", "azienda10", "G10", "2022-10-01", null, None, None, true, false, false, false, "202210")
    )).toDF(PdrGSettimoSchema.getValues: _*)

    val aggregatoTotale = Environment.sparkContext.parallelize(Seq(
      ("udd1", "distr1", "id_indennizzo1", 0, 0, 1, "12345"),
      ("udd2", "distr2", "id_indennizzo2", 0, 1, 0, "12345"),
      ("udd3", "distr3", "id_indennizzo3", 0, 0, 1, "12345"),
      ("udd4", "distr4", "id_indennizzo4", 0, 1, 0, "12345"),
      ("udd5", "distr5", "id_indennizzo5", 0, 1, 1, "12345"),
      ("udd6", "distr6", "id_indennizzo6", 1, 0, 0, "12345"),
      ("udd7", "distr7", "id_indennizzo7", 1, 1, 1, "12345"),
      ("udd8", "distr8", "id_indennizzo8", 1, 0, 0, "12345"),
      ("udd9", "distr9", "id_indennizzo9", 0, 1, 0, "12345"),
      ("udd10", "distr10", "id_indennizzo10", 0, 0, 1, "12345")
    )).toDF(
      AggregatoTotaleSchema.piva_udd,
      AggregatoTotaleSchema.piva_distr,
      AggregatoTotaleSchema.id_indennizzo,
      AggregatoTotaleSchema.indennizzo_om1,
      AggregatoTotaleSchema.indennizzo_om2,
      AggregatoTotaleSchema.indennizzo_om3,
      AggregatoTotaleSchema.executionid
    )

    val result = PdrGController.getPdrTotale(pdrGSettimo, aggregatoTotale)
    result.show

    Assert.assertEquals(10, result.count())
    Assert.assertEquals(29, result.columns.length)
    Assert.assertEquals(3, result.where(col(PdrTotaleSchema.local_file).isNull).count)
    Assert.assertEquals(8, result.where(col(PdrTotaleSchema.nome_file_tgl_om1) === "no").count)
    Assert.assertEquals(7, result.where(col(PdrTotaleSchema.nome_file_tgl_om2) === "no").count)
    Assert.assertEquals(8, result.where(col(PdrTotaleSchema.nome_file_tgl_om3) === "no").count)
    Assert.assertEquals(3, result.where(col(PdrTotaleSchema.count_tgl_effettive).isNull).count)
  }

  def testGetPdrCount(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val pdrGSettimo = Environment.sparkContext.parallelize(Seq(
      ("pdr1", "udd1", "azienda1", "distr1", "azienda5", true, true, false, true),
      ("pdr2", "udd1", "azienda1", "distr1", "azienda5", true, true, true, false),
      ("pdr3", "udd1", "azienda1", "distr1", "azienda5", true, false, false, false),
      ("pdr4", "udd2", "azienda2", "distr2", "azienda6", true, true, true, false),
      ("pdr5", "udd2", "azienda2", "distr2", "azienda6", true, true, true, false),
      ("pdr6", "udd2", "azienda2", "distr2", "azienda6", true, true, false, false),
      ("pdr7", "udd3", "azienda3", "distr3", "azienda7", true, false, false, false),
      ("pdr8", "udd3", "azienda3", "distr3", "azienda7", true, true, true, false),
      ("pdr9", "udd3", "azienda3", "distr3", "azienda7", true, false, false, false),
      ("pdr10", "udd4", "azienda4", "distr4", "azienda8", true, true, false, true),
      ("pdr11", "udd4", "azienda4", "distr4", "azienda8", true, true, true, false),
      ("pdr12", "udd4", "azienda4", "distr4", "azienda8", true, true, false, true)
    )).toDF(PdrGSettimoSchema.codice_pdr,
      PdrGSettimoSchema.piva_udd,
      PdrGSettimoSchema.rag_soc_udd,
      PdrGSettimoSchema.piva_distr,
      PdrGSettimoSchema.rag_soc_distr,
      PdrGSettimoSchema.is_pdrG,
      PdrGSettimoSchema.is_pdrG_om1,
      PdrGSettimoSchema.is_pdrG_om2,
      PdrGSettimoSchema.is_pdrG_om3)

    val result = PdrGController.getPdRCount(pdrGSettimo)
    result.show

    Assert.assertEquals(4, result.count())
    Assert.assertEquals(8, result.columns.length)
    Assert.assertEquals(4, result.where(col(PdrCountSchema.pdr_g) === 3).count())
    Assert.assertEquals(1, result.where(col(PdrCountSchema.pdr_g_om1) === 1).count())
    Assert.assertEquals(3, result.where(col(PdrCountSchema.pdr_g_om2) === 1).count())
    Assert.assertEquals(1, result.where(col(PdrCountSchema.pdr_g_om3) === 1).count())
  }

  def testGetRagioneSociale(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val pdrG = Environment.sparkContext.parallelize(Seq(
      ("pdr1", "udd1", "distr1", "G10", "2022-10-01", "202210"),
      ("pdr2", "udd2", "distr2", "G10", "2022-10-01", "202210"),
      ("pdr3", "udd3", "distr3", "G10", "2022-10-01", "202210"),
      ("pdr4", "udd4", "distr4", "G10", "2022-10-01", "202210")
    )).toDF(
      PdrGSchema.codice_pdr,
      PdrGSchema.piva_udd,
      PdrGSchema.piva_distr,
      PdrGSchema.classe_gdm,
      PdrGSchema.data_attivazione_pdr,
      PdrGSchema.annomese
    )

    val rcuAziendaForId = Environment.sparkContext.parallelize(Seq(
      ("distr1", "azienda4"),
      ("distr2", "azienda5"),
      ("distr3", "azienda6")
    )).toDF(RcuAziendaSchema.getValues: _*)

    val rcuAziendaForUdd = Environment.sparkContext.parallelize(Seq(
      ("udd1", "azienda1"),
      ("udd2", "azienda2"),
      ("udd3", "azienda3")
    )).toDF(RcuAziendaSchema.getValues: _*)


    val result = PdrGController.getRagioneSociale(pdrG, rcuAziendaForId, rcuAziendaForUdd)
    result.show

    Assert.assertEquals(4, result.count())
    Assert.assertEquals(8, result.columns.length)
    Assert.assertEquals(3, result.where(col(PdrGSchema.rag_soc_udd).isNotNull).count())
    Assert.assertEquals(1, result.where(col(PdrGSchema.rag_soc_distr).isNull).count())
    Assert.assertEquals(1, result.where(col(PdrGSchema.rag_soc_udd) === lit("azienda1")).count())
    Assert.assertEquals(1, result.where(col(PdrGSchema.rag_soc_distr) === lit("azienda4")).count())
    Assert.assertEquals(0, result.where(col(PdrGSchema.rag_soc_distr) === lit("azienda2")).count())
  }

  def testGetPdrGSettimoDataFrame(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val pdrG = Environment.sparkContext.parallelize(Seq(
      ("pdr1", "udd1", "azienda1", "distr1", "azienda5", "G10", "2022-10-01", "202210"),
      ("pdr2", "udd1", "azienda1", "distr1", "azienda5", "G10", "2022-10-01", "202210"),
      ("pdr3", "udd2", "azienda2", "distr2", "azienda6", "G10", "2022-10-01", "202210"),
      ("pdr4", "udd2", "azienda2", "distr2", "azienda6", "G10", "2022-10-01", "202210"),
      ("pdr5", "udd3", "azienda3", "distr3", "azienda7", "G10", "2022-10-01", "202210"),
      ("pdr6", "udd3", "azienda3", "distr3", "azienda7", "G10", "2022-10-01", "202210"),
      ("pdr7", "udd4", "azienda4", "distr4", "azienda8", "G10", "2022-10-01", "202210"),
      ("pdr8", "udd4", "azienda4", "distr4", "azienda8", "G10", "2022-10-01", "202210")
    )).toDF(PdrGSchema.getValues: _*)

    val tglWithReadTypeInfo = Environment.sparkContext.parallelize(Seq(
      ("pdr1", List("/mnt/localfile1"), 3, 0, true, true, false),
      ("pdr2", List("/mnt/localfile1"), 3, 0, true, true, false),
      ("pdr3", List("/mnt/localfile1"), 3, 0, true, true, false),
      ("pdr4", List("/mnt/localfile2"), 2, 1, true, false, true),
      ("pdr5", List("/mnt/localfile2"), 2, 1, true, false, true),
      ("pdr6", List("/mnt/localfile2"), 2, 1, true, false, false)
    )).toDF(
      TglSchema.cod_pdr,
      TglSchema.local_file,
      PdrGSettimoSchema.count_tgl_effettive,
      PdrGSettimoSchema.count_tgl_stimate,
      TglController.isTglOM1,
      TglController.isTglOM2,
      TglController.isTglOM3
    )

    val result = PdrGController.getPdrGSettimoDataFrame(pdrG, tglWithReadTypeInfo)
    result.show

    Assert.assertEquals(8, result.count())
    Assert.assertEquals(15, result.columns.length)
    Assert.assertEquals(8, result.where(col(PdrGSettimoSchema.is_pdrG) === true).count())
    Assert.assertEquals(6, result.where(col(PdrGSettimoSchema.is_pdrG_om1) === true).count())
    Assert.assertEquals(3, result.where(col(PdrGSettimoSchema.is_pdrG_om2) === true).count())
    Assert.assertEquals(2, result.where(col(PdrGSettimoSchema.is_pdrG_om3) === true).count())
    Assert.assertEquals(3, result.where(col(PdrGSettimoSchema.is_pdrG_om2) === false && col(PdrGSettimoSchema.is_pdrG_om3) === false).count())
  }

  def testGetPdrGDataFrame(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val rcugasMassivo: DataFrame = Environment.sparkContext.parallelize(Seq(
      ("nIdPdr1", "pdr1", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr2", "pdr2", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr3", "pdr3", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr4", "pdr4", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr5", "pdr5", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr6", "pdr6", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr7", "pdr7", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr8", "pdr8", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr9", "pdr9", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr10", "pdr10", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr11", "pdr11", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr12", "pdr12", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr13", "pdr13", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr14", "pdr14", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr15", "pdr15", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr16", "pdr16", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr17", "pdr17", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr18", "pdr18", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr19", "pdr19", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr20", "pdr20", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr21", "pdr21", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr22", "pdr22", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr23", "pdr23", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr24", "pdr24", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr25", "pdr25", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr26", "pdr26", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr27", "pdr27", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr28", "pdr28", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr29", "pdr29", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr30", "pdr30", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr31", "pdr31", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr32", "pdr32", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr33", "pdr33", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr34", "pdr34", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr35", "pdr35", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr36", "pdr36", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr37", "pdr37", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr38", "pdr38", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"), ("nIdPdr39", "pdr39", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01")
    )).toDF(RcugasMassivoPSchema.n_id_pdr,
      RcugasMassivoPSchema.t_codice_pdr,
      RcugasMassivoPSchema.n_id_fornitura,
      RcugasMassivoPSchema.piva_udd,
      dataAttivazione,
      RcugasMassivoPSchema.d_data_inizio_for,
      RcugasMassivoPSchema.data_fine_for
    )

    val rcugasVarMisuratore: DataFrame = Environment.sparkContext.parallelize(Seq(
      ("nIdPdr1", "G10"), ("nIdPdr2", "G10"), ("nIdPdr3", "G10"),
      ("nIdPdr4", "G10"), ("nIdPdr5", "G10"), ("nIdPdr6", "G10"),
      ("nIdPdr7", "G10"), ("nIdPdr8", "G10"), ("nIdPdr9", "G10"),
      ("nIdPdr10", "G10"), ("nIdPdr11", "G10"), ("nIdPdr12", "G10"),
      ("nIdPdr13", "G10"), ("nIdPdr14", "G10"), ("nIdPdr15", "G10"),
      ("nIdPdr16", "G10"), ("nIdPdr17", "G10"), ("nIdPdr18", "G10"),
      ("nIdPdr19", "G10"), ("nIdPdr20", "G10"), ("nIdPdr21", "G10"),
      ("nIdPdr22", "G10"), ("nIdPdr23", "G10"), ("nIdPdr24", "G10"),
      ("nIdPdr25", "G10"), ("nIdPdr26", "G10"), ("nIdPdr27", "G10"),
      ("nIdPdr28", "G10"), ("nIdPdr29", "G10"), ("nIdPdr30", "G10"),
      ("nIdPdr31", "G10"), ("nIdPdr32", "G10"), ("nIdPdr33", "G10"),
      ("nIdPdr34", "G10"), ("nIdPdr35", "G10"), ("nIdPdr36", "G10"),
      ("nIdPdr37", "G10"), ("nIdPdr38", "G10"), ("nIdPdr39", "G10")
    )).toDF(RcugasVarMisuratoreSchema.n_id_pdr,RcugasVarMisuratoreSchema.t_classe_misuratore)

    val schema = StructType(Array(
      StructField("n_id_pdr", StringType, nullable = true)
    ))

    val rcugasVarTrattamento: DataFrame = Environment.spark.createDataFrame(
      Environment.sparkContext.parallelize(Seq(
        Row("nIdPdr1"), Row("nIdPdr4"), Row("nIdPdr7"), Row("nIdPdr10"), Row("nIdPdr13"), Row("nIdPdr16"), Row("nIdPdr19"), Row("nIdPdr22"), Row("nIdPdr25"), Row("nIdPdr28"), Row("nIdPdr31"), Row("nIdPdr34"), Row("nIdPdr37"),
        Row("nIdPdr2"), Row("nIdPdr5"), Row("nIdPdr8"), Row("nIdPdr11"), Row("nIdPdr14"), Row("nIdPdr17"),
        Row("nIdPdr3"), Row("nIdPdr6"), Row("nIdPdr9"), Row("nIdPdr12"), Row("nIdPdr15"), Row("nIdPdr18"), Row("nIdPdr21"), Row("nIdPdr24"), Row("nIdPdr27"), Row("nIdPdr30"), Row("nIdPdr33"), Row("nIdPdr36"), Row("nIdPdr39")
      )), schema)

    val rcugasSospensioni: DataFrame = Environment.spark.createDataFrame(Seq(
      ("nIdPdr1",  "fornitura", "2022-10-01", "2022-10-05"),
      ("nIdPdr4",  "fornitura", "2022-10-01", "2022-10-05"),
      ("nIdPdr7",  "fornitura", "2022-10-01", "2022-10-05"),
      ("nIdPdr10", "fornitura", "2022-10-01", "2022-10-05")
    )).toDF(RcugasSospensioniPSchema.n_id_pdr,
      RcugasSospensioniPSchema.n_id_fornitura,
      RcugasSospensioniPSchema.d_data_inizio_sosp,
      RcugasSospensioniPSchema.d_data_revoca_sosp)

    val rcugasConnessioniDistr2: DataFrame = Environment.sparkContext.parallelize(Seq(
      ("pdr1", "distr1"), ("pdr2", "distr2"), ("pdr3", "distr3"),
      ("pdr4", "distr1"), ("pdr5", "distr2"), ("pdr6", "distr3"),
      ("pdr7", "distr1"), ("pdr8", "distr2"), ("pdr9", "distr3"),
      ("pdr10", "distr1"), ("pdr11", "distr2"), ("pdr12", "distr3"),
      ("pdr13", "distr1"), ("pdr14", "distr2"), ("pdr15", "distr3"),
      ("pdr16", "distr1"), ("pdr17", "distr2"), ("pdr18", "distr3"),
      ("pdr19", "distr1"), ("pdr20", "distr2"), ("pdr21", "distr3"),
      ("pdr22", "distr1"), ("pdr23", "distr2"), ("pdr24", "distr3"),
      ("pdr25", "distr1"), ("pdr26", "distr2"), ("pdr27", "distr3"),
      ("pdr28", "distr1"), ("pdr29", "distr2"), ("pdr30", "distr3"),
      ("pdr31", "distr1"), ("pdr32", "distr2"), //("pdr33", "distr3"),
      ("pdr34", "distr1"), ("pdr35", "distr2"), ("pdr36", "distr3"),
      ("pdr37", "distr1"), ("pdr38", "distr2"), ("pdr39", "distr3")
    )).toDF(RcugasConnessioniDistr2Schema.t_codice_pdr, RcugasConnessioniDistr2Schema.t_piva_distr)

    val pdrG = PdrGController.getPdrGDataFrame(rcugasMassivo, rcugasVarMisuratore, rcugasVarTrattamento, rcugasSospensioni, rcugasConnessioniDistr2)
    pdrG.show

    Assert.assertEquals(12, pdrG.count)
    Assert.assertEquals(6, pdrG.columns.length)
    Assert.assertEquals(0, pdrG.where(col(PdrGSchema.piva_udd) === "udd2").count)
    Assert.assertEquals(12, pdrG.where(col(PdrGSchema.piva_distr) === "distr3").count)
    Assert.assertEquals(12, pdrG.where(col(PdrGSchema.piva_udd) === "udd3").count)
  }

  def testRemoveSospesi(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val rcugasMassivo: DataFrame = Environment.sparkContext.parallelize(Seq(
      ("nIdPdr1", "pdr1", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr2", "pdr2", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr3", "pdr3", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr4", "pdr4", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr5", "pdr5", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr6", "pdr6", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr7", "pdr7", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr8", "pdr8", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr9", "pdr9", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr10", "pdr10", "fornitura", "udd1", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr11", "pdr11", "fornitura", "udd2", "2022-10-01", "2022-10-01", "2022-11-01"),
      ("nIdPdr12", "pdr12", "fornitura", "udd3", "2022-10-01", "2022-10-01", "2022-11-01")
      )).toDF(RcugasMassivoPSchema.n_id_pdr,
      RcugasMassivoPSchema.t_codice_pdr,
      RcugasMassivoPSchema.n_id_fornitura,
      RcugasMassivoPSchema.piva_udd,
      dataAttivazione,
      RcugasMassivoPSchema.d_data_inizio_for,
      RcugasMassivoPSchema.data_fine_for
    )

    val rcugasSospensioni: DataFrame = Environment.spark.createDataFrame(Seq(
      ("nIdPdr1",  "fornitura", "2022-10-01", "2022-10-05"),
      ("nIdPdr4",  "fornitura", "2022-10-01", "2022-10-05"),
      ("nIdPdr7",  "fornitura", "2022-10-01", "2022-10-05"),
      ("nIdPdr10", "fornitura", "2022-10-01", "2022-10-05")
    )).toDF(RcugasSospensioniPSchema.n_id_pdr,
      RcugasSospensioniPSchema.n_id_fornitura,
      RcugasSospensioniPSchema.d_data_inizio_sosp,
      RcugasSospensioniPSchema.d_data_revoca_sosp)

    val result = PdrGController.removeSospesi(rcugasMassivo, rcugasSospensioni)
    result.show

    Assert.assertEquals(8, result.count)
    Assert.assertEquals(4, result.columns.length)
  }
}
