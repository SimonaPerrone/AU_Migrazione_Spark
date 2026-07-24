package it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.dedotti

import it.eng.au.aggregatoreConsumiCdp.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCdp.schema.{OutputCsvSchema, OutputHiveSchema}
import it.eng.au.aggregatoreConsumiCdp.utility.Environment
import org.apache.spark.sql.functions.col
import org.junit.Assert

class DedottiDistrTest extends EnvironmentSparkTest {
  val sqlContext = Environment.sqlContext
  import sqlContext.implicits._

  def testRun(): Unit = {
    val df = Seq(
      ("codiceremi1", "codicepdr1", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "SI", "2022", "456", "", "123", "piva_distr1", "piva_udd1", "piva_udb1", "PRE", "1648648490000", "DF")
      , ("codiceremi1", "codicepdr1", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "SI", "2022", "456", "", "123", "piva_distr2", "piva_udd2", "piva_udb2", "PRE", "1648648490000", "DF")
      , ("codiceremi1", "codicepdr1", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "SI", "2022", "456", "", "123", "piva_distr3", "piva_udd3", "piva_udb3", "sessione3", "1648648490000", "DF")
      , ("codiceremi2", "codicepdr2", "catuso2", "classeprelievo2", "zonaclimatica2", "idregclim2", "codprofprelstd2", "prelievoannuoprev2", "trattamento2", "SI", "2022", "456", "", "123", "piva_distr4", "piva_udd4", "piva_udb4", null, "1648648490000", "DF")
      , ("codiceremi2", "codicepdr2", "catuso2", "classeprelievo2", "zonaclimatica2", "idregclim2", "codprofprelstd2", "prelievoannuoprev2", "trattamento2", "SI", "2022", "456", "", "123", "piva_distr5", "piva_udd5", "piva_udb5", "fin", "1648648490000", "DF")
      , ("codiceremi2", "codicepdr2", "catuso2", "classeprelievo2", "zonaclimatica2", "idregclim2", "codprofprelstd2", "prelievoannuoprev2", "trattamento2", "SI", "2022", "456", "", "123", "piva_distr6", "piva_udd6", "piva_udb6", "FIN", "1648648490000", "DF")
      , ("codiceremi3", "codicepdr3", "catuso3", "classeprelievo3", "zonaclimatica3", "idregclim3", "codprofprelstd3", "prelievoannuoprev3", "trattamento3", "SI", "2022", "456", "", "123", "piva_distr7", "piva_udd7", "piva_udb7", "pre", "1648648490000", "DF")
      , ("codiceremi6", "codicepdr6", "catuso6", "classeprelievo6", "zonaclimatica6", "idregclim6", "codprofprelstd6", "prelievoannuoprev6", "trattamento6", "SI", "2022", "456", "", "123", "piva_distr8", "piva_udd8", "piva_udb8", "sessione8", "1648648490000", "M1")
    ).toDF(
      OutputHiveSchema.cod_remi,
      OutputHiveSchema.cod_pdr,
      OutputHiveSchema.cat_uso,
      OutputHiveSchema.classe_prelievo,
      OutputHiveSchema.zona_climatica,
      OutputHiveSchema.id_reg_clim,
      OutputHiveSchema.cod_prof_prel_std,
      OutputHiveSchema.prelievo_annuo_prev,
      OutputHiveSchema.trattamento,
      OutputHiveSchema.pres_tds,
      OutputHiveSchema.anno_competenza,
      OutputHiveSchema.massivo_freezer_executiond_id,
      OutputHiveSchema.data_decorrenza,
      OutputHiveSchema.calc_executiond_id,
      OutputHiveSchema.piva_distr,
      OutputHiveSchema.piva_udd,
      OutputHiveSchema.piva_udb,
      OutputHiveSchema.tipo_trasmissione,
      OutputHiveSchema.execution_id,
      OutputHiveSchema.causale
    )
    DedottiDistr.run(df)
  }

  def testGetAggreato(): Unit = {
    val df = Seq(
      ("codiceremi1", "codicepdr1", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "SI", "2022", "456", "2019-07-01 12:01:19", "123", "pivadistr1", "pivaudd1", "pivaudb1", "DEDOTTI", "1648648490000", "DF")
      , ("codiceremi1", "codicepdr1", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "SI", "2022", "456", "2019-07-01 12:01:19", "123", "pivadistr1", "pivaudd1", "pivaudb1", "DEDOTTI", "1648648490000", "DF")
      , ("codiceremi1", "codicepdr1", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "SI", "2022", "456", "2019-07-01 12:01:19", "123", "pivadistr1", null, "pivaudb1", "DEDOTTI", "1648648490000", "DF")
      , ("codiceremi1", "codicepdr1", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "SI", "2022", "456", "2019-07-01 12:01:19", "123", "pivadistr1", "pivaudd1", null, "DEDOTTI", "1648648490000", "DF")
      , ("codiceremi1", "codicepdr1", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "SI", "2022", "456", "2019-07-01 12:01:19", "123", "pivadistr1", "pivaudd1", "pivaudb1", "DEDOTTI", "1648648490000", "DF")
      , ("codiceremi3", "codicepdr3", "catuso3", "classeprelievo3", "zonaclimatica3", "idregclim3", "codprofprelstd3", "prelievoannuoprev3", "trattamento3", "SI", "2022", "456", "2019-07-01 12:01:19", "123", "pivadistr1", "pivaudd1", "pivaudb1", "DEDOTTI", "1648648490000", "DF")
      , ("codiceremi6", "codicepdr6", "catuso6", "classeprelievo6", "zonaclimatica6", "idregclim6", "codprofprelstd6", "prelievoannuoprev6", "trattamento6", "SI", "2022", "456", null, "123", "pivadistr1", "pivaudd1", "pivaudb1", "DEDOTTI", "1648648490000", "M1")
    ).toDF(
      OutputHiveSchema.cod_remi,
      OutputHiveSchema.cod_pdr,
      OutputHiveSchema.cat_uso,
      OutputHiveSchema.classe_prelievo,
      OutputHiveSchema.zona_climatica,
      OutputHiveSchema.id_reg_clim,
      OutputHiveSchema.cod_prof_prel_std,
      OutputHiveSchema.prelievo_annuo_prev,
      OutputHiveSchema.trattamento,
      OutputHiveSchema.pres_tds,
      OutputHiveSchema.anno_competenza,
      OutputHiveSchema.massivo_freezer_executiond_id,
      OutputHiveSchema.data_decorrenza,
      OutputHiveSchema.calc_executiond_id,
      OutputHiveSchema.piva_distr,
      OutputHiveSchema.piva_udd,
      OutputHiveSchema.piva_udb,
      OutputHiveSchema.tipo_trasmissione,
      OutputHiveSchema.execution_id,
      OutputHiveSchema.causale
    )
    val res = DedottiDistr.getAggregato(df)
    res.show()

    Assert.assertEquals(5, res.count())
    Assert.assertEquals(4, res.where(col(OutputHiveSchema.cod_pdr) === "codicepdr1").count())
    Assert.assertEquals(5, res.where(col(OutputHiveSchema.causale) === "DF").count())
    Assert.assertEquals(0, res.where(col(OutputHiveSchema.data_decorrenza).isNull).count())
    Assert.assertEquals(1, res.where(col(OutputCsvSchema.piva_udb).isNull).count())
    Assert.assertEquals(0, res.where(col(OutputCsvSchema.piva_udd).isNull).count())
    Assert.assertEquals(4, res.filter(col(OutputHiveSchema.causale) === "DF" and col(OutputHiveSchema.cod_pdr) === "codicepdr1").count())
    Assert.assertEquals(1, res.filter(col(OutputHiveSchema.causale) === "DF" and col(OutputHiveSchema.cod_pdr) === "codicepdr3").count())
  }
}
