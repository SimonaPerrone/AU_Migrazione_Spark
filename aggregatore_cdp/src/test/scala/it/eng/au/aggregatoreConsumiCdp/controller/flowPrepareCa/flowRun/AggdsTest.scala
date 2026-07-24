package it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa.flowRun

import it.eng.au.aggregatoreConsumiCdp.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCdp.model.CaFinalLikeCodProfTds
import it.eng.au.aggregatoreConsumiCdp.schema._
import it.eng.au.aggregatoreConsumiCdp.utility.Environment

import java.sql.Timestamp

class AggdsTest extends EnvironmentSparkTest {
  val sqlContext = Environment.sqlContext
  import sqlContext.implicits._

  def testRun(): Unit = {
    val executionId = Timestamp.valueOf(Environment.dateRun).getTime

    val df = Seq(
      CaFinalLikeCodProfTds(n_id_distr = "distr1", n_id_udd = "udd1", n_id_udb = "udb1", cod_remi = "codiceremi1", codice_pdr = "codicepdr1", cat_uso = "catuso1", classe_prelievo = "classeprelievo1", zona_climatica = "zonaclimatica1", id_regione_climatica = "idregclim1", cod_prof_prel_std = "codprofprelstd1", prelievo_annuo_prev = "prelievoannuoprev1", trattamento = "trattamento1", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr2", n_id_udd = "udd2", n_id_udb = "udb2", cod_remi = "codiceremi1", codice_pdr = "codicepdr1", cat_uso = "catuso1", classe_prelievo = "classeprelievo1", zona_climatica = "zonaclimatica1", id_regione_climatica = "idregclim1", cod_prof_prel_std = "codprofprelstd1", prelievo_annuo_prev = "prelievoannuoprev1", trattamento = "trattamento1", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr3", n_id_udd = "udd3", n_id_udb = "udb3", cod_remi = "codiceremi1", codice_pdr = "codicepdr1", cat_uso = "catuso1", classe_prelievo = "classeprelievo1", zona_climatica = "zonaclimatica1", id_regione_climatica = "idregclim1", cod_prof_prel_std = "codprofprelstd1", prelievo_annuo_prev = "prelievoannuoprev1", trattamento = "trattamento1", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr4", n_id_udd = "udd4", n_id_udb = "udb4", cod_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_regione_climatica = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr5", n_id_udd = "udd5", n_id_udb = "udb5", cod_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_regione_climatica = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr6", n_id_udd = "udd6", n_id_udb = "udb6", cod_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_regione_climatica = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr7", n_id_udd = "udd7", n_id_udb = "udb7", cod_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_regione_climatica = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr8", n_id_udd = "udd8", n_id_udb = "udb8", cod_remi = "codiceremi3", codice_pdr = "codicepdr3", cat_uso = "catuso3", classe_prelievo = "classeprelievo3", zona_climatica = "zonaclimatica3", id_regione_climatica = "idregclim3", cod_prof_prel_std = "codprofprelstd3", prelievo_annuo_prev = "prelievoannuoprev3", trattamento = "trattamento3", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr9", n_id_udd = "udd9", n_id_udb = "udb9", cod_remi = "codiceremi4", codice_pdr = "codicepdr4", cat_uso = "catuso4", classe_prelievo = "classeprelievo4", zona_climatica = "zonaclimatica4", id_regione_climatica = "idregclim4", cod_prof_prel_std = "codprofprelstd4", prelievo_annuo_prev = "prelievoannuoprev4", trattamento = "trattamento4", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr10", n_id_udd = "udd10", n_id_udb = "udb10", cod_remi = "codiceremi5", codice_pdr = "codicepdr5", cat_uso = "catuso5", classe_prelievo = "classeprelievo5", zona_climatica = "zonaclimatica5", id_regione_climatica = "idregclim5", cod_prof_prel_std = "codprofprelstd5", prelievo_annuo_prev = "prelievoannuoprev5", trattamento = "trattamento5", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr11", n_id_udd = "udd11", n_id_udb = "udb11", cod_remi = "codiceremi6", codice_pdr = "codicepdr6", cat_uso = "catuso6", classe_prelievo = "classeprelievo6", zona_climatica = "zonaclimatica6", id_regione_climatica = "idregclim6", cod_prof_prel_std = "codprofprelstd6", prelievo_annuo_prev = "prelievoannuoprev6", trattamento = "trattamento6", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr12", n_id_udd = "udd12", n_id_udb = "udb12", cod_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_regione_climatica = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr13", n_id_udd = "udd13", n_id_udb = "udb13", cod_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_regione_climatica = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr14", n_id_udd = "udd14", n_id_udb = "udb14", cod_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_regione_climatica = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr15", n_id_udd = "udd15", n_id_udb = "udb15", cod_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_regione_climatica = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr16", n_id_udd = "udd16", n_id_udb = "udb16", cod_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_regione_climatica = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr17", n_id_udd = "udd17", n_id_udb = "udb17", cod_remi = "codiceremi8", codice_pdr = "codicepdr8", cat_uso = "catuso8", classe_prelievo = "classeprelievo8", zona_climatica = "zonaclimatica8", id_regione_climatica = "idregclim8", cod_prof_prel_std = "codprofprelstd8", prelievo_annuo_prev = "prelievoannuoprev8", trattamento = "trattamento8", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr18", n_id_udd = "udd18", n_id_udb = "udb18", cod_remi = "codiceremi8", codice_pdr = "codicepdr8", cat_uso = "catuso8", classe_prelievo = "classeprelievo8", zona_climatica = "zonaclimatica8", id_regione_climatica = "idregclim8", cod_prof_prel_std = "codprofprelstd8", prelievo_annuo_prev = "prelievoannuoprev8", trattamento = "trattamento8", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
      , CaFinalLikeCodProfTds(n_id_distr = "distr19", n_id_udd = "udd19", n_id_udb = "udb19", cod_remi = "codiceremi8", codice_pdr = "codicepdr8", cat_uso = "catuso8", classe_prelievo = "classeprelievo8", zona_climatica = "zonaclimatica8", id_regione_climatica = "idregclim8", cod_prof_prel_std = "codprofprelstd8", prelievo_annuo_prev = "prelievoannuoprev8", trattamento = "trattamento8", pres_tds = "SI", anno_competenza = 2022, massivo_freeze_execution_id = 456, execution_id = 123)
    ).toDF(
      CaFinalLikeSchema.getValues: _*
    ).selectExpr(CaFinalLikeSchema.getValues: _*)

    val distr = Seq(
      ("distr1", "pivadistr1")
      , ("distr2", "pivadistr2")
    )
      .toDF(RcugasDistributoreSchema.n_id_distributore, RcugasDistributoreSchema.t_piva)

    val azienda = Seq(
      ("udd1", "pivaudd1")
      , ("udd2", "pivaudd2")
    )
      .toDF(RcugasAziendaSchema.n_id_azienda, RcugasAziendaSchema.t_piva)

    val udb = Seq(
      ("udb1", "pivaudb1")
      , ("udb2", "pivaudb2")
    )
      .toDF(RcugasUdbSchema.n_id_udb, RcugasUdbSchema.n_id_azienda)

    val res = CaAggds.run(CaAggds.convertCaLikeInCaFinal(df), distr, azienda, udb, executionId)

    res.show()
  }
}
