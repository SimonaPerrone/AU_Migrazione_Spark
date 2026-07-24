package it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa.flowRun

import it.eng.au.aggregatoreConsumiCdp.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCdp.model.CaFinal
import it.eng.au.aggregatoreConsumiCdp.schema.{CaFinalSchema, RcugasAziendaSchema, RcugasDistributoreSchema, RcugasUdbSchema}
import it.eng.au.aggregatoreConsumiCdp.utility.Constants.{DATA_DECORRENZA_FORMAT, TIMESTAMP_FORMAT}
import it.eng.au.aggregatoreConsumiCdp.utility.Environment
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

import java.sql.Timestamp

class CaPreTest extends EnvironmentSparkTest {
  val sqlContext = Environment.sqlContext
  import sqlContext.implicits._

  def testRun(): Unit = {
    val executionId = Timestamp.valueOf(Environment.dateRun).getTime

    val df = Seq(
      ("idsagann1", "distr1", "udd1", "udb1", "pivadistr1", "pivaudd1", "pivaudb1", "codiceremi1", "codicepdr1", "captrasppdr1", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "dricezione1", "tipotrasmissione1", "codistat1", "annocompetenza1", "executionid1")
      , ("idsagann1", "distr1", "udd1", "udb1", "pivadistr2", "pivaudd2", "pivaudb2", "codiceremi1", "codicepdr1", "captrasppdr1", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "dricezione1", "tipotrasmissione1", "codistat1", "annocompetenza1", "executionid1")
      , ("idsagann1", "distr1", "udd1", "udb1", "pivadistr3", "pivaudd3", "pivaudb3", "codiceremi1", "codicepdr1", "captrasppdr1", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "dricezione1", "tipotrasmissione1", "codistat1", "annocompetenza1", "executionid1")
      , ("idsagann2", "distr2", "udd3", "udb2", "pivadistr4", "pivaudd4", "pivaudb4", "codiceremi2", "codicepdr2", "captrasppdr2", "catuso2", "classeprelievo2", "zonaclimatica2", "idregclim2", "codprofprelstd2", "prelievoannuoprev2", "trattamento2", "dricezione2", "tipotrasmissione2", "codistat2", "annocompetenza2", "executionid2")
      , ("idsagann2", "distr2", "udd4", "udb2", "pivadistr5", "pivaudd5", "pivaudb5", "codiceremi2", "codicepdr2", "captrasppdr2", "catuso2", "classeprelievo2", "zonaclimatica2", "idregclim2", "codprofprelstd2", "prelievoannuoprev2", "trattamento2", "dricezione2", "tipotrasmissione2", "codistat2", "annocompetenza2", "executionid2")
      , ("idsagann2", "distr2", "udd5", "udb2", "pivadistr6", "pivaudd6", "pivaudb6", "codiceremi2", "codicepdr2", "captrasppdr2", "catuso2", "classeprelievo2", "zonaclimatica2", "idregclim2", "codprofprelstd2", "prelievoannuoprev2", "trattamento2", "dricezione2", "tipotrasmissione2", "codistat2", "annocompetenza2", "executionid2")
      , ("idsagann2", "distr2", "udd2", "udb2", "pivadistr7", "pivaudd7", "pivaudb7", "codiceremi2", "codicepdr2", "captrasppdr2", "catuso2", "classeprelievo2", "zonaclimatica2", "idregclim2", "codprofprelstd2", "prelievoannuoprev2", "trattamento2", "dricezione2", "tipotrasmissione2", "codistat2", "annocompetenza2", "executionid2")
      , ("idsagann3", "distr2", "udd2", "udb3", "pivadistr8", "pivaudd8", "pivaudb8", "codiceremi3", "codicepdr3", "captrasppdr3", "catuso3", "classeprelievo3", "zonaclimatica3", "idregclim3", "codprofprelstd3", "prelievoannuoprev3", "trattamento3", "dricezione3", "tipotrasmissione3", "codistat3", "annocompetenza3", "executionid3")
      , ("idsagann4", "distr2", "udd2", "udb4", "pivadistr9", "pivaudd9", "pivaudb9", "codiceremi4", "codicepdr4", "captrasppdr4", "catuso4", "classeprelievo4", "zonaclimatica4", "idregclim4", "codprofprelstd4", "prelievoannuoprev4", "trattamento4", "dricezione4", "tipotrasmissione4", "codistat4", "annocompetenza4", "executionid4")
      , ("idsagann5", "distr2", "udd2", "udb5", "pivadistr10", "pivaudd10", "pivaudb10", "codiceremi5", "codicepdr5", "captrasppdr5", "catuso5", "classeprelievo5", "zonaclimatica5", "idregclim5", "codprofprelstd5", "prelievoannuoprev5", "trattamento5", "dricezione5", "tipotrasmissione5", "codistat5", "annocompetenza5", "executionid5")
      , ("idsagann6", "distr2", "udd2", "udb6", "pivadistr11", "pivaudd11", "pivaudb11", "codiceremi6", "codicepdr6", "captrasppdr6", "catuso6", "classeprelievo6", "zonaclimatica6", "idregclim6", "codprofprelstd6", "prelievoannuoprev6", "trattamento6", "dricezione6", "tipotrasmissione6", "codistat6", "annocompetenza6", "executionid6")
      , ("idsagann7", "distr2", "udd2", "udb7", "pivadistr12", "pivaudd12", "pivaudb12", "codiceremi7", "codicepdr7", "captrasppdr7", "catuso7", "classeprelievo7", "zonaclimatica7", "idregclim7", "codprofprelstd7", "prelievoannuoprev7", "trattamento7", "dricezione7", "tipotrasmissione7", "codistat7", "annocompetenza7", "executionid7")
      , ("idsagann7", "distr3", "udd2", "udb7", "pivadistr13", "pivaudd13", "pivaudb13", "codiceremi7", "codicepdr7", "captrasppdr7", "catuso7", "classeprelievo7", "zonaclimatica7", "idregclim7", "codprofprelstd7", "prelievoannuoprev7", "trattamento7", "dricezione7", "tipotrasmissione7", "codistat7", "annocompetenza7", "executionid7")
      , ("idsagann7", "distr4", "udd2", "udb7", "pivadistr14", "pivaudd14", "pivaudb14", "codiceremi7", "codicepdr7", "captrasppdr7", "catuso7", "classeprelievo7", "zonaclimatica7", "idregclim7", "codprofprelstd7", "prelievoannuoprev7", "trattamento7", "dricezione7", "tipotrasmissione7", "codistat7", "annocompetenza7", "executionid7")
      , ("idsagann7", "distr5", "udd2", "udb7", "pivadistr15", "pivaudd15", "pivaudb15", "codiceremi7", "codicepdr7", "captrasppdr7", "catuso7", "classeprelievo7", "zonaclimatica7", "idregclim7", "codprofprelstd7", "prelievoannuoprev7", "trattamento7", "dricezione7", "tipotrasmissione7", "codistat7", "annocompetenza7", "executionid7")
      , ("idsagann7", "distr5", "udd2", "udb7", "pivadistr16", "pivaudd16", "pivaudb16", "codiceremi7", "codicepdr7", "captrasppdr7", "catuso7", "classeprelievo7", "zonaclimatica7", "idregclim7", "codprofprelstd7", "prelievoannuoprev7", "trattamento7", "dricezione7", "tipotrasmissione7", "codistat7", "annocompetenza7", "executionid7")
      , ("idsagann8", "distr8", "udd8", "udb8", "pivadistr17", "pivaudd17", "pivaudb17", "codiceremi8", "codicepdr8", "captrasppdr8", "catuso8", "classeprelievo8", "zonaclimatica8", "idregclim8", "codprofprelstd8", "prelievoannuoprev8", "trattamento8", "dricezione8", "tipotrasmissione8", "codistat8", "annocompetenza8", "executionid8")
      , ("idsagann8", "distr9", "udd8", "udb8", "pivadistr18", "pivaudd18", "pivaudb18", "codiceremi8", "codicepdr8", "captrasppdr8", "catuso8", "classeprelievo8", "zonaclimatica8", "idregclim8", "codprofprelstd8", "prelievoannuoprev8", "trattamento8", "dricezione8", "tipotrasmissione8", "codistat8", "annocompetenza8", "executionid8")
      , ("idsagann8", "distr10", "udd8", "udb8", "pivadistr19", "pivaudd19", "pivaudb19", "codiceremi8", "codicepdr8", "captrasppdr8", "catuso8", "classeprelievo8", "zonaclimatica8", "idregclim8", "codprofprelstd8", "prelievoannuoprev8", "trattamento8", "dricezione8", "tipotrasmissione8", "codistat8", "annocompetenza8", "executionid8")
    ).toDF(
      "id_sag_ann",
      "n_id_distr",
      "n_id_az_udd",
      "n_id_udb",
      "piva_distr",
      "piva_udd",
      "piva_udb",
      "codice_remi",
      "codice_pdr",
      "cap_trasp_pdr",
      "cat_uso",
      "classe_prelievo",
      "zona_climatica",
      "id_reg_clim",
      "cod_prof_prel_std",
      "prelievo_annuo_prev",
      "trattamento",
      "d_ricezione",
      "tipo_trasmissione",
      "codistat",
      "anno_competenza",
      "executionid"
    )

    val prepDf = CaFin.prepare(df)

    val distr = Seq(
      ("distr1", "pivadistr1")
      , ("distr2", "pivadistr2")
    )
      .toDF(RcugasDistributoreSchema.n_id_distributore, RcugasDistributoreSchema.t_piva)

    val azienda = Seq(
      ("udd1", "pivaazienda1")
      , ("udd2", "pivaazienda2")
    )
      .toDF(RcugasAziendaSchema.n_id_azienda, RcugasAziendaSchema.t_piva)

    val udb = Seq(
      ("udb1", "udd1")
      , ("udb2", "udd2")
    )
      .toDF(RcugasUdbSchema.n_id_udb, RcugasUdbSchema.n_id_azienda)


    CaPre.transform(prepDf, distr, azienda, udb, executionId)
      .show()
  }

  def testWrite(): Unit = {
    val executionId = Timestamp.valueOf(Environment.dateRun).getTime

    val df = Seq(
      CaFinal(n_id_distr = "distr1", n_id_az_udd = "udd1", n_id_udb = "udb1", codice_remi = "codiceremi1", codice_pdr = "codicepdr1", cat_uso = "catuso1", classe_prelievo = "classeprelievo1", zona_climatica = "zonaclimatica1", id_reg_clim = "idregclim1", cod_prof_prel_std = "codprofprelstd1", prelievo_annuo_prev = "prelievoannuoprev1", trattamento = "trattamento1", pres_tds = "SI", anno_competenza = "annocompetenza1", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr2", n_id_az_udd = "udd2", n_id_udb = "udb2", codice_remi = "codiceremi1", codice_pdr = "codicepdr1", cat_uso = "catuso1", classe_prelievo = "classeprelievo1", zona_climatica = "zonaclimatica1", id_reg_clim = "idregclim1", cod_prof_prel_std = "codprofprelstd1", prelievo_annuo_prev = "prelievoannuoprev1", trattamento = "trattamento1", pres_tds = "SI", anno_competenza = "annocompetenza1", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr3", n_id_az_udd = "udd3", n_id_udb = "udb3", codice_remi = "codiceremi1", codice_pdr = "codicepdr1", cat_uso = "catuso1", classe_prelievo = "classeprelievo1", zona_climatica = "zonaclimatica1", id_reg_clim = "idregclim1", cod_prof_prel_std = "codprofprelstd1", prelievo_annuo_prev = "prelievoannuoprev1", trattamento = "trattamento1", pres_tds = "SI", anno_competenza = "annocompetenza1", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr4", n_id_az_udd = "udd4", n_id_udb = "udb4", codice_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_reg_clim = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = "annocompetenza2", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr5", n_id_az_udd = "udd5", n_id_udb = "udb5", codice_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_reg_clim = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = "annocompetenza2", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr6", n_id_az_udd = "udd6", n_id_udb = "udb6", codice_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_reg_clim = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = "annocompetenza2", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr7", n_id_az_udd = "udd7", n_id_udb = "udb7", codice_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_reg_clim = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = "annocompetenza2", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr8", n_id_az_udd = "udd8", n_id_udb = "udb8", codice_remi = "codiceremi3", codice_pdr = "codicepdr3", cat_uso = "catuso3", classe_prelievo = "classeprelievo3", zona_climatica = "zonaclimatica3", id_reg_clim = "idregclim3", cod_prof_prel_std = "codprofprelstd3", prelievo_annuo_prev = "prelievoannuoprev3", trattamento = "trattamento3", pres_tds = "SI", anno_competenza = "annocompetenza3", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr9", n_id_az_udd = "udd9", n_id_udb = "udb9", codice_remi = "codiceremi4", codice_pdr = "codicepdr4", cat_uso = "catuso4", classe_prelievo = "classeprelievo4", zona_climatica = "zonaclimatica4", id_reg_clim = "idregclim4", cod_prof_prel_std = "codprofprelstd4", prelievo_annuo_prev = "prelievoannuoprev4", trattamento = "trattamento4", pres_tds = "SI", anno_competenza = "annocompetenza4", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr10", n_id_az_udd = "udd10", n_id_udb = "udb10", codice_remi = "codiceremi5", codice_pdr = "codicepdr5", cat_uso = "catuso5", classe_prelievo = "classeprelievo5", zona_climatica = "zonaclimatica5", id_reg_clim = "idregclim5", cod_prof_prel_std = "codprofprelstd5", prelievo_annuo_prev = "prelievoannuoprev5", trattamento = "trattamento5", pres_tds = "SI", anno_competenza = "annocompetenza5", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr11", n_id_az_udd = "udd11", n_id_udb = "udb11", codice_remi = "codiceremi6", codice_pdr = "codicepdr6", cat_uso = "catuso6", classe_prelievo = "classeprelievo6", zona_climatica = "zonaclimatica6", id_reg_clim = "idregclim6", cod_prof_prel_std = "codprofprelstd6", prelievo_annuo_prev = "prelievoannuoprev6", trattamento = "trattamento6", pres_tds = "SI", anno_competenza = "annocompetenza6", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr12", n_id_az_udd = "udd12", n_id_udb = "udb12", codice_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_reg_clim = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = "annocompetenza7", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr13", n_id_az_udd = "udd13", n_id_udb = "udb13", codice_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_reg_clim = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = "annocompetenza7", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr14", n_id_az_udd = "udd14", n_id_udb = "udb14", codice_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_reg_clim = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = "annocompetenza7", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr15", n_id_az_udd = "udd15", n_id_udb = "udb15", codice_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_reg_clim = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = "annocompetenza7", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr16", n_id_az_udd = "udd16", n_id_udb = "udb16", codice_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_reg_clim = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = "annocompetenza7", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr17", n_id_az_udd = "udd17", n_id_udb = "udb17", codice_remi = "codiceremi8", codice_pdr = "codicepdr8", cat_uso = "catuso8", classe_prelievo = "classeprelievo8", zona_climatica = "zonaclimatica8", id_reg_clim = "idregclim8", cod_prof_prel_std = "codprofprelstd8", prelievo_annuo_prev = "prelievoannuoprev8", trattamento = "trattamento8", pres_tds = "SI", anno_competenza = "annocompetenza8", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr18", n_id_az_udd = "udd18", n_id_udb = "udb18", codice_remi = "codiceremi8", codice_pdr = "codicepdr8", cat_uso = "catuso8", classe_prelievo = "classeprelievo8", zona_climatica = "zonaclimatica8", id_reg_clim = "idregclim8", cod_prof_prel_std = "codprofprelstd8", prelievo_annuo_prev = "prelievoannuoprev8", trattamento = "trattamento8", pres_tds = "SI", anno_competenza = "annocompetenza8", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
      , CaFinal(n_id_distr = "distr19", n_id_az_udd = "udd19", n_id_udb = "udb19", codice_remi = "codiceremi8", codice_pdr = "codicepdr8", cat_uso = "catuso8", classe_prelievo = "classeprelievo8", zona_climatica = "zonaclimatica8", id_reg_clim = "idregclim8", cod_prof_prel_std = "codprofprelstd8", prelievo_annuo_prev = "prelievoannuoprev8", trattamento = "trattamento8", pres_tds = "SI", anno_competenza = "annocompetenza8", tipo_trasmissione = "PRE", massivo_freeze_executionid = 123, executionid = 123)
    ).toDF(
      CaFinalSchema.getValues: _*
    ).selectExpr(CaFinalSchema.getValues: _*)

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


    val res = CaPre.run(df, distr, azienda, udb, executionId)

    res.show()
  }
}
