package it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa.flowRun

import it.eng.au.aggregatoreConsumiCdp.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCdp.model.{CaFinal, CaPreFinal}
import it.eng.au.aggregatoreConsumiCdp.schema._
import it.eng.au.aggregatoreConsumiCdp.utility.Environment
import org.apache.spark.sql.functions._
import org.junit.Assert

import java.sql.Timestamp

class DedottiTest extends EnvironmentSparkTest {
  val sqlContext = Environment.sqlContext
  import sqlContext.implicits._

  def testRun(): Unit = {
    val executionId = Timestamp.valueOf(Environment.dateRun).getTime

    val caFinal = Seq(
      CaFinal(n_id_distr = "distr1", n_id_az_udd = "udd1", n_id_udb = "udb1", piva_distr = "pivadistr1", piva_udd = "pivaudd1", piva_udb = "pivaudb1", codice_remi = "codiceremi1", codice_pdr = "codicepdr1", cat_uso = "catuso1", classe_prelievo = "classeprelievo1", zona_climatica = "zonaclimatica1", id_reg_clim = "idregclim1", cod_prof_prel_std = "codprofprelstd1", prelievo_annuo_prev = "prelievoannuoprev1", trattamento = "trattamento1", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr2", n_id_az_udd = "udd2", n_id_udb = "udb2", piva_distr = "pivadistr2", piva_udd = "pivaudd2", piva_udb = "pivaudb2", codice_remi = "codiceremi1", codice_pdr = "codicepdr1", cat_uso = "catuso1", classe_prelievo = "classeprelievo1", zona_climatica = "zonaclimatica1", id_reg_clim = "idregclim1", cod_prof_prel_std = "codprofprelstd1", prelievo_annuo_prev = "prelievoannuoprev1", trattamento = "trattamento1", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr3", n_id_az_udd = "udd3", n_id_udb = "udb3", piva_distr = "pivadistr3", piva_udd = "pivaudd3", piva_udb = "pivaudb3", codice_remi = "codiceremi1", codice_pdr = "codicepdr1", cat_uso = "catuso1", classe_prelievo = "classeprelievo1", zona_climatica = "zonaclimatica1", id_reg_clim = "idregclim1", cod_prof_prel_std = "codprofprelstd1", prelievo_annuo_prev = "prelievoannuoprev1", trattamento = "trattamento1", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr4", n_id_az_udd = "udd4", n_id_udb = "udb4", piva_distr = "pivadistr4", piva_udd = "pivaudd4", piva_udb = "pivaudb4", codice_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_reg_clim = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr5", n_id_az_udd = "udd5", n_id_udb = "udb5", piva_distr = "pivadistr5", piva_udd = "pivaudd5", piva_udb = "pivaudb5", codice_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_reg_clim = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr6", n_id_az_udd = "udd6", n_id_udb = "udb6", piva_distr = "pivadistr6", piva_udd = "pivaudd6", piva_udb = "pivaudb6", codice_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_reg_clim = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr7", n_id_az_udd = "udd7", n_id_udb = "udb7", piva_distr = "pivadistr7", piva_udd = "pivaudd7", piva_udb = "pivaudb7", codice_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_reg_clim = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr8", n_id_az_udd = "udd8", n_id_udb = "udb8", piva_distr = "pivadistr8", piva_udd = "pivaudd8", piva_udb = "pivaudb8", codice_remi = "codiceremi3", codice_pdr = "codicepdr3", cat_uso = "catuso3", classe_prelievo = "classeprelievo3", zona_climatica = "zonaclimatica3", id_reg_clim = "idregclim3", cod_prof_prel_std = "codprofprelstd3", prelievo_annuo_prev = "prelievoannuoprev3", trattamento = "trattamento3", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr9", n_id_az_udd = "udd9", n_id_udb = "udb9", piva_distr = "pivadistr9", piva_udd = "pivaudd9", piva_udb = "pivaudb9", codice_remi = "codiceremi4", codice_pdr = "codicepdr4", cat_uso = "catuso4", classe_prelievo = "classeprelievo4", zona_climatica = "zonaclimatica4", id_reg_clim = "idregclim4", cod_prof_prel_std = "codprofprelstd4", prelievo_annuo_prev = "prelievoannuoprev4", trattamento = "trattamento4", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr10", n_id_az_udd = "udd10", n_id_udb = "udb10", piva_distr = "pivadistr10", piva_udd = "pivaudd10", piva_udb = "pivaudb10", codice_remi = "codiceremi5", codice_pdr = "codicepdr5", cat_uso = "catuso5", classe_prelievo = "classeprelievo5", zona_climatica = "zonaclimatica5", id_reg_clim = "idregclim5", cod_prof_prel_std = "codprofprelstd5", prelievo_annuo_prev = "prelievoannuoprev5", trattamento = "trattamento5", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr11", n_id_az_udd = "udd11", n_id_udb = "udb11", piva_distr = "pivadistr11", piva_udd = "pivaudd11", piva_udb = "pivaudb11", codice_remi = "codiceremi6", codice_pdr = "codicepdr6", cat_uso = "catuso6", classe_prelievo = "classeprelievo6", zona_climatica = "zonaclimatica6", id_reg_clim = "idregclim6", cod_prof_prel_std = "codprofprelstd6", prelievo_annuo_prev = "prelievoannuoprev6", trattamento = "trattamento6", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr12", n_id_az_udd = "udd12", n_id_udb = "udb12", piva_distr = "pivadistr12", piva_udd = "pivaudd12", piva_udb = "pivaudb12", codice_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_reg_clim = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr13", n_id_az_udd = "udd13", n_id_udb = "udb13", piva_distr = "pivadistr13", piva_udd = "pivaudd13", piva_udb = "pivaudb13", codice_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_reg_clim = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr14", n_id_az_udd = "udd14", n_id_udb = "udb14", piva_distr = "pivadistr14", piva_udd = "pivaudd14", piva_udb = "pivaudb14", codice_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_reg_clim = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr15", n_id_az_udd = "udd15", n_id_udb = "udb15", piva_distr = "pivadistr15", piva_udd = "pivaudd15", piva_udb = "pivaudb15", codice_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_reg_clim = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr16", n_id_az_udd = "udd16", n_id_udb = "udb16", piva_distr = "pivadistr16", piva_udd = "pivaudd16", piva_udb = "pivaudb16", codice_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_reg_clim = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr17", n_id_az_udd = "udd17", n_id_udb = "udb17", piva_distr = "pivadistr17", piva_udd = "pivaudd17", piva_udb = "pivaudb17", codice_remi = "codiceremi8", codice_pdr = "codicepdr8", cat_uso = "catuso8", classe_prelievo = "classeprelievo8", zona_climatica = "zonaclimatica8", id_reg_clim = "idregclim8", cod_prof_prel_std = "codprofprelstd8", prelievo_annuo_prev = "prelievoannuoprev8", trattamento = "trattamento8", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr18", n_id_az_udd = "udd18", n_id_udb = "udb18", piva_distr = "pivadistr18", piva_udd = "pivaudd18", piva_udb = "pivaudb18", codice_remi = "codiceremi8", codice_pdr = "codicepdr8", cat_uso = "catuso8", classe_prelievo = "classeprelievo8", zona_climatica = "zonaclimatica8", id_reg_clim = "idregclim8", cod_prof_prel_std = "codprofprelstd8", prelievo_annuo_prev = "prelievoannuoprev8", trattamento = "trattamento8", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr19", n_id_az_udd = "udd19", n_id_udb = "udb19", piva_distr = "pivadistr19", piva_udd = "pivaudd19", piva_udb = "pivaudb19", codice_remi = "codiceremi8", codice_pdr = "codicepdr8", cat_uso = "catuso8", classe_prelievo = "classeprelievo8", zona_climatica = "zonaclimatica8", id_reg_clim = "idregclim8", cod_prof_prel_std = "codprofprelstd8", prelievo_annuo_prev = "prelievoannuoprev8", trattamento = "trattamento8", pres_tds = "SI", anno_competenza = "2022", freeze_date = "2022-10-10 00:00:00", massivo_freeze_executionid = 456, executionid = 123)
    ).toDF(
      CaFinalSchema.getValues: _*
    ).selectExpr(CaFinalSchema.getValues: _*)

    val caPreFinal = Seq(
      CaPreFinal(id_sag_ann = "sagann1", n_id_distr = "distr1", n_id_az_udd = "udd1", n_id_udb = "udb1", codice_remi = "codiceremi1", codice_pdr = "codicepdr1", calcmode = "dedotto                                 ", cat_uso = "cat_uso1", classe_prelievo = "prelivo1", zona_climatica = "A", id_reg_clim = "T1", cod_prof_prel_std = "code1", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123),
      CaPreFinal(id_sag_ann = "sagann2", n_id_distr = "distr2", n_id_az_udd = "udd2", n_id_udb = "udb2", codice_remi = "codiceremi1", codice_pdr = "codicepdr1", calcmode = "                                        ", cat_uso = "cat_uso1", classe_prelievo = "prelivo1", zona_climatica = "B", id_reg_clim = "C1", cod_prof_prel_std = "code2", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123),
      CaPreFinal(id_sag_ann = "sagann3", n_id_distr = "distr3", n_id_az_udd = "udd3", n_id_udb = "udb3", codice_remi = "codiceremi1", codice_pdr = "codicepdr1", calcmode = "                                        ", cat_uso = "cat_uso1", classe_prelievo = "prelivo1", zona_climatica = "B", id_reg_clim = "C1", cod_prof_prel_std = "code3", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123),
      CaPreFinal(id_sag_ann = "sagann4", n_id_distr = "distr4", n_id_az_udd = "udd4", n_id_udb = "udb4", codice_remi = "codiceremi2", codice_pdr = "codicepdr2", calcmode = "                                        ", cat_uso = "cat_uso2", classe_prelievo = "prelivo2", zona_climatica = "A", id_reg_clim = "T1", cod_prof_prel_std = "code4", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123),
      CaPreFinal(id_sag_ann = "sagann5", n_id_distr = "distr5", n_id_az_udd = "udd5", n_id_udb = "udb5", codice_remi = "codiceremi2", codice_pdr = "codicepdr2", calcmode = "                                        ", cat_uso = "cat_uso2", classe_prelievo = "prelivo2", zona_climatica = "A", id_reg_clim = "T1", cod_prof_prel_std = "code5", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123),
      CaPreFinal(id_sag_ann = "sagann6", n_id_distr = "distr6", n_id_az_udd = "udd6", n_id_udb = "udb6", codice_remi = "codiceremi2", codice_pdr = "codicepdr2", calcmode = "                                        ", cat_uso = "cat_uso2", classe_prelievo = "prelivo2", zona_climatica = "B", id_reg_clim = "C1", cod_prof_prel_std = "code6", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123),
      CaPreFinal(id_sag_ann = "sagann7", n_id_distr = "distr7", n_id_az_udd = "udd7", n_id_udb = "udb7", codice_remi = "codiceremi3", codice_pdr = "codicepdr3", calcmode = "dedotto                                 ", cat_uso = "cat_uso3", classe_prelievo = "prelivo3", zona_climatica = "A", id_reg_clim = "T1", cod_prof_prel_std = "code7", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123),
      CaPreFinal(id_sag_ann = "sagann8", n_id_distr = "distr8", n_id_az_udd = "udd8", n_id_udb = "udb8", codice_remi = "codiceremi3", codice_pdr = "codicepdr3", calcmode = "                                        ", cat_uso = "cat_uso3", classe_prelievo = "prelivo3", zona_climatica = "A", id_reg_clim = "T1", cod_prof_prel_std = "code7", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123),
      CaPreFinal(id_sag_ann = "sagann9", n_id_distr = "distr9", n_id_az_udd = "udd9", n_id_udb = "udb9", codice_remi = "codiceremi3", codice_pdr = "codicepdr3", calcmode = "                                        ", cat_uso = "cat_uso3", classe_prelievo = "prelivo3", zona_climatica = "A", id_reg_clim = "T1", cod_prof_prel_std = "code1", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123),
      CaPreFinal(id_sag_ann = "sagann10", n_id_distr = "distr10", n_id_az_udd = "udd10", n_id_udb = "udb10", codice_remi = "codiceremi4", codice_pdr = "codicepdr4", calcmode = "dedotto                                 ", cat_uso = "cat_uso4", classe_prelievo = "prelivo4", zona_climatica = "B", id_reg_clim = "C1", cod_prof_prel_std = "code2", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123),
      CaPreFinal(id_sag_ann = "sagann11", n_id_distr = "distr11", n_id_az_udd = "udd11", n_id_udb = "udb11", codice_remi = "codiceremi4", codice_pdr = "codicepdr4", calcmode = "                                        ", cat_uso = "cat_uso4", classe_prelievo = "prelivo4", zona_climatica = "B", id_reg_clim = "C1", cod_prof_prel_std = "code3", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123),
      CaPreFinal(id_sag_ann = "sagann12", n_id_distr = "distr12", n_id_az_udd = "udd12", n_id_udb = "udb12", codice_remi = "codiceremi4", codice_pdr = "codicepdr4", calcmode = "                                        ", cat_uso = "cat_uso4", classe_prelievo = "prelivo4", zona_climatica = "A", id_reg_clim = "T1", cod_prof_prel_std = "code4", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123),
      CaPreFinal(id_sag_ann = "sagann13", n_id_distr = "distr13", n_id_az_udd = "udd13", n_id_udb = "udb13", codice_remi = "codiceremi5", codice_pdr = "codicepdr5", calcmode = "                                        ", cat_uso = "cat_uso5", classe_prelievo = "prelivo5", zona_climatica = "A", id_reg_clim = "T1", cod_prof_prel_std = "code5", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123),
      CaPreFinal(id_sag_ann = "sagann14", n_id_distr = "distr14", n_id_az_udd = "udd14", n_id_udb = "udb14", codice_remi = "codiceremi5", codice_pdr = "codicepdr5", calcmode = "                                        ", cat_uso = "cat_uso5", classe_prelievo = "prelivo5", zona_climatica = "B", id_reg_clim = "C1", cod_prof_prel_std = "code6", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123),
      CaPreFinal(id_sag_ann = "sagann15", n_id_distr = "distr15", n_id_az_udd = "udd15", n_id_udb = "udb15", codice_remi = "codiceremi5", codice_pdr = "codicepdr5", calcmode = "dedotto con forzatura per valori anomali", cat_uso = "cat_uso5", classe_prelievo = "prelivo5", zona_climatica = "A", id_reg_clim = "T1", cod_prof_prel_std = "code7", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123),
      CaPreFinal(id_sag_ann = "sagann16", n_id_distr = "distr16", n_id_az_udd = "udd16", n_id_udb = "udb16", codice_remi = "codiceremi6", codice_pdr = "codicepdr6", calcmode = "dedotto con forzatura per valori anomali", cat_uso = "cat_uso6", classe_prelievo = "prelivo6", zona_climatica = "A", id_reg_clim = "T1", cod_prof_prel_std = "code7", prelievo_annuo_prev = "prev1", trattamento = "Y", anno_competenza = "anno1", executionid = 123)
    ).toDF(
      CaPreFinalSchema.getValues: _*
    ).selectExpr(CaPreFinalSchema.getValues: _*)

    val ca = Seq(
      ("codicepdr1", 7, 1631183643380L),
      ("codicepdr1", 7, 1631183643380L),
      ("codicepdr2", 0, 1631183643380L),
      ("codicepdr3", 2, 1631183643380L),
      ("codicepdr4", 0, 1631183643380L),
      ("codicepdr5", 0, 1631183643380L),
      ("codicepdr6", 3, 1631183643380L),
      ("codicepdr7", 0, 1631183643380L),
      ("codicepdr1", 9, 1637929282000L)
    ).toDF(CaSchema.getValues: _*)
      .filter(col(CaSchema.executionid) === Environment.getCaFinalExecutionId)

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

    val prepCaFinal = CaDedotti.prepare(caFinal)
    val resTransform = CaDedotti.transform(prepCaFinal, distr, broadcast(azienda), udb, executionId)
      .drop(OutputHiveSchema.causale)

    val caPrepared = CaDedotti.prepareCa(ca)
    val preparedCaPreFinal = CaDedotti.prepareCaPreFinal(caPreFinal)

    val df = preparedCaPreFinal
      .join(caPrepared, col(CaPreFinalSchema.codice_pdr) === col(CaSchema.pdr), "left")
      .drop(CaSchema.pdr)
      .withColumn(OutputHiveSchema.causale, coalesce(col(CaPreFinalSchema.calcmode), col(CaSchema.idcaerrorcode)))
      .drop(CaPreFinalSchema.calcmode)
      .drop(CaSchema.idcaerrorcode)
      .withColumn(OutputHiveSchema.causale, when(col(OutputHiveSchema.causale).isNull, lit("M1")).otherwise(col(OutputHiveSchema.causale)))

    val res = resTransform.join(df, col(OutputHiveSchema.cod_pdr) === col(CaPreFinalSchema.codice_pdr), "inner")
      .drop(CaPreFinalSchema.codice_pdr)
    res.show(100)

    Assert.assertEquals(7, res.count())
    Assert.assertEquals(3, res.where(col(OutputHiveSchema.cod_pdr) === "codicepdr1").count())
    Assert.assertEquals(1, res.where(col(OutputHiveSchema.cod_pdr) === "codicepdr3").count())
    Assert.assertEquals(1, res.where(col(OutputHiveSchema.cod_pdr) === "codicepdr4").count())
    Assert.assertEquals(1, res.where(col(OutputHiveSchema.cod_pdr) === "codicepdr5").count())
    Assert.assertEquals(1, res.where(col(OutputHiveSchema.cod_pdr) === "codicepdr6").count())

    Assert.assertEquals(2, res.where(col(OutputHiveSchema.causale) === "DF").count())
    Assert.assertEquals(5, res.where(col(OutputHiveSchema.causale) === "M1").count())

    Assert.assertEquals(1, res.where(col(OutputHiveSchema.piva_distr) === "pivadistr1").count())
    Assert.assertEquals(1, res.where(col(OutputHiveSchema.piva_distr) === "pivadistr2").count())

    Assert.assertEquals(1, res.where(col(OutputHiveSchema.piva_udd) === "pivaudd1").count())
    Assert.assertEquals(1, res.where(col(OutputHiveSchema.piva_udd) === "pivaudd2").count())

    Assert.assertEquals(1, res.where(col(OutputHiveSchema.piva_udb) === "pivaudb1").count())
    Assert.assertEquals(1, res.where(col(OutputHiveSchema.piva_udb) === "pivaudb2").count())
  }
}
