package it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa.flowRun

import it.eng.au.aggregatoreConsumiCdp.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCdp.model.CaFinal
import it.eng.au.aggregatoreConsumiCdp.schema._
import it.eng.au.aggregatoreConsumiCdp.utility.Environment
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import java.sql.Timestamp

class AggricTest extends EnvironmentSparkTest {
  val sqlContext = Environment.sqlContext
  import sqlContext.implicits._

  def testRun(): Unit = {
    val executionId = Timestamp.valueOf(Environment.dateRun).getTime

    val df = Seq(
      CaFinal(n_id_distr = "distr1", n_id_az_udd = "udd1", n_id_udb = "udb1", piva_distr = "pivadistr1", piva_udd = "pivaudd1", piva_udb = "pivaudb1", codice_remi = "codiceremi1", codice_pdr = "codicepdr1", cat_uso = "catuso1", classe_prelievo = "classeprelievo1", zona_climatica = "zonaclimatica1", id_reg_clim = "idregclim1", cod_prof_prel_std = "codprofprelstd1", prelievo_annuo_prev = "prelievoannuoprev1", trattamento = "trattamento1", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr2", n_id_az_udd = "udd2", n_id_udb = "udb2", piva_distr = "pivadistr2", piva_udd = "pivaudd2", piva_udb = "pivaudb2", codice_remi = "codiceremi1", codice_pdr = "codicepdr1", cat_uso = "catuso1", classe_prelievo = "classeprelievo1", zona_climatica = "zonaclimatica1", id_reg_clim = "idregclim1", cod_prof_prel_std = "codprofprelstd1", prelievo_annuo_prev = "prelievoannuoprev1", trattamento = "trattamento1", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr3", n_id_az_udd = "udd3", n_id_udb = "udb3", piva_distr = "pivadistr3", piva_udd = "pivaudd3", piva_udb = "pivaudb3", codice_remi = "codiceremi1", codice_pdr = "codicepdr1", cat_uso = "catuso1", classe_prelievo = "classeprelievo1", zona_climatica = "zonaclimatica1", id_reg_clim = "idregclim1", cod_prof_prel_std = "codprofprelstd1", prelievo_annuo_prev = "prelievoannuoprev1", trattamento = "trattamento1", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr4", n_id_az_udd = "udd4", n_id_udb = "udb4", piva_distr = "pivadistr4", piva_udd = "pivaudd4", piva_udb = "pivaudb4", codice_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_reg_clim = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr5", n_id_az_udd = "udd5", n_id_udb = "udb5", piva_distr = "pivadistr5", piva_udd = "pivaudd5", piva_udb = "pivaudb5", codice_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_reg_clim = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr6", n_id_az_udd = "udd6", n_id_udb = "udb6", piva_distr = "pivadistr6", piva_udd = "pivaudd6", piva_udb = "pivaudb6", codice_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_reg_clim = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr7", n_id_az_udd = "udd7", n_id_udb = "udb7", piva_distr = "pivadistr7", piva_udd = "pivaudd7", piva_udb = "pivaudb7", codice_remi = "codiceremi2", codice_pdr = "codicepdr2", cat_uso = "catuso2", classe_prelievo = "classeprelievo2", zona_climatica = "zonaclimatica2", id_reg_clim = "idregclim2", cod_prof_prel_std = "codprofprelstd2", prelievo_annuo_prev = "prelievoannuoprev2", trattamento = "trattamento2", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr8", n_id_az_udd = "udd8", n_id_udb = "udb8", piva_distr = "pivadistr8", piva_udd = "pivaudd8", piva_udb = "pivaudb8", codice_remi = "codiceremi3", codice_pdr = "codicepdr3", cat_uso = "catuso3", classe_prelievo = "classeprelievo3", zona_climatica = "zonaclimatica3", id_reg_clim = "idregclim3", cod_prof_prel_std = "codprofprelstd3", prelievo_annuo_prev = "prelievoannuoprev3", trattamento = "trattamento3", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr9", n_id_az_udd = "udd9", n_id_udb = "udb9", piva_distr = "pivadistr9", piva_udd = "pivaudd9", piva_udb = "pivaudb9", codice_remi = "codiceremi4", codice_pdr = "codicepdr4", cat_uso = "catuso4", classe_prelievo = "classeprelievo4", zona_climatica = "zonaclimatica4", id_reg_clim = "idregclim4", cod_prof_prel_std = "codprofprelstd4", prelievo_annuo_prev = "prelievoannuoprev4", trattamento = "trattamento4", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr10", n_id_az_udd = "udd10", n_id_udb = "udb10", piva_distr = "pivadistr10", piva_udd = "pivaudd10", piva_udb = "pivaudb10", codice_remi = "codiceremi5", codice_pdr = "codicepdr5", cat_uso = "catuso5", classe_prelievo = "classeprelievo5", zona_climatica = "zonaclimatica5", id_reg_clim = "idregclim5", cod_prof_prel_std = "codprofprelstd5", prelievo_annuo_prev = "prelievoannuoprev5", trattamento = "trattamento5", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr11", n_id_az_udd = "udd11", n_id_udb = "udb11", piva_distr = "pivadistr11", piva_udd = "pivaudd11", piva_udb = "pivaudb11", codice_remi = "codiceremi6", codice_pdr = "codicepdr6", cat_uso = "catuso6", classe_prelievo = "classeprelievo6", zona_climatica = "zonaclimatica6", id_reg_clim = "idregclim6", cod_prof_prel_std = "codprofprelstd6", prelievo_annuo_prev = "prelievoannuoprev6", trattamento = "trattamento6", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr12", n_id_az_udd = "udd12", n_id_udb = "udb12", piva_distr = "pivadistr12", piva_udd = "pivaudd12", piva_udb = "pivaudb12", codice_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_reg_clim = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr13", n_id_az_udd = "udd13", n_id_udb = "udb13", piva_distr = "pivadistr13", piva_udd = "pivaudd13", piva_udb = "pivaudb13", codice_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_reg_clim = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr14", n_id_az_udd = "udd14", n_id_udb = "udb14", piva_distr = "pivadistr14", piva_udd = "pivaudd14", piva_udb = "pivaudb14", codice_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_reg_clim = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr15", n_id_az_udd = "udd15", n_id_udb = "udb15", piva_distr = "pivadistr15", piva_udd = "pivaudd15", piva_udb = "pivaudb15", codice_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_reg_clim = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr16", n_id_az_udd = "udd16", n_id_udb = "udb16", piva_distr = "pivadistr16", piva_udd = "pivaudd16", piva_udb = "pivaudb16", codice_remi = "codiceremi7", codice_pdr = "codicepdr7", cat_uso = "catuso7", classe_prelievo = "classeprelievo7", zona_climatica = "zonaclimatica7", id_reg_clim = "idregclim7", cod_prof_prel_std = "codprofprelstd7", prelievo_annuo_prev = "prelievoannuoprev7", trattamento = "trattamento7", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr17", n_id_az_udd = "udd17", n_id_udb = "udb17", piva_distr = "pivadistr17", piva_udd = "pivaudd17", piva_udb = "pivaudb17", codice_remi = "codiceremi8", codice_pdr = "codicepdr8", cat_uso = "catuso8", classe_prelievo = "classeprelievo8", zona_climatica = "zonaclimatica8", id_reg_clim = "idregclim8", cod_prof_prel_std = "codprofprelstd8", prelievo_annuo_prev = "prelievoannuoprev8", trattamento = "trattamento8", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr18", n_id_az_udd = "udd18", n_id_udb = "udb18", piva_distr = "pivadistr18", piva_udd = "pivaudd18", piva_udb = "pivaudb18", codice_remi = "codiceremi8", codice_pdr = "codicepdr8", cat_uso = "catuso8", classe_prelievo = "classeprelievo8", zona_climatica = "zonaclimatica8", id_reg_clim = "idregclim8", cod_prof_prel_std = "codprofprelstd8", prelievo_annuo_prev = "prelievoannuoprev8", trattamento = "trattamento8", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
      , CaFinal(n_id_distr = "distr19", n_id_az_udd = "udd19", n_id_udb = "udb19", piva_distr = "pivadistr19", piva_udd = "pivaudd19", piva_udb = "pivaudb19", codice_remi = "codiceremi8", codice_pdr = "codicepdr8", cat_uso = "catuso8", classe_prelievo = "classeprelievo8", zona_climatica = "zonaclimatica8", id_reg_clim = "idregclim8", cod_prof_prel_std = "codprofprelstd8", prelievo_annuo_prev = "prelievoannuoprev8", trattamento = "trattamento8", pres_tds = "SI", anno_competenza = "2022", tipo_trasmissione = "AGG_RIC", massivo_freeze_executionid = 456, executionid = 123)
    ).toDF(
      CaFinalSchema.getValues: _*
    ).selectExpr(CaFinalSchema.getValues: _*)

    val distr = Seq(
      ("niddistr1", "pivadistr1")
      , ("niddistr2", "pivadistr2")
    )
      .toDF(RcugasDistributoreSchema.n_id_distributore, RcugasDistributoreSchema.t_piva)

    val azienda = Seq(
      ("nidazudd1", "pivaazienda1")
      , ("nidazudd2", "pivaazienda2")
    )
      .toDF(RcugasAziendaSchema.n_id_azienda, RcugasAziendaSchema.t_piva)

    val udb = Seq(
      ("nidudb1", "nidazudd1")
      , ("nidudb2", "nidazudd2")
    )
      .toDF(RcugasUdbSchema.n_id_udb, RcugasUdbSchema.n_id_azienda)

    val massivoFreezer = Seq(
      ("codicepdr1", "pivaudd1", "SWG", "123", "2021-11-20 00:00:00.0")
      , ("codicepdr1", "pivaudd2", "SWG", "123", "2021-11-20 00:00:00.0")
      , ("codicepdr1", "pivaudd3", "UIG", "123", "2021-11-20 00:00:00.0")
      , ("codicepdr2", "pivaudd4", "SWG", "123", "2021-11-20 00:00:00.0")
      , ("codicepdr2", "pivaudd5", "UIG", "123", "2021-11-20 00:00:00.0")

    )
      .toDF(
        MassivoFreezeSchema.t_codice_pdr,
        MassivoFreezeSchema.piva_udd,
        MassivoFreezeSchema.t_processo,
        MassivoFreezeSchema.n_id_fornitura,
        MassivoFreezeSchema.data_fine_for
      )

    val ca = Seq(
      ("codicepdr1", 1, 1648648490000L),
      ("codicepdr2", 0, 1648648490000L),
      ("codicepdr3", 2, 1648648490000L),
      ("codicepdr4", 0, 1648648490000L),
      ("codicepdr5", 0, 1648648490000L),
      ("codicepdr65", 0, 1648648490000L),
      ("codicepdr7", 0, 1648648490000L),
      ("codicepdr8", 0, 1637929282000L)

    ).toDF(CaSchema.getValues: _*)
      .filter(col(CaSchema.executionid) === Environment.getCaFinalExecutionId)

    val prepCa = CaAggric.prepare(df)
    val resTransform = CaAggric.transform(prepCa, distr, azienda, udb, executionId)
    val resTransformFiltered = CaAggric.removeDedotti(resTransform, ca)
    val (dataDecorrenza, execIdMassivo) = CaAggric.getDataDecorrenzaExecIdMassivo(resTransformFiltered)
    val recoverSwitch = CaAggric.recoverUddSwitching(resTransformFiltered, massivoFreezer, "01/10/" + dataDecorrenza, execIdMassivo)

    val result = CaAggric.writeCaOnHive(recoverSwitch)

    result.show(100, false)
  }
}
