package it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.aggric

import it.eng.au.aggregatoreConsumiCdp.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCdp.schema.OutputHiveSchema
import it.eng.au.aggregatoreConsumiCdp.utility.Environment

class AggricDistrTest extends EnvironmentSparkTest {
  val sqlContext = Environment.sqlContext
  import sqlContext.implicits._

  def testRun(): Unit = {
    val df = Seq(
      ("nidudd1", "pivaudd1", "niddistr1", "pivadistr1", "nidudb1", "pivaudb1", "codpdr1", "codremi1", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "datadecorrenza1", "2022", "uddoggettoswithcing1;uddoggettoswithcing2;uddoggettoswithcing3", "AGG_RIC", "executionid1")
      , ("nidudd1", "pivaudd1", "niddistr1", "pivadistr2", "nidudb1", "pivaudb1", "codpdr1", "codremi2", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "datadecorrenza1", "2022", "uddoggettoswithcing1", "AGG_RIC", "executionid1")
      , ("nidudd1", "pivaudd1", "niddistr1", "pivadistr3", "nidudb1", "pivaudb1", "codpdr1", "codremi3", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "datadecorrenza1", "2022", "uddoggettoswithcing1", "AGG_RIC", "executionid1")
      , ("nidudd1", "pivaudd2", "niddistr1", "pivadistr4", "nidudb1", "pivaudb1", "codpdr1", "codremi4", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "datadecorrenza1", "2022", "uddoggettoswithcing1", "AGG_RIC", "executionid1")
      , ("nidudd1", "pivaudd2", "niddistr1", "pivadistr4", "nidudb1", "pivaudb1", "codpdr1", "codremi5", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "datadecorrenza1", "2022", "uddoggettoswithcing1", "AGG_RIC", "executionid1")
      , ("nidudd1", "pivaudd2", "niddistr1", "pivadistr4", "nidudb1", "pivaudb1", "codpdr1", "codremi6", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "datadecorrenza1", "2022", "uddoggettoswithcing1", "AGG_RIC", "executionid1")
      , ("nidudd1", "pivaudd2", "niddistr1", "pivadistr4", "nidudb1", "pivaudb1", "codpdr1", "codremi7", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "datadecorrenza1", "2022", "uddoggettoswithcing1", "AGG_RIC", "executionid1")
      , ("nidudd1", "pivaudd2", "niddistr1", "pivadistr4", "nidudb1", "pivaudb1", "codpdr1", "codremi8", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "datadecorrenza1", "2022", "uddoggettoswithcing1", "AGG_RIC", "executionid1")
      , ("nidudd1", "pivaudd2", "niddistr1", "pivadistr4", "nidudb1", "pivaudb1", "codpdr1", "codremi81", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "datadecorrenza1", "2022", "uddoggettoswithcing1;uddoggettoswithcing2;uddoggettoswithcing3", "AGG_RIC", "executionid1")
      , ("nidudd1", "pivaudd2", "niddistr1", "pivadistr4", "nidudb1", "pivaudb1", "codpdr1", "codremi82", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "datadecorrenza1", "2022", "uddoggettoswithcing1", "AGG_RIC", "executionid1")
      , ("nidudd1", "pivaudd2", "niddistr1", "pivadistr4", "nidudb1", "pivaudb1", "codpdr1", "codremi83", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "datadecorrenza1", "2022", "uddoggettoswithcing1", "AGG_RIC", "executionid1")
      , ("nidudd1", "pivaudd3", "niddistr1", "pivadistr1", "nidudb1", "pivaudb1", "codpdr1", "codremi9", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "datadecorrenza1", "2022", "uddoggettoswithcing1", "AGG_RIC", "executionid1")
      , ("nidudd1", "pivaudd3", "niddistr1", "pivadistr2", "nidudb1", "pivaudb1", "codpdr1", "codremi10", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "datadecorrenza1", "2022", "uddoggettoswithcing1", "AGG_RIC", "executionid1")
      , ("nidudd1", "pivaudd3", "niddistr1", "pivadistr3", "nidudb1", "pivaudb1", "codpdr1", "codremi11", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "datadecorrenza1", "2022", "uddoggettoswithcing1", "AGG_RIC", "executionid1")
      , ("nidudd1", "pivaudd3", "niddistr1", "pivadistr4", "nidudb1", "pivaudb1", "codpdr1", "codremi12", "catuso1", "classeprelievo1", "zonaclimatica1", "idregclim1", "codprofprelstd1", "prelievoannuoprev1", "trattamento1", "datadecorrenza1", "2022", "uddoggettoswithcing1", "AGG_RIC", "executionid1")
    ).toDF(
      OutputHiveSchema.n_id_udd,
      OutputHiveSchema.piva_udd,
      OutputHiveSchema.n_id_distr,
      OutputHiveSchema.piva_distr,
      OutputHiveSchema.n_id_udb,
      OutputHiveSchema.piva_udb,
      OutputHiveSchema.cod_pdr,
      OutputHiveSchema.cod_remi,
      OutputHiveSchema.cat_uso,
      OutputHiveSchema.classe_prelievo,
      OutputHiveSchema.zona_climatica,
      OutputHiveSchema.id_reg_clim,
      OutputHiveSchema.cod_prof_prel_std,
      OutputHiveSchema.prelievo_annuo_prev,
      OutputHiveSchema.trattamento,
      OutputHiveSchema.data_decorrenza,
      OutputHiveSchema.anno_competenza,
      OutputHiveSchema.udd_oggetto_swithcing,
      OutputHiveSchema.tipo_trasmissione,
      OutputHiveSchema.execution_id
    )
    AggricDistr.run(df)
  }
}
