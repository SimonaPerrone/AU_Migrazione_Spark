package it.eng.au.ccgPubblicazione.sessionFactory.impl

import it.eng.au.ccgPubblicazione.EnvironmentSparkTest
import it.eng.au.ccgPubblicazione.controller.sessionfactory.impl.{AggSession, CdpFinSession}
import it.eng.au.ccgPubblicazione.model.cdp.{CaFinalCdp, ValidatedFlowCdp}
import it.eng.au.ccgPubblicazione.model.request.RequestPdr
import it.eng.au.ccgPubblicazione.schema.cdp.CaFinalCdpSchema
import it.eng.au.ccgPubblicazione.utility.Constants._
import it.eng.au.ccgPubblicazione.utility.Environment

class CdpFinSessionTest extends EnvironmentSparkTest{

  def testRunPdrUdd(): Unit = {
    val spark = Environment.spark

    import spark.implicits._

    val consumption = List(
      CaFinalCdp(codice_pdr = "1", piva_udd = "123", prelievo_annuo_prev = "1")
      , CaFinalCdp(codice_pdr = "1", piva_udd = "123", prelievo_annuo_prev = "2")
      , CaFinalCdp(codice_pdr = "1", piva_udd = "123", prelievo_annuo_prev = "3")
      , CaFinalCdp(codice_pdr = "2", piva_udd = "456", prelievo_annuo_prev = "1")
      , CaFinalCdp(codice_pdr = "3", piva_udd = "456", prelievo_annuo_prev = "1")
      , CaFinalCdp(codice_pdr = "4", piva_udd = "456", prelievo_annuo_prev = "1")
    ).toDF
      .selectExpr(CaFinalCdpSchema.getValues: _*)

    val validation = List(
      ValidatedFlowCdp(pdr = "1")
      , ValidatedFlowCdp(pdr = "2")
      , ValidatedFlowCdp(pdr = "3")
      , ValidatedFlowCdp(pdr = "4")
    ).toDF

    val request = List(
      RequestPdr(N_ID_RICHIESTA = "1", T_PIVA = "123", T_RUOLO = UDD, T_CODICE_PDR = "1")
      , RequestPdr(N_ID_RICHIESTA = "2", T_PIVA = "456", T_RUOLO = UDD, T_CODICE_PDR = "2")
      , RequestPdr(N_ID_RICHIESTA = "2", T_PIVA = "456", T_RUOLO = UDD, T_CODICE_PDR = "3")
      , RequestPdr(N_ID_RICHIESTA = "3", T_PIVA = "456", T_RUOLO = UDD, T_CODICE_PDR = "3")
      , RequestPdr(N_ID_RICHIESTA = "4", T_PIVA = "456", T_RUOLO = UDD, T_CODICE_PDR = "1")
      , RequestPdr(N_ID_RICHIESTA = "4", T_PIVA = "456", T_RUOLO = UDD, T_CODICE_PDR = "3")
      , RequestPdr(N_ID_RICHIESTA = "5", T_PIVA = "789", T_RUOLO = UDD, T_CODICE_PDR = "10")
      , RequestPdr(N_ID_RICHIESTA = "6", T_PIVA = "111", T_RUOLO = UDD, T_CODICE_PDR = "1", B_AMMISSIBILITA = AMMISSIBILITA_NO_0, T_COD_CAUSALE = "1", T_TIPO_AMM = AMMISSIBILITA_PDR)
      , RequestPdr(N_ID_RICHIESTA = "7", T_PIVA = "111", T_RUOLO = UDD, T_CODICE_PDR = "1", B_AMMISSIBILITA = AMMISSIBILITA_NO_0, T_COD_CAUSALE = "2", T_TIPO_AMM = AMMISSIBILITA_FILE)
      , RequestPdr(N_ID_RICHIESTA = "8", T_PIVA = "111", T_RUOLO = UDD, T_CODICE_PDR = "1", B_AMMISSIBILITA = AMMISSIBILITA_NO_0, T_COD_CAUSALE = "4", T_TIPO_AMM = AMMISSIBILITA_FILE)
      , RequestPdr(N_ID_RICHIESTA = "9", T_PIVA = "999", T_RUOLO = UDD, T_CODICE_PDR = "999", T_COD_CAUSALE = "0")
    ).toDF

    CdpFinSession.runRunnableAggregator(consumption, validation, request, UDD, PDR)

  }
}
