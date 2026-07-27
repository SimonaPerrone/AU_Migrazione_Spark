package it.eng.au.portale_consumi_ee.calcolo.forniture_ele_4

import it.eng.au.portale_consumi_ee.EnvironmentSparkTest
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.rcu.{RcuAziendaPModel, RcuFasceMisuratore2gPModel, RcuMisuratore2gPModel, RcuPodDistrPModel}
import it.eng.au.portale_consumi_ee.trasformation.{forniture_ele_3_trasformations, forniture_ele_4_trasformations}

class calcolo_fasce extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

   //test calcolo_fornitura
  def testCalcoloFornitura(): Unit = {

    val dsRcuFasceMisuratore2gP = Seq(
      RcuFasceMisuratore2gPModel("190401000000000641", "501", "1", null, "3", "28", "2", "32", "1", "76", "2", "92", "3", "96", null, null, null, null, null, null, null, null, null, null, null, "2019-04-01 14:41:05.0", "190401000234264346", null, null),
      RcuFasceMisuratore2gPModel("190401000000000642", "501", "2", null, "3", "28", "2", "32", "1", "76", "2", "92", "3", "96", null, null, null, null, null, null, null, null, null, null, null, "2019-04-01 14:41:05.0", "190401000234264347", null, null),
      RcuFasceMisuratore2gPModel("190401000000000643", "501", "3", null, "3", "28", "2", "32", "1", "76", "2", "92", "3", "96", null, null, null, null, null, null, null, null, null, null, null, "2019-04-01 14:41:05.0", "190401000234264348", null, null),
      RcuFasceMisuratore2gPModel("190401000000000644", "501", "4", null, "3", "28", "2", "32", "1", "76", "2", "92", "3", "96", null, null, null, null, null, null, null, null, null, null, null, "2019-04-01 14:41:05.0", "190401000234264349", null, null),
      RcuFasceMisuratore2gPModel("190401000000000645", "501", "5", null, "3", "28", "2", "32", "1", "76", "2", "92", "3", "96", null, null, null, null, null, null, null, null, null, null, null, "2019-04-01 14:41:05.0", "190401000234264350", null, null),
      RcuFasceMisuratore2gPModel("190401000000000646", "501", "6", null, "3", "28", "2", "32", "1", "76", "2", "92", "3", "96", null, null, null, null, null, null, null, null, null, null, null, "2019-04-01 14:41:05.0", "190401000234264351", null, null),
      RcuFasceMisuratore2gPModel("190401000000000647", "501", "7", null, "3", "28", "2", "32", "1", "76", "2", "92", "3", "96", null, null, null, null, null, null, null, null, null, null, null, "2019-04-01 14:41:05.0", "190401000234264352", null, null),
      RcuFasceMisuratore2gPModel("190401000000000648", "501", "8", null, "3", "28", "2", "32", "1", "76", "2", "92", "3", "96", null, null, null, null, null, null, null, null, null, null, null, "2019-04-01 14:41:05.0", "190401000234264353", null, null)
    ).toDS()

    val dsRcuMisuratore2gP = Seq(
      RcuMisuratore2gPModel(
        n_id_misuratore_2g = "501",
        n_id_pod = "150205000036543817",
        b_vis_fasce = "N",
        b_vis_venditore = "N",
        b_vis_telefonov = "S",
        b_vis_datainicontr = "N",
        b_vis_datainiziofreezing = "S",
        b_vis_messaggicliente = "S",
        b_vis_codcli = "S",
        t_codcli = "1237487454474547",
        t_venditore = "PEPPINOVENDITORE",
        t_telefonov = "3",
        d_data_inicontr = null,
        d_data_iniziofreezing = "2011-01-01 00:00:00.0",
        t_messaggio_cliente_1 = null,
        t_messaggio_cliente_2 = null,
        t_messaggio_cliente_3 = null,
        t_messaggio_cliente_4 = null,
        t_messaggio_cliente_5 = null,
        n_num_fasce = "3",
        d_inizio_validita = "2019-12-31 00:00:00.0",
        d_fine_validita = null,
        t_nota = null,
        d_aggiornamento = "2019-04-01 14:41:05.0",
        n_id_traccia = "190401000234264345",
        n_id_s_prec = null,
        d_data_rif = null,
        t_tipo_configurazione = "C"
      )
    ).toDS()


    val rcuPodDistr = forniture_ele_4_trasformations.calcolo_fasce(dsRcuFasceMisuratore2gP,dsRcuMisuratore2gP)

    rcuPodDistr.show(false)
  }

}
