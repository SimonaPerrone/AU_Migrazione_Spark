package it.eng.au.portale_consumi_ee.calcolo.forniture_ele_2

import it.eng.au.portale_consumi_ee.EnvironmentSparkTest
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.rcu.{RcuPodMisurePModel, RcuPodPModel, RcuPodTecnPModel}
import it.eng.au.portale_consumi_ee.model.rcus.RcusPodtecnPModel
import it.eng.au.portale_consumi_ee.model.swtch.SwtchPrtSePModel
import it.eng.au.portale_consumi_ee.model.userappl.UserapplT001AppPrtPratichePModel
import it.eng.au.portale_consumi_ee.trasformation.{forniture_ele_1_trasformations, forniture_ele_2_trasformations}

class calcolo_gdm  extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

   //test calcolo_fornitura
  def testCalcoloFornitura(): Unit = {

    val dsRcuPodMisureP = Seq(
      RcuPodMisurePModel(
        n_id_pod = "150205000036543818",
        d_anno_mese = "2017-01-01 00:00:00.0",
        t_trattamento = "F",
        t_trattamento_succ = "O",
        n_consumo_annuo = "1389",
        t_nota = null,
        d_aggiornamento = "2017-12-01 00:00:00.0",
        n_id_traccia = "150428000234195525",
        n_id_s_prec = null
      )
    ).toDS()

    val dsRcuPodP = Seq(
      RcuPodPModel(
        n_id_pod = "150205000036543818",
        t_codice_pod = "IT444E12310795",
        t_area_rif = "NORD",
        b_rich_indennizzo = null,
        b_rich_prest_distr = null,
        n_id_indirizzo = null,
        t_nota = null,
        d_aggiornamento = "2015-02-05 17:19:20.0",
        n_id_traccia = "150205000234122596",
        n_id_s_prec = null,
        n_id_ind_forn = null
      )
    ).toDS()

    val dsRcuPodTecnP = Seq(
      RcuPodTecnPModel(
        n_id_pod = "150205000036543818",
        n_potenza_disponibile = "212121213.555",
        n_potenza_impegnata = "111111112.444",
        n_tensione = "12",
        t_tipo_misuratore = "G",
        n_k_trasformazione = null,
        d_inst_misuratore = null,
        d_rimoz_misuratore = null,
        t_nota = null,
        d_aggiornamento = "2018-02-19 00:00:00.0",
        n_id_traccia = "180219000234257430",
        n_id_s_prec = null,
        n_num_cifre_ea = "222",
        n_num_cifre_er = "111",
        n_k_trasfor_att = "111456111222.333",
        n_k_trasfor_rea = "111456111222.333",
        n_k_trasfor_pot = "111456111222.333",
        t_mat_misuratore_att = "ABC",
        t_mat_misuratore_rea = "ggg",
        t_mat_misuratore_pot = "ASD",
        d_inst_misurator_att = "2017-02-14 00:00:00.0",
        d_inst_misurator_rea = "2017-02-14 00:00:00.0",
        d_inst_misurator_pot = "2017-02-14 00:00:00.0",
        n_num_cifre_att = "333",
        n_num_cifre_rea = "333",
        n_num_cifre_pot = "333",
        b_presenza_mis = "SI",
        b_gest_forfait = "NO",
        t_tipo_pod = "03",
        d_fine_tipo_pod = "2017-01-01 00:00:00.0",
        d_oper_misurator_att = "2018-08-30 00:00:00.0",
        d_oper_misurator_rea = null,
        d_oper_misurator_pot = null,
        t_motivazione = null
      )
    ).toDS()

    val dsRcusPodTecnP = Seq(RcusPodtecnPModel()).toDS()



    val switch = forniture_ele_2_trasformations.calcolo_gdm(dsRcuPodMisureP,dsRcuPodP,dsRcuPodTecnP,dsRcusPodTecnP)

    switch.show()
  }

}
