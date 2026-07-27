package it.eng.au.portale_consumi_ee.calcolo.forniture_ele_7

import it.eng.au.portale_consumi_ee.EnvironmentSparkTest
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.mongodbs.{FornitureInfoModel, FornitureModel, GdmModel, RcuPodDistrModel, SwitchModel, fasceModel, podModel}
import it.eng.au.portale_consumi_ee.model.rcu.{RcuAziendaPModel, RcuClienteFinalePModel, RcuFasceMisuratore2gPModel, RcuIndirizzoPModel, RcuMisuratore2gPModel, RcuPodPModel, rcuCodiceOffertaPModel}
import it.eng.au.portale_consumi_ee.model.tde.tdeVulnPModel
import it.eng.au.portale_consumi_ee.trasformation.{forniture_ele_4_trasformations, forniture_ele_7_trasformations}
import org.apache.spark.sql.Dataset

class calcolo_mongodbs extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

   //test Pod
  def testCalcoloPods(): Unit = {

    val dsFornitureInfo = Seq(
      FornitureInfoModel(
        n_id_fornitura = "220126000036018419",
        n_id_pod = "150205000036544212",
        n_id_cliente = "140924000029396904",
        d_inizio_titolarita = 20220301L,
        d_fine_titolarita = 20250424L,
        d_inizio_titolarita_str = "20220126",
        d_fine_titolarita_str = null,
        n_id_fornitore = "1415",
        t_tipo_mercato = "L",
        n_id_indirizzo = "130114000000001476",
        n_id_ind_forn = "130114000000001336",
        codice_pod = "IT444E12310909",
        t_residente = "Y",
        t_tariffa_distr = null,
        t_piva = "03756540286",
        t_rag_soc = "TEST_PDC_2xx",
        t_servizio_tutela_sii = "MT"
      )
    ).toDS()

    val dsSwitch = Seq(SwitchModel()).toDS()
    val dsGdm = Seq(
      GdmModel(
        n_id_pod = "150205000036544212",
        codice_pod = "IT444E12310909",
        n_potenza_disponibile = "11124.04",
        n_potenza_impegnata = "11124.01",
        n_tensione = "10",
        t_tipo_misuratore = "O",
        d_oper_misurator_att = 20211003L,
        d_oper_misurator_att_str = "2021-10-03 14:39:52.0",
        cambio_gdm = null,
        data_cambio_gdm = 19700101L,
        data_cambio_gdm_str = null,
        trattamento = null,
        stato_misuratore_2g = null,
        t_mat_misuratore_att = "PARG8DS6",
        d_inst_misurator_att = 20151110L,
        anno_start_misure_orarie = 2022,
        mese_start_misure_orarie = 11
      )
    ).toDS()
    val dsForniture = Seq(
      FornitureModel(
        n_id_fornitura = "220126000036018419",
        inizio = 20220301L,
        fine = 20250424L,
        d_inizio_str = "20220126",
        d_fine_str = null,
        codice_pod = "IT444E12310909",
        attivo = "1",
        n_id_pod = "150205000036544212",
        n_id_fornitore = "1415",
        t_tipo_mercato = "L",
        n_id_cliente = "140924000029396904",
        n_id_indirizzo = "130114000000001476",
        n_id_ind_forn = "130114000000001336",
        t_servizio_tutela_sii = "MT"
      )
    ).toDS()

    val dsRcuIndirizzoP =Seq(
      RcuIndirizzoPModel(
        n_id = "130114000000001476",
        t_toponimo = "Via",
        t_nomestrada = "UBICAZIONE",
        t_civico = "55",
        t_comune = "Bari",
        t_comune_istat = "072006",
        t_cap = "70124",
        t_provincia = "BA",
        t_nazione = "ITALIA",
        t_indirizzo_completo = "Via",
        t_nota = null
      )
    ).toDS()

    val dsRcuAziendaP = Seq(
      RcuAziendaPModel(
        n_id_azienda = "1415",
        n_id_utente = "428",
        t_codice_aeeg = null,
        t_piva = "03756540286",
        t_cf = "03756540286",
        t_rag_soc = "TEST_PDC_2xx",
        n_id_sedelegale = null,
        t_contatto = null,
        t_email = null,
        t_pec = null,
        d_aggiornamento = "2021-02-09 15:52:51.0",
        n_id_traccia = "210209000234397652",
        n_id_s_prec = null
      )
    ).toDS()

    val rcuPodDistr = forniture_ele_7_trasformations.calcolo_pod(dsFornitureInfo,dsSwitch,dsGdm,dsForniture,dsRcuIndirizzoP,dsRcuAziendaP)

    rcuPodDistr.show(false)
  }

  //test calcolo fornitura_elettrica
  def testCalcoloFornituraElettrica(): Unit = {

    val dsRcuClienteFinaleP = Seq(RcuClienteFinalePModel()).toDS()
    val dsPod = Seq(podModel()).toDS()
    val dsRcuPodDistr = Seq(RcuPodDistrModel()).toDS()
    val dsfasce = Seq(fasceModel()).toDS()
    val dsForniture = Seq(FornitureModel()).toDS()
    val dsTdeVulnP = Seq(tdeVulnPModel()).toDS()
    val dsRcuPodP = Seq(RcuPodPModel()).toDS()
    val dsRcuCodiceOffertaP = Seq(rcuCodiceOffertaPModel()).toDS()


    val rcuPodDistr = forniture_ele_7_trasformations.calcolo_forniture_elettriche(
      dsRcuClienteFinaleP,dsPod,dsRcuPodDistr,dsfasce,dsForniture,dsTdeVulnP,dsRcuPodP,dsRcuCodiceOffertaP
      )

    rcuPodDistr.show()
  }

}
