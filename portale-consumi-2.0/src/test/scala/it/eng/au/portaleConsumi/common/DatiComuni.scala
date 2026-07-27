package it.eng.au.portaleConsumi.common

import it.eng.au.portaleConsumi.model.hive.rcu.VRcuAziendaPModel
import it.eng.au.portaleConsumi.model.hive.rcugas.{RcugasVenditorePModel, VRcugasDistributorePModel}

import java.sql.Timestamp

/*
Dati venditori, aziende, infrastrutture
 */
object DatiComuni {

  val distributori: Seq[VRcugasDistributorePModel] = Seq(VRcugasDistributorePModel(
    n_id_distributore = "n_id_distributore1",
    n_id_azienda = "n_id_azienda1",
    n_id_utente = "n_id_utente_Distributore1",
    t_piva = "t_piva_Distributore1",
    t_rag_soc = "t_rag_soc_Distributore1",
    d_data_inizio = null,
    d_data_fine = null
  ),
    VRcugasDistributorePModel(
      n_id_distributore= "150601000000000266",
      n_id_azienda= "150601000000007303",
      t_codice_esercente= "6149",
      n_id_utente= "640",
      t_codice_aeeg= "22668",
      t_piva= "06724610966",
      t_rag_soc= "2I RETE GAS S.P.A.",
      d_data_inizio= null,
      d_data_fine= null
    )
  )

  val venditori: Seq[RcugasVenditorePModel] = Seq(
    RcugasVenditorePModel(
      n_id_venditore = "n_id_venditore1",
      n_id_azienda = "n_id_azienda1",
      d_data_inizio = Timestamp.valueOf("2022-12-01 00:00:00.0"),
      d_data_fine = null,
      d_aggiornamento = Timestamp.valueOf("2022-12-01 00:00:00.0")
    ),
    RcugasVenditorePModel(
      n_id_venditore = "141128000000000042",
      n_id_azienda = "1327",
      t_codice_map = "217",
      d_data_inizio = null,
      d_data_fine = null,
      t_note = "AggiornaCODICE_MAP",
      d_aggiornamento = Timestamp.valueOf("2016-01-08 20:00:01.0"),
      n_id_traccia = "160108000450361012",
      n_id_s_prec = null,
      d_data_rif = null
    )
  )

  val aziende: Seq[VRcuAziendaPModel] = Seq(VRcuAziendaPModel(
    n_id_azienda = "n_id_azienda1",
    t_piva = "t_piva_Azienda1",
    t_cf = "t_cf_Azienda1",
    t_rag_soc = "t_rag_soc_Azienda1"
  ),
    VRcuAziendaPModel(
      n_id_azienda= "1327",
      n_id_utente= "339",
      t_codice_aeeg= "294",
      t_piva= "12883420155",
      t_cf= "12883420155",
      t_rag_soc= "A2A ENERGIA SPA",
      n_id_sedelegale= null,
      t_contatto= null,
      t_email= null,
      t_pec= null,
      d_aggiornamento= Timestamp.valueOf("2021-04-28 21:00:02.0"),
      n_id_traccia= "210428002256061842",
      n_id_s_prec= null,
      t_ruoli= "MT,UD,VD",
      t_desc_ruoli= "EMT,EVD,Unita' di Dispacciamento"
    ))
}
