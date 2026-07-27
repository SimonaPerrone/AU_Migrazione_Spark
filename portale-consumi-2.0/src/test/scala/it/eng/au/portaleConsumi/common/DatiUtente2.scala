package it.eng.au.portaleConsumi.common

import it.eng.au.portaleConsumi.model.hive.misuregas.FornitureProcessiGasModel
import it.eng.au.portaleConsumi.model.hive.rcugas._
import it.eng.au.portaleConsumi.model.hive.switch_gas.PrtSwgPModel
import it.eng.au.portaleConsumi.model.hive.tdg.TdgVulnPModel

import java.sql.{Date, Timestamp}

object DatiUtente2 {

  val clientefinale: Seq[RcugasClientefinalePModel] = Seq(RcugasClientefinalePModel(
    n_id_cliente = "191216000028455273",
    t_codice_fiscale = "PIVAHIDDEN",
    t_partita_iva = "PIVAHIDDEN",
    t_nome = null,
    t_cognome = null,
    t_ragione_sociale = "HIDDEN FRATELLI SRL",
    t_note = "Nuovo ClienteFinale",
    t_dettaglio_cf = null,
    t_dettaglio_piva = null,
    t_sede_legale = null,
    d_aggiornamento = Timestamp.valueOf("2019-12-16 09:16:38.0"),
    n_id_traccia = "191216001200513289",
    n_id_s_prec = null,
    t_dettaglio_anacli = null,
    d_data_rif = Timestamp.valueOf("2019-12-13 00:00:00.0"),
    t_codice_ateco = null,
    b_cf_straniero = "0",
    b_persona_fisica = "N",
    t_telefono = "038270040",
    t_email = "FITTIZIO@HIDDEN"
  ))

  val residenze: Seq[RcugasResidenzaPModel] = Seq(RcugasResidenzaPModel())

  val pdr: Seq[RcugasPdrPModel] = Seq(RcugasPdrPModel(
    n_id_pdr = "150606000009716624",
    t_codice_pdr = "02800000011745",
    t_cod_tipo_pdr = "2",
    t_codice_istat = "018192",
    t_note = "AggiornaPDR",
    d_aggiornamento = Timestamp.valueOf("2018-08-20 15:15:52.0"),
    n_id_traccia = "180820000792586282",
    n_id_s_prec = null,
    n_id_indirizzo = "180820000005579970",
    d_data_rif = Timestamp.valueOf("2018-06-01 00:00:00.0"),
    t_disalimentabilita = "SI",
    t_accesso_ui = null
  ),
    RcugasPdrPModel(
      n_id_pdr = "150606000008998271",
      t_codice_pdr = "02800000011746",
      t_cod_tipo_pdr = "2",
      t_codice_istat = "018192",
      t_note = "AggiornaPDR",
      d_aggiornamento = Timestamp.valueOf("2018-08-20 11:57:08.0"),
      n_id_traccia = "180820000791128572",
      n_id_s_prec = null,
      n_id_indirizzo = "180820000005579970",
      d_data_rif = Timestamp.valueOf("2018-06-01 00:00:00.0"),
      t_disalimentabilita = "SI",
      t_accesso_ui = null
    )
  )

  val forniture: Seq[RcugasFornituraPModel] = Seq(RcugasFornituraPModel(
    n_id_fornitura = "191202000045205474",
    d_data_inizio = Timestamp.valueOf("2019-12-01 00:00:00.0"),
    d_data_fine = null,
    n_id_cliente = "191216000028455273",
    n_id_pdr = "150606000009716624",
    n_id_vend = "141128000000000042",
    b_tariffa_tm = null,
    t_codice_ateco = "49.41",
    n_lettura_attivazione = null,
    t_aliquota_iva = null,
    t_imposte = null,
    n_indirizzo_fornitura = "180820000005579970",
    n_indirizzo_recap = null,
    t_bonus_gas = null,
    d_data_inizio_bonus = null,
    d_data_fine_bonus = null,
    b_prestazioni_non_concluse = null,
    b_disalimentabilita = null,
    t_codice_contratto_vendita = "Premia2aGas1909",
    t_id_contratto_vend = null,
    d_data_stipula = Timestamp.valueOf("2019-10-28 00:00:00.0"),
    t_note = "Aggiorna Fornitura",
    d_aggiornamento = Timestamp.valueOf("2021-04-02 12:17:13.0"),
    n_id_traccia = "210402001477422331",
    n_id_s_prec = null,
    tipo_data_inizio = null,
    tipo_data_fine = null,
    d_data_rif = Timestamp.valueOf("2021-04-02 00:00:00.0"),
    t_tipo_fornitura = "M1",
    n_indirizzo_fatt = null
  ),
    RcugasFornituraPModel(
      n_id_fornitura = "191202000045192474",
      d_data_inizio = Timestamp.valueOf("2019-12-01 00:00:00.0"),
      d_data_fine = null,
      n_id_cliente = "191216000028455273",
      n_id_pdr = "150606000008998271",
      n_id_vend = "141128000000000042",
      b_tariffa_tm = null,
      t_codice_ateco = "49.41",
      n_lettura_attivazione = null,
      t_aliquota_iva = null,
      t_imposte = null,
      n_indirizzo_fornitura = "180820000005579970",
      n_indirizzo_recap = null,
      t_bonus_gas = null,
      d_data_inizio_bonus = null,
      d_data_fine_bonus = null,
      b_prestazioni_non_concluse = null,
      b_disalimentabilita = null,
      t_codice_contratto_vendita = "Premia2aGas1909",
      t_id_contratto_vend = null,
      d_data_stipula = Timestamp.valueOf("2019-10-28 00:00:00.0"),
      t_note = "Aggiorna Fornitura",
      d_aggiornamento = Timestamp.valueOf("2021-04-02 12:17:15.0"),
      n_id_traccia = "210402001477421573",
      n_id_s_prec = null,
      tipo_data_inizio = null,
      tipo_data_fine = null,
      d_data_rif = Timestamp.valueOf("2021-04-02 00:00:00.0"),
      t_tipo_fornitura = "M1",
      n_indirizzo_fatt = null
    )
  )

  val datiprelievo: Seq[RcugasPdrDatiprelievoPModel] = Seq(
    RcugasPdrDatiprelievoPModel(
      n_id_pdr_datiprelievo = "220810000195192856",
      n_id_pdr = "150606000009716624",
      t_anno = "2023",
      t_cod_profilo = "T2E3",
      n_prelievo_annuo = "1600",
      n_lettura_convertitore = null,
      t_cod_cat_uso = "T2",
      t_cod_classe_prelievo = "3",
      t_note = "Inserimento Nuovo prelievo",
      d_aggiornamento = Timestamp.valueOf("2022-08-10 13:02:03.0"),
      n_id_traccia = "220810002969395561",
      n_id_s_prec = null,
      d_data_rif = Timestamp.valueOf("2022-06-01 00:00:00.0"),
      t_anno_mese_rif = "202301",
      t_fattore_correz_climatica = null,
      t_trattamento_settlement = "Y"
    ),
    RcugasPdrDatiprelievoPModel(
      n_id_pdr_datiprelievo = "220810000195348244",
      n_id_pdr = "150606000008998271",
      t_anno = "2023",
      t_cod_profilo = "T2E3",
      n_prelievo_annuo = "1287",
      n_lettura_convertitore = null,
      t_cod_cat_uso = "T2",
      t_cod_classe_prelievo = "3",
      t_note = "Inserimento Nuovo prelievo",
      d_aggiornamento = Timestamp.valueOf("2022-08-10 13:28:38.0"),
      n_id_traccia = "220810002970796325",
      n_id_s_prec = null,
      d_data_rif = Timestamp.valueOf("2022-06-01 00:00:00.0"),
      t_anno_mese_rif = "202301",
      t_fattore_correz_climatica = null,
      t_trattamento_settlement = "Y"
    )
  )

  val misuratori: Seq[RcugasPdrMisuratorePModel] = Seq(
    RcugasPdrMisuratorePModel(
      n_id_pdr_misuratore = "201222000035412597",
      n_id_pdr = "150606000009716624",
      t_matricola_misuratore = "SMGR036118006809",
      t_tipo_misuratore = "02",
      t_telegestito = "NO",
      n_coeff_correzione = "1.01",
      t_classe_misuratore = "G6",
      t_access_misuratore = "1",
      n_num_cifre_misuratore = "5",
      t_anno_fabbric_misuratore = "2018",
      t_data_inst_misuratore = Date.valueOf("2019-02-11"),
      t_misuratore_integrato = "SI",
      t_presenza_convertitore = "NO",
      t_matricola_convertitore = null,
      n_num_cifre_convertitore = null,
      t_anno_fabbric_convertitore = null,
      t_data_inst_convertitore = null,
      n_lettura_convertitore = null,
      t_note = "AggiornaRCUGAS AGG_VARMIS",
      d_aggiornamento = Timestamp.valueOf("2021-06-17 05:06:14.0"),
      n_id_traccia = "210617002471623157",
      n_id_s_prec = null,
      d_data_rif = Timestamp.valueOf("2019-02-11 00:00:00.0")
    ),
    RcugasPdrMisuratorePModel(
      n_id_pdr_misuratore = "201222000034415369",
      n_id_pdr = "150606000008998271",
      t_matricola_misuratore = "SMGR036118006687",
      t_tipo_misuratore = "02",
      t_telegestito = "NO",
      n_coeff_correzione = "1.01",
      t_classe_misuratore = "G6",
      t_access_misuratore = "1",
      n_num_cifre_misuratore = "5",
      t_anno_fabbric_misuratore = "2018",
      t_data_inst_misuratore = Date.valueOf("2019-02-11"),
      t_misuratore_integrato = "SI",
      t_presenza_convertitore = "NO",
      t_matricola_convertitore = null,
      n_num_cifre_convertitore = null,
      t_anno_fabbric_convertitore = null,
      t_data_inst_convertitore = null,
      n_lettura_convertitore = null,
      t_note = "AggiornaRCUGAS AGG_VARMIS",
      d_aggiornamento = Timestamp.valueOf("2021-06-17 05:11:31.0"),
      n_id_traccia = "210617002471636451",
      n_id_s_prec = null,
      d_data_rif = Timestamp.valueOf("2019-02-11 00:00:00.0")
    ))

  val connessioni: Seq[RcugasConnessioniDistrPModel] = Seq(
    RcugasConnessioniDistrPModel(
      t_codice_pdr = "02800000011746",
      n_id_pdr = "150606000008998271",
      n_id_remi = "150605000000000725",
      d_data_inizio_conn = null,
      d_data_fine_conn = null,
      t_remi = "34465201",
      n_id_distr = "150601000000000266",
      d_data_inizio_gestecn = null,
      d_data_fine_gestecn = null,
      t_remi_rcu = "34465201",
      id_regione_climatica = "11",
      t_piva_distr = "06724610966"
    ),
    RcugasConnessioniDistrPModel(
      t_codice_pdr = "02800000011745",
      n_id_pdr = "150606000009716624",
      n_id_remi = "150605000000000725",
      d_data_inizio_conn = null,
      d_data_fine_conn = null,
      t_remi = "34465201",
      n_id_distr = "150601000000000266",
      d_data_inizio_gestecn = null,
      d_data_fine_gestecn = null,
      t_remi_rcu = "34465201",
      id_regione_climatica = "11",
      t_piva_distr = "06724610966"
    ))

  val processi: Seq[PrtSwgPModel] = Seq(
    PrtSwgPModel()
  )

  val indirizzi: Seq[RcugasIndirizziPModel] = Seq(
    RcugasIndirizziPModel(
      n_id = "180820000005579970",
      t_toponimo = "VIA",
      t_nomestrada = "OLONA",
      t_civico = "1",
      t_comune = "CORTEOLONA-GENZONE",
      t_comune_istat = "018192",
      t_provincia = "PV",
      t_nazione = "ITALIA",
      t_indirizzo_completo = "VIA OLONA 1 27014 CORTEOLONA-GENZONE PV ",
      t_presso = null,
      d_aggiornamento = Timestamp.valueOf("2018-08-20 11:57:08.0"),
      n_id_traccia = "180820000791128571",
      n_id_s_prec = null,
      d_data_rif = Timestamp.valueOf("2018-06-01 00:00:00.0"),
      t_cap = "27014"
    )
  )

  val offerte: Seq[RcugasCodiceOffertaPModel] = Seq(
    RcugasCodiceOffertaPModel(
      n_id_fornitura = null,
      t_codice_offerta = null,
      d_data_fine = null)
  )

  val vulnerabilita: Seq[TdgVulnPModel] = Seq(
    TdgVulnPModel(
      n_id_tdg_vuln = null,
      n_id_pdr = null,
      n_id_cliente = null,
      d_data_fine = null
    )
  )

  // Rappresentazione tabella finale Hive per Forniture Gas
  val fornituraProcessiGas: Seq[FornitureProcessiGasModel] = Seq(
    FornitureProcessiGasModel(
      hashcode = null,
      codice_fiscale = "PIVAHIDDEN",
      codice_pdr = "02800000011745",
      nome = null,
      cognome = null,
      p_iva = "PIVAHIDDEN",
      ragione_sociale = "HIDDEN FRATELLI SRL",
      cap = "27014",
      categoria_uso = "T2",
      civico = "1",
      classe_misuratore = "G6",
      codice_fornitura = "191202000045205474",
      coefficiente_conversione = "1.01",
      comune = "CORTEOLONA-GENZONE",
      data_inizio_fornitura = Timestamp.valueOf("2019-12-01 00:00:00.0"),
      data_fine_fornitura = null,
      data_aggiornamento = Timestamp.valueOf("2021-04-02 12:17:13.0"),
      matricola_misuratore = "SMGR036118006809",
      nazione = "ITALIA",
      nome_strada = "OLONA",
      p_iva_cc = "12883420155",
      provincia = "PV",
      ragione_sociale_cc = "A2A ENERGIA SPA",
      ragione_sociale_distributore = "2I RETE GAS S.P.A.",
      residente = null,
      tipo_fornitura = "M1",
      tipo_pdr = "2",
      toponimo_Indirizzo = "VIA",
      data_inizio_processo_gdm = null,
      data_fine_processo_gdm = null,
      data_di_decorrenza_gdm = Timestamp.valueOf("2019-02-11 00:00:00"),
      id_processo_gdm = "PRO001",
      in_corso_gdm = "false",
      note_gdm = "note",
      tipo_processo_gdm = "cambio_gdm",
      data_inizio_processo_switch = null,
      data_fine_processo_switch = null,
      data_di_decorrenza_switch = null,
      id_processo_switch = "PRO002",
      in_corso_switch = "false",
      note_switch = "note",
      tipo_processo_switch = "switch",
      codice_offerta = null,
      cliente_vulnerabile = "N",
      data_calcolo = "2020-01-01"
    )
    ,
    FornitureProcessiGasModel(
      hashcode = null,
      codice_fiscale = "PIVAHIDDEN",
      codice_pdr = "02800000011746",
      nome = null,
      cognome = null,
      p_iva = "PIVAHIDDEN",
      ragione_sociale = "HIDDEN FRATELLI SRL",
      cap = "27014",
      categoria_uso = "T2",
      civico = "1",
      classe_misuratore = "G6",
      codice_fornitura = "191202000045192474",
      coefficiente_conversione = "1.01",
      comune = "CORTEOLONA-GENZONE",
      data_inizio_fornitura = Timestamp.valueOf("2019-12-01 00:00:00.0"),
      data_fine_fornitura = null,
      data_aggiornamento = Timestamp.valueOf("2021-04-02 12:17:15.0"),
      matricola_misuratore = "SMGR036118006687",
      nazione = "ITALIA",
      nome_strada = "OLONA",
      p_iva_cc = "12883420155",
      provincia = "PV",
      ragione_sociale_cc = "A2A ENERGIA SPA",
      ragione_sociale_distributore = "2I RETE GAS S.P.A.",
      residente = null,
      tipo_fornitura = "M1",
      tipo_pdr = "2",
      toponimo_Indirizzo = "VIA",
      data_inizio_processo_gdm = null,
      data_fine_processo_gdm = null,
      data_di_decorrenza_gdm = Timestamp.valueOf("2019-02-11 00:00:00"),
      id_processo_gdm = "PRO001",
      in_corso_gdm = "false",
      note_gdm = "note",
      tipo_processo_gdm = "cambio_gdm",
      data_inizio_processo_switch = null,
      data_fine_processo_switch = null,
      data_di_decorrenza_switch = null,
      id_processo_switch = "PRO002",
      in_corso_switch = "false",
      note_switch = "note",
      tipo_processo_switch = "switch",
      codice_offerta = null,
      cliente_vulnerabile = "N",
      data_calcolo = "2020-01-01"
    )
  )

}
