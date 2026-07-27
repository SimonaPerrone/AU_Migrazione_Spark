package it.eng.au.portaleConsumi.common

import it.eng.au.portaleConsumi.model.hive.misuregas.FornitureProcessiGasModel
import it.eng.au.portaleConsumi.model.hive.rcugas._
import it.eng.au.portaleConsumi.model.hive.switch_gas.PrtSwgPModel
import it.eng.au.portaleConsumi.model.hive.tdg.TdgVulnPModel
import it.eng.au.portaleConsumi.model.mongodb.forniture._

import java.sql.{Date, Timestamp}


/*
Dati utente con una singola fornitura attiva
 */
object DatiUtente1 {

  val clientefinale: Seq[RcugasClientefinalePModel] = Seq(RcugasClientefinalePModel(
    n_id_cliente = "n_id_cliente_Utente1",
    t_codice_fiscale = "t_codice_fiscale_Utente1",
    t_partita_iva = null,
    t_nome = "Utente1",
    t_cognome = "Uno",
    d_aggiornamento = Timestamp.valueOf("2021-07-22 00:00:00.0"),
    d_data_rif = Timestamp.valueOf("2021-07-22 00:00:00.0")
  ))

  val residenze: Seq[RcugasResidenzaPModel] = Seq(RcugasResidenzaPModel(
    n_id_residenza = "n_id_residenza_Utente1",
    n_id_fornitura = "n_id_fornitura1",
    t_residenza = "SI",
    d_data_inizio = Timestamp.valueOf("2021-07-22 00:00:00.0"),
    d_data_fine = null,
    d_aggiornamento = Timestamp.valueOf("2021-07-22 00:00:00.0"),
    n_id_traccia = "n_id_traccia_Utente1",
    n_id_s_prec = null,
    d_data_rif = Timestamp.valueOf("2021-07-22 00:00:00.0")
  ))

  val pdr: Seq[RcugasPdrPModel] = Seq(RcugasPdrPModel(
    n_id_pdr = "n_id_pdr1",
    t_codice_pdr = "t_codice_pdr1",
    t_cod_tipo_pdr = "0",
    t_codice_istat = "048017",
    t_note = "AggiornaPDR",
    d_aggiornamento = Timestamp.valueOf("2021-07-22 00:00:00.0"),
    n_id_traccia = "123123",
    n_id_s_prec = "",
    n_id_indirizzo = "n_indirizzo1",
    d_data_rif = Timestamp.valueOf("2021-07-22 00:00:00.0"),
    t_disalimentabilita = "SI",
    t_accesso_ui = null
  ))

  val forniture: Seq[RcugasFornituraPModel] = Seq(RcugasFornituraPModel(
    n_id_fornitura = "n_id_fornitura1",
    d_data_inizio = Timestamp.valueOf("2021-07-22 00:00:00.0"),
    d_data_fine = null,
    n_id_cliente = "n_id_cliente_Utente1",
    n_id_pdr = "n_id_pdr1",
    n_id_vend = "n_id_venditore1",
    d_data_stipula = Timestamp.valueOf("2021-07-22 00:00:00.0"),
    d_aggiornamento = Timestamp.valueOf("2021-07-22 00:00:00.0"),
    d_data_rif = Timestamp.valueOf("2021-07-22 00:00:00.0"),
    t_tipo_fornitura = "t_tipo_fornitura1",
    n_indirizzo_fornitura = "n_indirizzo1"
  ))

  val datiprelievo: Seq[RcugasPdrDatiprelievoPModel] = Seq(RcugasPdrDatiprelievoPModel(
    n_id_pdr_datiprelievo = "n_id_pdr_datiprelievo_Pdr1",
    n_id_pdr = "n_id_pdr1",
    t_anno = "2023",
    t_cod_cat_uso = "t_cod_cat_uso_pdr1"
  ))

  val misuratori: Seq[RcugasPdrMisuratorePModel] = Seq(RcugasPdrMisuratorePModel(
    n_id_pdr_misuratore = "n_id_pdr_misuratore1",
    n_id_pdr = "n_id_pdr1",
    t_matricola_misuratore = "t_matricola_misuratore1",
    n_coeff_correzione = "0.95",
    t_classe_misuratore = "t_classe_misuratore1",
    t_data_inst_misuratore = Date.valueOf("2022-11-01")
  ))

  val connessioni: Seq[RcugasConnessioniDistrPModel] = Seq(RcugasConnessioniDistrPModel(
    t_codice_pdr = "t_codice_pdr1",
    d_data_inizio_conn = Timestamp.valueOf("2020-01-01 00:00:00.0"),
    d_data_fine_conn = null,
    n_id_distr = "n_id_distributore1"
  ))

  val processi: Seq[PrtSwgPModel] = Seq(
    PrtSwgPModel(
      n_id_swg = "n_id_swg1",
      n_id_pratica = "n_id_pratica1",
      t_codice_pdr = "t_codice_pdr1",
      d_data_decorrenza = Timestamp.valueOf("2023-01-01 00:00:00.0"),
      t_stato = "E",
      d_data_richiesta = Timestamp.valueOf("2022-12-01 00:00:00.0")
    ))

  val indirizzi: Seq[RcugasIndirizziPModel] = Seq(
    RcugasIndirizziPModel(
      n_id = "n_indirizzo1",
      t_toponimo = "t_toponimo1",
      t_nomestrada = "t_nomestrada1",
      t_civico = "t_civico1",
      t_comune = "t_comune1",
      t_provincia = "t_provincia1",
      t_nazione = "t_nazione1",
      d_aggiornamento = Timestamp.valueOf("2022-12-01 00:00:00.0"),
      t_cap = "t_cap1"
    )
  )

  val offerte: Seq[RcugasCodiceOffertaPModel] = Seq(
    RcugasCodiceOffertaPModel(
      n_id_fornitura = "n_id_fornitura1",
      t_codice_offerta = "codice_offerta_1",
      d_data_fine = null)
  )

  val vulnerabilita: Seq[TdgVulnPModel] = Seq(
    TdgVulnPModel(
      n_id_tdg_vuln = "n_id_vuln",
      n_id_pdr = "n_id_pdr1",
      n_id_cliente = "n_id_cliente_Utente1",
      d_data_fine = null
    )
  )

  // Rappresentazione tabella finale Hive per Forniture Gas
  val fornituraProcessiGas: Seq[FornitureProcessiGasModel] = Seq(FornitureProcessiGasModel(
    hashcode = null,
    codice_fiscale = "t_codice_fiscale_Utente1",
    codice_pdr = "t_codice_pdr1",
    nome = "Utente1",
    cognome = "Uno",
    p_iva = null,
    ragione_sociale = null,
    cap = "t_cap1",
    categoria_uso = "t_cod_cat_uso_pdr1",
    civico = "t_civico1",
    classe_misuratore = "t_classe_misuratore1",
    codice_fornitura = "n_id_fornitura1",
    coefficiente_conversione = "0.95",
    comune = "t_comune1",
    data_inizio_fornitura = Timestamp.valueOf("2021-07-22 00:00:00.0"),
    data_fine_fornitura = null,
    data_aggiornamento = Timestamp.valueOf("2021-07-22 00:00:00.0"),
    matricola_misuratore = "t_matricola_misuratore1",
    nazione = "t_nazione1",
    nome_strada = "t_nomestrada1",
    p_iva_cc = "t_piva_Azienda1",
    provincia = "t_provincia1",
    ragione_sociale_cc = "t_rag_soc_Azienda1",
    ragione_sociale_distributore = "t_rag_soc_Distributore1",
    residente = "SI",
    tipo_fornitura = "t_tipo_fornitura1",
    tipo_pdr = "0",
    toponimo_Indirizzo = "t_toponimo1",
    data_inizio_processo_gdm = null,
    data_fine_processo_gdm = null,
    data_di_decorrenza_gdm = Timestamp.valueOf("2022-11-01 00:00:00.0"),
    id_processo_gdm = "PRO001",
    in_corso_gdm = "false",
    note_gdm = "note",
    tipo_processo_gdm = "cambio_gdm",
    data_inizio_processo_switch = null,
    data_fine_processo_switch = null,
    data_di_decorrenza_switch = Timestamp.valueOf("2023-01-01 00:00:00.0"),
    id_processo_switch = "PRO002",
    in_corso_switch = "true",
    note_switch = "note",
    tipo_processo_switch = "switch",
    codice_offerta = "codice_offerta_1",
    cliente_vulnerabile = "Y",
    data_calcolo = "2022-12-31"
  ))

  // Rappresentazione collezione MongoDB per FornitureGas
  val fornituraGasMongodb: FornitureGasMongoDbModel =
    FornitureGasMongoDbModel(
      _id = "t_codice_fiscale_Utente1_t_codice_pdr1",
      anagrafica = Anagrafica(
        nome = "Utente1",
        cognome = "Uno",
        p_iva = "",
        ragione_sociale = ""
      ),
      codice_fiscale = "t_codice_fiscale_Utente1",
      id = "t_codice_fiscale_Utente1_t_codice_pdr1",
      pdr = Array(
        Pdr(
          codice_pdr = "t_codice_pdr1",
          forniture = Array(
            Fornitura(
              cap = "t_cap1",
              civico = "t_civico1",
              categoria_uso = "t_cod_cat_uso_pdr1",
              classe_misuratore = "t_classe_misuratore1",
              codice_fornitura = "n_id_fornitura1",
              coefficiente_conversione = "0.95",
              comune = "t_comune1",
              data_inizio_fornitura = "20210722",
              data_fine_fornitura = "",
              matricola_misuratore = "t_matricola_misuratore1",
              nazione = "t_nazione1",
              nome_strada = "t_nomestrada1",
              p_iva_cc = "t_piva_Azienda1",
              provincia = "t_provincia1",
              ragione_sociale_cc = "t_rag_soc_Azienda1",
              ragione_sociale_distributore = "t_rag_soc_Distributore1",
              residente = "SI",
              tipo_fornitura = "t_tipo_fornitura1",
              tipo_pdr = "0",
              toponimo_Indirizzo = "t_toponimo1",
              codice_offerta = "codice_offerta_1",
              cliente_vulnerabile = "Y"
            )),
          processi = Array(
            Processo(
              id_processo = "PRO001",
              data_inizio_processo = "",
              data_fine_processo = "",
              data_di_decorrenza = "20221101",
              in_corso = "false",
              note = "note",
              tipo_processo = "cambio_gdm"),
            Processo(
              id_processo = "PRO002",
              data_inizio_processo = "",
              data_fine_processo = "",
              data_di_decorrenza = "20230101",
              in_corso = "true",
              note = "note",
              tipo_processo = "switch"))
        )))

}
