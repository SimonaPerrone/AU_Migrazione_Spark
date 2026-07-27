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
object DatiUtente4 {

  // Rappresentazione tabella finale Hive per Forniture Gas
  val fornituraProcessiGas: Seq[FornitureProcessiGasModel] = Seq(
    //elemento 1   codice_fiscale = t_codice_fiscale_Utente1,codice pdr = pdr t_codice_pdr1
    FornitureProcessiGasModel(
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
  ),
    //elemento 2   codice_fiscale = t_codice_fiscale_Utente1,codice pdr = t_codice_pd2
    FornitureProcessiGasModel(
      hashcode = null,
      codice_fiscale = "t_codice_fiscale_Utente1",
      codice_pdr = "t_codice_pd2",
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
    ),

    //elemento 3   codice_fiscale = t_codice_fiscale_Utente2,codice pdr = t_codice_pd3
    FornitureProcessiGasModel(
      hashcode = null,
      codice_fiscale = "t_codice_fiscale_Utente2",
      codice_pdr = "t_codice_pdr3",
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
    )
  )

  // Rappresentazione collezione MongoDB per FornitureGas
  val fornituraGasMongodb: Seq[FornitureGasMongoDbModel] = Seq(
    //documento1
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
        )
      )
    ),
    //docuemnto2
    FornitureGasMongoDbModel(
      _id = "t_codice_fiscale_Utente1_t_codice_pdr2",
      anagrafica = Anagrafica(
        nome = "Utente1",
        cognome = "Uno",
        p_iva = "",
        ragione_sociale = ""
      ),
      codice_fiscale = "t_codice_fiscale_Utente1",
      id = "t_codice_fiscale_Utente1_t_codice_pdr2",
      pdr = Array(
        Pdr(
          codice_pdr = "t_codice_pdr2",
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
        )
      )
    ),
    //documento3
    FornitureGasMongoDbModel(
      _id = "t_codice_fiscale_Utente2_t_codice_pdr3",
      anagrafica = Anagrafica(
        nome = "Utente1",
        cognome = "Uno",
        p_iva = "",
        ragione_sociale = ""
      ),
      codice_fiscale = "t_codice_fiscale_Utente2",
      id = "t_codice_fiscale_Utente2_t_codice_pdr3",
      pdr = Array(
        Pdr(
          codice_pdr = "t_codice_pdr3",
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
        )
      )
    )
  )

}
