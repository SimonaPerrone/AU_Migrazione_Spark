package it.eng.au.portale_consumi_ee.schema.mongodbs

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object podSchema extends SchemaEnum{
  val
        n_id_pod,
        t_codice_pod,
        id_indirizzo,
        n_id_fornitura,
        n_id_cliente,
        d_inizio_titolarita,
        d_inizio_titolarita_str,
        d_fine_titolarita,
        d_fine_titolarita_str,
        tipo_mercato,
        n_id_fornitore,
        t_residente,
        tariffa,
        t_toponimo,
        t_nomestrada,
        t_civico,
        t_comune,
        t_cap,
        t_provincia,
        t_nazione,
        potenza_disponibile,
        potenza_impegnata,
        tensione,
        tipo_misuratore,
        cambio_gdm,
        data_cambio_gdm,
        data_cambio_gdm_str,
        d_inst_misurator_att,
        stato_misuratore_2g,
        trattamento,
        data_switch,
        switching_in_corso,
        d_oper_misurator_att,
        d_oper_misurator_att_str,
        matricola_misuratore,
        anno_start_misure_orarie,
        mese_start_misure_orarie,
        t_piva,
        t_rag_soc,
        t_servizio_tutela_sii
  = Value
}
