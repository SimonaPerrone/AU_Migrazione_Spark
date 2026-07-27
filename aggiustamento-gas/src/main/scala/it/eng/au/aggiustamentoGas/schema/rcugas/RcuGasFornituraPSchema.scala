package it.eng.au.aggiustamentoGas.schema.rcugas

import it.eng.au.aggiustamentoGas.schema.SchemaEnum

object RcuGasFornituraPSchema extends SchemaEnum {
  val n_id_fornitura,
    d_data_inizio,
    d_data_fine,
    n_id_cliente,
    n_id_pdr,
    n_id_vend,
    b_tariffa_tm,
    t_codice_ateco,
    n_lettura_attivazione,
    t_aliquota_iva,
    t_imposte,
    n_indirizzo_fornitura,
    n_indirizzo_recap,
    t_bonus_gas,
    d_data_inizio_bonus,
    d_data_fine_bonus,
    b_prestazioni_non_concluse,
    b_disalimentabilita,
    t_codice_contratto_vendita,
    t_id_contratto_vend,
    d_data_stipula,
    t_note,
    d_aggiornamento,
    n_id_traccia,
    n_id_s_prec,
    tipo_data_inizio,
    tipo_data_fine,
    d_data_rif,
    t_tipo_fornitura,
    n_indirizzo_fatt
  = Value
}
