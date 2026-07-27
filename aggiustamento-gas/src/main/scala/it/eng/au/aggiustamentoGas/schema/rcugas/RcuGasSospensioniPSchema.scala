package it.eng.au.aggiustamentoGas.schema.rcugas

import it.eng.au.aggiustamentoGas.schema.SchemaEnum

object RcuGasSospensioniPSchema extends SchemaEnum {
  val  n_id_sospensione,
    n_id_fornitura,
    n_id_pdr,
    d_data_inizio_sosp,
    d_data_revoca_sosp,
    t_cod_causale_sospensione,
    t_mancata_riattivazione,
    t_mancata_sospensione,
    d_aggiornamento,
    n_id_traccia,
    n_id_s_prec,
    d_data_rif,

    n_id_fornitura_sosp
  = Value
}
