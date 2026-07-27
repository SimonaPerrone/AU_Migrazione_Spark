package it.eng.au.portale_consumi_ee.schema.mongodbs

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object rcuCodiceOffertaPSchema extends SchemaEnum{
  val
      n_id_codice_offerta,
      n_id_fornitura,
      t_codice_offerta,
      d_data_inizio,
      d_data_fine,
      t_nota,
      d_aggiornamento,
      n_id_traccia,
      n_id_s_prec,
      d_data_rif
  = Value
}
