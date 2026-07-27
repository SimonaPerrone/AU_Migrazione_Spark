package it.eng.au.portale_consumi_ee.schema.mongodbs

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object FornitureSchema extends SchemaEnum{
  val
          n_id_fornitura,
          inizio,
          fine,
          d_inizio_str,
          d_fine_str,
          codice_pod,
          attivo,
          n_id_pod,
          n_id_fornitore,
          t_tipo_mercato,
          n_id_cliente,
          n_id_indirizzo,
          n_id_ind_forn,
          t_servizio_tutela_sii
  = Value
}
