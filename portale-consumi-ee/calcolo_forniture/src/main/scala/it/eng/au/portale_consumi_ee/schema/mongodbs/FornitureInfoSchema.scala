package it.eng.au.portale_consumi_ee.schema.mongodbs

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object FornitureInfoSchema extends SchemaEnum{
  val
          n_id_fornitura,
          n_id_pod,
          n_id_cliente,
          d_inizio_titolarita,
          d_fine_titolarita,
          d_inizio_titolarita_str,
          d_fine_titolarita_str,
          n_id_fornitore,
          t_tipo_mercato,
          n_id_indirizzo,
          n_id_ind_forn,
          codice_pod,
          t_residente,
          t_tariffa_distr,
          t_piva,
          t_rag_soc,
          t_servizio_tutela_sii
  = Value
}
