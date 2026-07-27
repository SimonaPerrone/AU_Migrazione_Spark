package it.eng.au.portale_consumi_ee.schema.rcu

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object RcuPodMisurePSchema extends SchemaEnum{
  val
      n_id_pod,
      d_anno_mese,
      t_trattamento,
      t_trattamento_succ,
      n_consumo_annuo,
      t_nota,
      d_aggiornamento,
      n_id_traccia,
      n_id_s_prec
  = Value
}
