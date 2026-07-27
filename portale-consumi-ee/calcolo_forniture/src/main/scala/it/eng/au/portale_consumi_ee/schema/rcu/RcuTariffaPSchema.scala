package it.eng.au.portale_consumi_ee.schema.rcu

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object RcuTariffaPSchema extends SchemaEnum{
  val
          n_id_tariffa,
          n_id_fornitura,
          t_tariffa_distr,
          d_inizio_tariffa,
          d_fine_tariffa,
          b_storico,
          b_valido,
          b_ultima,
          n_id_traccia,
          d_aggiornamento
  = Value
}
