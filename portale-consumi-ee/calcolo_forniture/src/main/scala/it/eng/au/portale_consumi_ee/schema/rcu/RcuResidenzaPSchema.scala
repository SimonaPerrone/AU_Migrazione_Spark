package it.eng.au.portale_consumi_ee.schema.rcu

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object RcuResidenzaPSchema extends SchemaEnum{
  val
          n_id,
          n_id_fornitura,
          t_residente,
          d_inizio_residenza,
          d_fine_residenza,
          b_storico,
          b_valido,
          b_ultima,
          n_id_traccia,
          d_aggiornamento
  = Value
}
