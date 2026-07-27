package it.eng.au.portale_consumi_ee.schema.rcu

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object RcuIndirizzoPSchema extends SchemaEnum{
  val
          n_id,
          t_toponimo,
          t_nomestrada,
          t_civico,
          t_comune,
          t_comune_istat,
          t_cap,
          t_provincia,
          t_nazione,
          t_indirizzo_completo,
          t_nota
  = Value
}
