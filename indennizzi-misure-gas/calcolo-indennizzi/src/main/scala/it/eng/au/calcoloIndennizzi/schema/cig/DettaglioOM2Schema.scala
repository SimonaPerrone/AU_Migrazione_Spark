package it.eng.au.calcoloIndennizzi.schema.cig

import it.eng.au.indennizziMisureGasCommon.schema.SchemaEnum

object DettaglioOM2Schema extends SchemaEnum {
  val
  id_indennizzo,
  piva_distr,
  rag_soc_distr,
  piva_udd,
  rag_soc_udd,
  percentage_lower_bound,
  percentage_upper_bound,
  target_percentage,
  achieved_percentage,
  pdr_base,
  pdr_target,
  pdr_count,
  delta_pdr,
  euro_fee_per_pdr,
  indennizzo,
  annomese,
  executionid
  = Value
}
