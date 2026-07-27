package it.eng.au.sgsFlussoStoricoGas.schema.perimetro

import it.eng.au.sgsFlussoStoricoGas.schema.SchemaEnum

object SgsPerimetroSchema extends SchemaEnum {
  val
  n_id_pratica,
  t_tipo_pratica,
  d_data_decorrenza,
  t_codice_pdr,
  n_id_pdr,
  piva_udd_entrante,
  piva_udb_entrante,
  piva_udb_uscente,
  data_estrazione,
  t_trattamento,
  t_stato_perimetro,
  anno_mese_calcolo_perimetro,
  giorno_calcolo_perimetro,
  executionId
  = Value
}
