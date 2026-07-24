package it.eng.au.scambioDatiGasivori.schema.gasivori

import it.eng.au.scambioDatiGasivori.schema.SchemaEnum

object GasivoriFilieraIdSchema extends SchemaEnum {
  val t_ragione_sociale_cliente,
  t_piva_cliente,
  t_cf_cliente,
  t_codice_pdr,
  t_classe_agevolazione,
  d_data_inizio,
  d_data_fine,
  id_dest,
  t_executionid = Value
}
