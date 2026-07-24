package it.eng.au.scambioDatiGasivori.schema.gasivori

import it.eng.au.scambioDatiGasivori.schema.SchemaEnum

object GasivoriPerimetroAmmSchema extends SchemaEnum {
  val n_id_gasivori_file,
  t_nome_file_in,
  piva_cliente,
  cf_cliente,
  prestazione,
  classe_agevolazione,
  data_inizio,
  verifica_amm,
  cod_causale,
  motivazione = Value
}
