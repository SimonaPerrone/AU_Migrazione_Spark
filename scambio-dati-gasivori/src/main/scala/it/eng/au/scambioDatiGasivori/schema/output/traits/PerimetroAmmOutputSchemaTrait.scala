package it.eng.au.scambioDatiGasivori.schema.output.traits

import it.eng.au.scambioDatiGasivori.schema.SchemaEnum

trait PerimetroAmmOutputSchemaTrait extends SchemaEnum {
  val PIVA_CLIENTE,
  CF_CLIENTE,
  PRESTAZIONE,
  CLASSE_AGEVOLAZIONE,
  DATA_INIZIO,
  VERIFICA_AMM,
  COD_CAUSALE,
  MOTIVAZIONE = Value
}
