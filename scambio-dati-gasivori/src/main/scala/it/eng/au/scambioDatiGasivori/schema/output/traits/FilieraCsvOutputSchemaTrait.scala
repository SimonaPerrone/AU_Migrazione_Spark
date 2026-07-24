package it.eng.au.scambioDatiGasivori.schema.output.traits

import it.eng.au.scambioDatiGasivori.schema.SchemaEnum

trait FilieraCsvOutputSchemaTrait extends SchemaEnum {
  val RAGIONE_SOCIALE_CLIENTE,
  PIVA_CLIENTE,
  CF_CLIENTE,
  COD_PDR,
  CLASSE_AGEVOLAZIONE,
  DATA_INIZIO,
  DATA_FINE = Value
}
