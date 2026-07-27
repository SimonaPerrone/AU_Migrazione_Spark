package it.eng.au.portaleConsumi.schema.flow.misure

import it.eng.au.portaleConsumi.schema.SchemaEnum

/***
 * Classe standard per le misure gas derivanti da tutte le fonti in input (flussi)
 */
object MisureGasSchema extends SchemaEnum {
  val
  codice_pdr,
  lettura,
  data_lettura,
  motivazione,
  data_caricamento,
  flusso,
  annomese
  = Value
}
