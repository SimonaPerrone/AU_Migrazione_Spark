package it.eng.au.pubblicazione_cce.schema.file

import it.eng.au.pubblicazione_cce.schema.SchemaEnum

object FileAmmissibilitaPodSchema extends SchemaEnum {
  val
  id_richiesta,
  timestamp,
  pod,
  ammissibilita,
  codice_inammissibilita,
  descrizione
  = Value
}
