package it.eng.au.pubblicazione_cce.schema.file

import it.eng.au.pubblicazione_cce.schema.SchemaEnum

object FileElencoFlussiCaSchema extends SchemaEnum {
  val
  //file name
  piva,
  ruolo,
  sessione, //CCE
  processo, //CA
  anno,
  timestamp,
  id_richiesta,

  //contenuto
  pod,
  path_cloud
  = Value
}
