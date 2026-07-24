package it.eng.au.pubblicazione_cce.schema.file

import it.eng.au.pubblicazione_cce.schema.SchemaEnum

object FileElencoFlussiSchema extends SchemaEnum {
  val
  //file name
  piva,
  ruolo,
  sessione, //CCE
  processo, //P,Pr...
  annomese,
  timestamp,
  id_richiesta,

  //contenuto
  pod,
  path_cloud,
  data_aggiornamento
  = Value
}
