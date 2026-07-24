package it.eng.au.pubblicazione_cce.schema.file

import it.eng.au.pubblicazione_cce.schema.SchemaEnum

object FileConsumiCaSchema extends SchemaEnum {
  val

  //file name
  piva,
  ruolo,
  sessione, //CCE
  processo, //P,Pr...
  anno,
  timestamp,
  id_richiesta,

  //contenuto
  cod_pod,
  piva_distr,
  piva_udd,
  ca,
  data_aggiornamento,
  nome_file,
  executionid
  = Value
}
