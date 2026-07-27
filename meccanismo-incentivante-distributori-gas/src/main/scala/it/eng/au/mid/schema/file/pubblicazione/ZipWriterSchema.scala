package it.eng.au.mid.schema.file.pubblicazione

import it.eng.au.mid.schema.SchemaEnum

// zip filename e contenuto dei files csv, aggiunto pivaID per popolare tabella AggregatoreInfo
object ZipWriterSchema extends SchemaEnum {
  val
  pivaId,
  fileName,
  files
  = Value

}
