package it.au.misure.ingestionMisureGasUnico.model.schema.unzip

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum

object LogSchema extends SchemaEnum{
  val
  codice,
  descrizione,
  filename_src,
  filename_folder_dest,
  annomesegiornodir,
  copy_or_unzip,
  dataelaborazione,
  annomese
  = Value
}
