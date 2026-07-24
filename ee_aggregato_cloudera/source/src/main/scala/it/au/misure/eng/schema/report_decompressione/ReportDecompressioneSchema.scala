package it.au.misure.eng.schema.report_decompressione

import it.au.misure.eng.schema.SchemaEnum

object ReportDecompressioneSchema extends SchemaEnum{
  val  codice
  , descrizione
  , filename_src
  , filename_folder_dest
  , annomesegiornodir
  , copy_or_unzip
  , dataelaborazione
  , annomese  = Value
}
