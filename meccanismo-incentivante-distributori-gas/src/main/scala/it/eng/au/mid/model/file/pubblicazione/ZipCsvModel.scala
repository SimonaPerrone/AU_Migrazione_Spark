package it.eng.au.mid.model.file.pubblicazione

// classe che determina il contenuto dei file CSV con i parametri richiesti per definire il file Zip
case class ZipCsvModel(
                        pivaId: String,
                        zipFileName: String = null, // nome file zip dove salvare CSV
                        fileModel: FileModel // file CSV nome e contenuto
                      ) {

  override def toString: String = {
    s"(pivaId: $pivaId, zipFileName: $zipFileName, fileModel: $fileModel)"
  }

}
