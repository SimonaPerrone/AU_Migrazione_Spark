package it.eng.au.mid.model.file.pubblicazione

// nome e contenuto del file CSV
case class FileModel(
                      fileName: String,
                      content: Array[Byte]
                    ) {

  override def toString: String = {
    s"(fileName: $fileName, content: ${content.map(_.toChar).mkString})"
  }

}
