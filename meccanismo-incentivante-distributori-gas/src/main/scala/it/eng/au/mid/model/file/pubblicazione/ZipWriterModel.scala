package it.eng.au.mid.model.file.pubblicazione

/** *
 * Ogni elemento rappresenta un file Zip contenente uno o piu' file.
 *
 * @param pivaId pivaId
 * @param fileName nome del file da scrivere (deve essere un nome completo comprensivo di path)
 * @param files    lista di file testuali da scrivere all'interno del file zip
 */
case class ZipWriterModel(
                           pivaId: String,
                           fileName: String, // nome file zip
                           files: List[FileModel] // lista di file da scrivere all'interno dello zip
                         ){
  override def toString: String = s"pivaId: $pivaId, fileName: $fileName, files: ${files.mkString(";")}"

  override def equals(that: Any): Boolean =
    that match {
      case that: ZipWriterModel => {
          this.pivaId == that.pivaId &&
          this.fileName == that.fileName &&
          this.files.mkString("") == that.files.mkString("")
      }
      case _ => false
    }
}
