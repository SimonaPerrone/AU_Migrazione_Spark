package it.eng.au.pubblicazione_cce.model.file

case class FileModel(
                      filePathRoot: String,
                      filePathSubDirectories: Option[String] = None,
                      fileName: String,
                      fileFullName: String,
                      fileContent: Array[Byte]
                    ) {
  private val pathSeparator: String = "/"

  // Ensure root path has path separator at the end of string
  private def getFilePathRoot: String = filePathRoot.stripSuffix(pathSeparator) + pathSeparator

  // Ensure path path separator at end of string but not at the beginning
  private def getFilePathSubDirectories: String = {
    filePathSubDirectories match {
      case Some(path) => path.stripPrefix(pathSeparator).stripSuffix(pathSeparator) + pathSeparator
      case None => ""
    }
  }

  // File root path + sub directories
  def filePath: String = getFilePathRoot + getFilePathSubDirectories

  // Return number of sub directories in path sub directories
  def numberOfSubDirectories: Int = getFilePathSubDirectories.count(_ == pathSeparator.head)

  def fileInfoToString: String = {
    s"(fileFullName: $fileFullName, content: ${fileContent.map(_.toChar).mkString})"
  }
}
