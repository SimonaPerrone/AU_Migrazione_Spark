package it.eng.au.pubblicazione_cce.file.writer

import it.eng.au.pubblicazione_cce.model.file.FileModel
import it.eng.au.pubblicazione_cce.utility.file.FileUtility.set777
import org.apache.spark.sql.Dataset

import java.io.{File, FileOutputStream}
import java.nio.file.Files
import scala.util.Try

class FileWriter extends Serializable {
  /** *
   * Scrive file in dataset ed associa permessi 777 a file e cartelle create
   * (se le cartelle esistono gia' allora non cambia i permessi)
   */
  def write(ds: Dataset[FileModel]): Unit = {
    ds.foreachPartition( partition =>
      partition
      .foreach(fileMetadata => {
      val fileOutput = new File(fileMetadata.fileFullName)
      val pathFile = fileOutput.getParentFile.toPath
      // crea path se non esiste la cartella di destinazione
      if (!Files.exists(pathFile)) {
        Files.createDirectories(pathFile)
        // imposta permessi 777 alle cartelle create
        var parentDir = pathFile
        Try(set777(parentDir.toFile))
        val numberOfSubDirectories = fileMetadata.numberOfSubDirectories
        for (_ <- Range.inclusive(1, numberOfSubDirectories)) {
          parentDir = parentDir.getParent
          Try(set777(parentDir.toFile))
        }
      }
      // scrivi contenuto file
      val fileOutputStream = new FileOutputStream(fileOutput)
      fileOutputStream.write(fileMetadata.fileContent)
      fileOutputStream.close()
      set777(fileOutput)
    }
      )
    )
  }
}
