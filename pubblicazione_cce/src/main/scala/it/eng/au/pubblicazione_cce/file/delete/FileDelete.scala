package it.eng.au.pubblicazione_cce.file.delete

import it.eng.au.pubblicazione_cce.args.ArgsFactory.logger
import it.eng.au.pubblicazione_cce.model.file.FileModel
import it.eng.au.pubblicazione_cce.utility.file.FileUtility.set777
import org.apache.spark.sql.Dataset

import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Paths}
import scala.util.Try

class FileDelete extends Serializable {
  /** *
   * Rimuove file preccedentemente scritti per essere zippati, successivamente inutili
   */
  def delete(ds: Dataset[FileModel]): Unit = {
    ds.foreachPartition(partition =>
      partition
        .foreach(fileMetadata => {
          val fileOutput = new File(fileMetadata.fileFullName)
          val filePath = Paths.get(fileMetadata.fileFullName)
          // Check if the file exists before attempting to delete it
          if (Files.exists(filePath)) {
            // Delete the file
            try {
              fileOutput.delete()
            } catch {
              case e: Exception =>
                logger.error(s"An error occurred while deleting the file: ${e.getMessage}")
            }
          } else {
            logger.warn(s"File ${filePath.getFileName} does not exist.")
          }
        }
        )
    )
  }
}
