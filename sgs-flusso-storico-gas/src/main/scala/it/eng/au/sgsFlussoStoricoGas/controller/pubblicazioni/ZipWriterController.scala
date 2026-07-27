package it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni

import it.eng.au.sgsFlussoStoricoGas.model.PubListValidatedModel
import java.io._
import java.nio.file.Paths
import java.util.zip.{ZipEntry, ZipOutputStream}

class ZipWriterController {
  def writeZip(xmlFiles: List[PubListValidatedModel]): Unit = {
    try {
      xmlFiles.foreach { case PubListValidatedModel(_, xmlDir, xmlFileName, _, zipFilePath, _, _, validationResult) =>
        val fileToZipPath = Paths.get(xmlDir, xmlFileName).toString
        val fileToZip = new File(fileToZipPath)

        if (fileToZip.exists() && validationResult == "OK") {

          println(s"Creazione ZIP: $zipFilePath")
          val zipOut = new ZipOutputStream(new FileOutputStream(zipFilePath))
          try {
            val inputStream = new FileInputStream(fileToZip)
            try {
              zipOut.putNextEntry(new ZipEntry(xmlFileName))
              val buffer = new Array[Byte](1024)
              var bytesRead = inputStream.read(buffer)
              while (bytesRead != -1) {
                zipOut.write(buffer, 0, bytesRead)
                bytesRead = inputStream.read(buffer)
              }
              zipOut.closeEntry()
            } finally {
              inputStream.close()
            }

            // Elimina il file XML dopo il successo
            if (fileToZip.delete()) {
              println(s"File eliminato: $fileToZipPath")
            } else {
              println(s"Errore nell'eliminazione del file: $fileToZipPath")
            }
          } finally {
            zipOut.close()
          }
        } else if (validationResult == "KO") {
          if (fileToZip.exists() && fileToZip.delete()) {
            println(s"File KO eliminato: $fileToZipPath")
          } else {
            println(s"Errore nell'eliminazione del file KO o file non trovato: $fileToZipPath")
          }
        }
      }
    } catch {
      case ex: IOException =>
        println(s"Errore durante la creazione dello ZIP: ${ex.getMessage}")
    }
  }
}
