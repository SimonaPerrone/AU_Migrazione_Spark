package it.eng.au.mid.dao.file.zip

import it.eng.au.mid.common.FileUtility.set777
import it.eng.au.mid.model.file.pubblicazione.ZipWriterModel
import org.apache.spark.sql.Dataset

import java.io.{File, FileOutputStream}
import java.nio.file.Files
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.util.Try



class ZipWriterDao {

  /***
   * Scrive i file zip con all'interno i file CSV passati nel dataset
   */
  def write(ds: Dataset[ZipWriterModel]): Unit = {
    ds.foreach(fileZip => {
      val fileOutput = new File(fileZip.fileName)
      val pathFile = fileOutput.getParentFile.toPath
      // crea path se non esiste la cartella di destinazione
      if (!Files.exists(pathFile)){
        Files.createDirectories(pathFile)
        // imposta permessi cartella -> AGG4_PIVA/ANNO/MESE per MID1, ANNO/MESE per MID2
        Try(set777(pathFile.getParent.getParent.toFile)) //AGG4_PIVA (solo MID1)
        set777(pathFile.getParent.toFile) //ANNO
        set777(pathFile.toFile) //MESE
      }
      val zip = new ZipOutputStream(new FileOutputStream(fileOutput))
      fileZip.files.foreach(csv => {
        zip.putNextEntry(new ZipEntry(csv.fileName))
        zip.write(csv.content)
        zip.closeEntry()
      })
      zip.close()
      set777(fileOutput)
    })
  }

}
