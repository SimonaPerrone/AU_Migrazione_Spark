package it.eng.au.pubblicazione_cce.file.writer

import it.eng.au.pubblicazione_cce.common.CostantiCCE
import it.eng.au.pubblicazione_cce.model.file.ZipFileModel
import it.eng.au.pubblicazione_cce.model.flow.EsitoConsumiModel
import it.eng.au.pubblicazione_cce.schema.file.{FileConsumiSchema, FileSchema, ZipFileSchema}
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import it.eng.au.pubblicazione_cce.utility.file.FileUtility.set777
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.io.{BufferedInputStream, File, FileInputStream, FileOutputStream}
import java.nio.file.Files
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.mutable.ListBuffer
import scala.util.Try

/*
Interfaccia per definire gli elementi necessari per generare un file ZIP.
La funzione computeCsvElements aggiunge al dataframe le colonne presenti in FileModel
 */
class ZipWriter extends Serializable {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  // Carattere separatore percorso file system
  val PATH_SEPARATOR: String = "/"
  // Estensione file
  val FILE_EXTENSION: String = ".zip"

  val MAX_BYTES_SIZE_ZIP: Long = Environment.getOutputFileZipMaxByteSize

  val outputFilePath: String = Environment.getOutputFilePath

  val processDate: LocalDate = Environment.processDate

  val fileTimestamp: String = Environment.fileTimestamp

  val numberOfSubDirectories = 6 //da root a file creato ci sono 6 cartelle

  // Funzione per calcolare nome file (senza estensione) da colonne DataFrame
  def computeFileName: Column = {
    val dataCalcolo: LocalDate = processDate
    val annoCalcolo: String = dataCalcolo.getYear.toString
    val meseCalcolo: String = dataCalcolo.format(DateTimeFormatter.ofPattern("MM"))

    concat_ws("_",
      col(FileConsumiSchema.piva),
      lit(CostantiCCE.CCE),
      col(FileConsumiSchema.processo),
      lit(annoCalcolo + meseCalcolo),
      lit(fileTimestamp),
      col(FileConsumiSchema.id_richiesta)
    )
  }

  // Funzione per calcolare colonna del percorso base del file (root)
  def computePathRoot: Column = lit(outputFilePath)

  // Raggruppa i file CSV per richiesta e compone la lista dei file da comprimere insieme nel file zip
  def computeZipFiles(dataFrame: DataFrame): Dataset[ZipFileModel] = {
    dataFrame
      .groupBy(
        col(FileConsumiSchema.id_richiesta),
        col(FileConsumiSchema.piva),
        col(FileConsumiSchema.processo),
        col(FileConsumiSchema.sessione)
      )
      .agg(
        collect_set(FileSchema.fileFullName).as(ZipFileSchema.files), // lista file da comprimere in file zip per ogni richiesta
        max(ZipFileSchema.filePathSubDirectories).as(ZipFileSchema.filePathSubDirectories)
      )
      .withColumn(ZipFileSchema.filePathRoot, computePathRoot)
      .withColumn(ZipFileSchema.fileName, computeFileName)
      // id richiesta, filename, path, subidrs
      .selectExpr(ZipFileSchema.getValues: _*)
      .as[ZipFileModel]
  }

  // Scrive i file csv all'interno del file zip, ritorna esito con file scritti
  def write(ds: Dataset[ZipFileModel]): Dataset[EsitoConsumiModel] = {
    // SEMBRA importante trasformare in RDD prima dell'elaborazione. Non sembra fare l'azione altrimenti
    val esito = ds.rdd.map(zip => {
      // ogni elemento del ciclo rappresenta un file zip da scrivere

      var fileCount = 1 // conteggio progressivo file zip
      var zipBytesSize = 0L // dimensione in bytes del file zip
      // dati file zip
      var filenameZip = zip.fileName + "_" + fileCount + FILE_EXTENSION
      val zipOutputPath = zip.filePathRoot + zip.filePathSubDirectories

      val zipFileList = ListBuffer(filenameZip) // lista file zip prodotti (da portare in esito finale)

      var fileZip = new File(zipOutputPath + filenameZip)
      val filePath = fileZip.getParentFile.toPath
      // crea cartella se non esiste
      if (!Files.exists(filePath)) {
        Files.createDirectories(filePath)
        // imposta permessi 777 alle cartelle create
        var parentDir = filePath
        Try(set777(parentDir.toFile))
        for (_ <- Range.inclusive(1, numberOfSubDirectories)) {
          parentDir = parentDir.getParent
          Try(set777(parentDir.toFile))
        }
      }

      // crea file zip
      var outputFileZip = new ZipOutputStream(new FileOutputStream(fileZip))

      // scrivi csv all'interno
      // files: /path/to/file1.csv/path/to/file2.csv,
      val filesLength = zip.files.length
      for ((csvFullPath, index) <- zip.files.zipWithIndex) {
        // ogni elemento del ciclo rappresenta un file csv da scrivere nel file zip

        // leggi e comprimi file csv in zip
        val csvFile = new File(csvFullPath)
        val zipEntry = new ZipEntry(csvFile.getName)
        val inputStream = new BufferedInputStream(new FileInputStream(csvFullPath))
        outputFileZip.putNextEntry(zipEntry)
        var dataByte = inputStream.read()
        while (dataByte > -1) {
          outputFileZip.write(dataByte)
          dataByte = inputStream.read()
        }
        inputStream.close()
        outputFileZip.closeEntry()

        // se non e' l'ultimo file CSV da scrivere
        if (index + 1 < filesLength) {
          // Controlla se inserendo un nuovo file si supera la dimensione massima dello zip
          zipBytesSize = zipBytesSize + zipEntry.getCompressedSize
          if (zipBytesSize >= MAX_BYTES_SIZE_ZIP) {
            // chiudi zip corrente
            outputFileZip.close()
            set777(fileZip)
            // crea nuovo file zip con progressivo +1
            fileCount = fileCount + 1 // incrementa progressivo file
            zipBytesSize = 0L // azzera contatore dimensione file
            filenameZip = zip.fileName + "_" + fileCount + FILE_EXTENSION
            fileZip = new File(zipOutputPath + filenameZip)
            outputFileZip = new ZipOutputStream(new FileOutputStream(fileZip))
            zipFileList += filenameZip
          }
        }
      }
      outputFileZip.close()
      set777(fileZip)
      //id_richiesta, lista file zip, numero file zip
      EsitoConsumiModel(
        richiesta = zip.id_richiesta,
        nZipFiles = fileCount,
        zipFiles = zipFileList.toList,
        execution_id_input_read = null
      )
    })

    esito.toDS()
  }

}
