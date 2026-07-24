package it.au.misure.ingestionMisureGasUnico.utility

import it.au.misure.ingestionMisureGasUnico.model.GasXmlMetadata
import org.apache.log4j.Logger

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDate
import java.time.format._
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}
import scala.xml.{NodeSeq, XML}

object FileUtility {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  val gasOldRegex: Regex = "^([A-Za-z0-9]+)_([A-Za-z0-9]+)_(\\d{6})_(?:(DEF|FUI|RGL|RML|RMV|RSL|SW1|TAL|TAS|TAV|TGL|TML|TMV)[0-9.]*)_(\\d+)(?:_(\\d+))?\\.((?i)xml|zip|xml\\.zip)$".r // unzip: vecchio tracciato
  val gasOldPrestazionaliRegex: Regex = "^[A-Za-z0-9_.]*(IM1|A01|A40|D01|SM1|R01|A02|V01|SM2|M01|V02)[0-9_.]*\\.((?i)xml|zip|xml\\.zip)$".r // unzip: prestazionali vecchio tracciato

  val gasStandardPreFilterRegex: Regex = "^[A-Za-z0-9_.]*(?:SWG1|AD2|AD3|AD4|AD5|AD2R|AD3R|AD4R|AD5R|_[M|R])(?:_[A-Za-z0-9_.]*)?\\.((?i)xml|zip|xml\\.zip)$".r  // unzip: tracciato Standard
  val gasIGMGPreFilterRegex: Regex = "^[A-Za-z0-9_.]*(IGMG)[A-Za-z0-9_.]*\\.((?i)xml|zip|xml\\.zip)$".r // unzip: IGMG
  val gasIGMRPreFilterRegex: Regex = "^[A-Za-z0-9_.]*(IGMR)[A-Za-z0-9_.]*\\.((?i)xml|zip|xml\\.zip)$".r // unzip: IGMR
  val gasStandardRegex: Regex = "^([A-Za-z0-9]+)_([A-Za-z0-9]+)_(\\d{6})_([A-Z0-9]{3,4})_(\\d+)_(\\d+)_([M|R])\\.((?i)zip|xml\\.zip)$".r // unzip: tracciato Standard
  val gasIGMGRegex: Regex = "^([A-Za-z0-9]+)_([A-Za-z0-9]+)_(\\d{6})_(IGMG)_(\\d+)_(\\d+)\\.((?i)zip|xml\\.zip)$".r // unzip: IGMG
  val gasIGMRRegex: Regex = "^([A-Za-z0-9]+)_([A-Za-z0-9]+)_(\\d{6})_(IGMR)_(\\d+)_(\\d+)\\.((?i)zip|xml\\.zip)$".r // unzip: IGMR

  val gasStandardRegexXml: Regex = "^([A-Za-z0-9]+)_([A-Za-z0-9]+)_(\\d{6})_([A-Z0-9]{3,4})_(\\d+)_(\\d+)_([M|R])\\.((?i)xml)$".r // ingestion: tracciato Standard
  val gasIGMGRegexXml: Regex = "^([A-Za-z0-9]+)_([A-Za-z0-9]+)_(\\d{6})_(IGMG)_(\\d+)_(\\d+)\\.((?i)xml)$".r // ingestion: IGMG
  val gasIGMRRegexXml: Regex = "^([A-Za-z0-9]+)_([A-Za-z0-9]+)_(\\d{6})_(IGMR)_(\\d+)_(\\d+)\\.((?i)xml)$".r // ingestion: IGMR


  def dateToFolder(date: LocalDate): String = {
    val year = date.getYear
    val month = "%02d".format(date.getMonthValue)
    val day = "%02d".format(date.getDayOfMonth)
    s"$year/$month$day"
  }

  def isFolderBetweenDates(folder: File, startDate: LocalDate, endDate: LocalDate): Boolean = {
    val dateTry = Try(folderToDate(folder))

    dateTry match {
      case Success(date) => !date.isBefore(startDate) && !date.isAfter(endDate)
      case Failure(exception) =>
        logger.warn(s"folder ${folder.getPath} error", exception)
        false
    }
  }

  def isFolderBetweenYears(folder: File, startDate: LocalDate, endDate: LocalDate): Boolean = {
    val yearTry = Try(folder.getName.toInt)
    yearTry match {
      case Success(year) => year >= startDate.getYear && year <= endDate.getYear
      case Failure(exception) =>
        logger.warn(s"folder ${folder.getPath} error", exception)
        false
    }

  }

  def folderToDate(folder: File): LocalDate = {
    LocalDate.parse(folder.getParentFile.getName + folder.getName,
      DateTimeFormatter.BASIC_ISO_DATE)
  }

  def fileToDate(file: File): LocalDate = {
    LocalDate.parse(file.getParentFile.getParentFile.getName + file.getParentFile.getName,
      DateTimeFormatter.BASIC_ISO_DATE)
  }

  def extractNodeOrNull(nodeSeq: NodeSeq): String = {
    if (nodeSeq.isEmpty) {
      null
    } else {
      nodeSeq.text
    }
  }

  def xmlToMetadata(xmlFile: File): GasXmlMetadata = {
    val xmlNode = XML.loadFile(xmlFile)
    val giorno = xmlFile.getParentFile.getName
    val mese = xmlFile.getParentFile.getParentFile.getName
    val anno = xmlFile.getParentFile.getParentFile.getParentFile.getName

    xmlFile.getName match {
      case FileUtility.gasStandardRegexXml(pivaDistributore, pivaUtente, dataRiferimento, flusso, timestamp, progressivo, ts, fileType) =>
        GasXmlMetadata(
          xmlNode = xmlNode
          , file = xmlFile
          , pivaDistributore = pivaDistributore
          , pivaUtente = pivaUtente
          , anno = anno
          , annoRiferimento = dataRiferimento.substring(0, 4)
          , mese = mese
          , meseRiferimento = dataRiferimento.substring(4)
          , giorno = giorno
          , flusso = flusso
          , timestamp = timestamp
          , progressivo = progressivo
          , tS = ts
        )
      case FileUtility.gasIGMGRegexXml(pivaDistributore, pivaUtente, dataRiferimento, flusso, timestamp, progressivo,  fileType) =>
        GasXmlMetadata(
          xmlNode = xmlNode
          , file = xmlFile
          , pivaDistributore = pivaDistributore
          , pivaUtente = pivaUtente
          , anno = anno
          , annoRiferimento = dataRiferimento.substring(0, 4)
          , mese = mese
          , meseRiferimento = dataRiferimento.substring(4)
          , giorno = giorno
          , flusso = flusso
          , timestamp = timestamp
          , progressivo = progressivo
          , tS = ""
        )
      case FileUtility.gasIGMRRegexXml(pivaDistributore, pivaUtente, dataRiferimento, flusso, timestamp, progressivo, fileType) =>
        GasXmlMetadata(
          xmlNode = xmlNode
          , file = xmlFile
          , pivaDistributore = pivaDistributore
          , pivaUtente = pivaUtente
          , anno = anno
          , annoRiferimento = dataRiferimento.substring(0, 4)
          , mese = mese
          , meseRiferimento = dataRiferimento.substring(4)
          , giorno = giorno
          , flusso = flusso
          , timestamp = timestamp
          , progressivo = progressivo
          , tS = ""
        )

      case _ => (
        GasXmlMetadata(
          xmlNode = xmlNode
          , file = xmlFile
          , pivaDistributore = ""
          , pivaUtente = ""
          , anno = ""
          , annoRiferimento = ""
          , mese = ""
          , meseRiferimento = ""
          , giorno = ""
          , flusso = ""
          , timestamp = ""
          , progressivo = ""
          , tS = ""
      ))
    }
  }

  def writeCsv(path: String, header: String, content: List[String], byteLimit: Option[Long] = None): Unit = {
    var fileOut = new File(path)

    if (!fileOut.getParentFile.exists()) fileOut.getParentFile.mkdirs()

    var indexFile = 1
    val fileExists = fileOut.exists()  // Controlla se il file esiste già
    var pw = new PrintWriter(new FileOutputStream(fileOut, true)) // Modalità append
    if (!fileExists) pw.write(header + "\n") // Scrive l'header solo se il file non esiste

    if (byteLimit.isDefined) {
      content.zipWithIndex.foreach({ case (row, index) =>
        pw.write(row + "\n")
        // every 1000 rows => flush
        if (index != 0 && index % 1000 == 0) {
          pw.flush()
          if (fileOut.length() >= byteLimit.get) {
            pw.close()
            // se non è l'ultima riga crea un nuovo file
            if (index < content.length - 1) {
              fileOut = new File(path.replace(".txt", s"_$indexFile.txt"))
              indexFile += 1
              pw = new PrintWriter(new FileOutputStream(fileOut, true))
              pw.write(header + "\n") // Scrive header nel nuovo file
            }
          }
        }
      })
    } else {
      content.foreach(row => pw.write(row + "\n"))
    }
    pw.flush()
    pw.close()
  }

  def getAmmissibilitaOutputFolder(filePath: String, inputPath: String, outputPath: String): String = {
    val outFolder = filePath.replaceAll("\\\\", "/").replace(inputPath, outputPath)
    outFolder
  }
}
