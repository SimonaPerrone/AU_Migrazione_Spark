package it.au.misure.ingestionMisureGasUnico.unzip

import com.typesafe.config.ConfigFactory
import it.au.misure.ingestionMisureGasUnico.args.UnzipArgsConfig
import it.au.misure.ingestionMisureGasUnico.model.GasUnzipMetadata
import it.au.misure.ingestionMisureGasUnico.model.schema.CsvRecoverySchema
import it.au.misure.ingestionMisureGasUnico.model.schema.DecompressioneLogSchema.{PHASE_C, PHASE_E, PHASE_U, STATUS_0, STATUS_2, STATUS_3, STATUS_4, STATUS_5, STATUS_6, STATUS_7}
import it.au.misure.ingestionMisureGasUnico.model.schema.unzip.LogSchema
import it.au.misure.ingestionMisureGasUnico.utility.Constants._
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import it.au.misure.ingestionMisureGasUnico.utility.{Constants, FileUtility, PropertyUtility}
import it.au.misure.ingestionMisureGasUnico.validate.{ValidateFileIGMG, ValidateFileIGMR, ValidateFileStandard}
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{from_unixtime, lead, to_date, unix_timestamp}
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel
import org.xml.sax.SAXParseException

import java.io.{File, FileNotFoundException}
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.zip.{ZipException, ZipFile}
import scala.collection.JavaConverters._
import scala.xml.XML

object UnzipFlow {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def run(argsConfig: UnzipArgsConfig): Unit = {
    val timeZone: String = ConfigFactory.load.getString("timeZone")
    val unzipTimestamp = Timestamp.valueOf(LocalDateTime.now(ZoneId.of(timeZone)))
    val isFileRecoveryEnabled = argsConfig.fileRecovery
    logger.warn(s"*** timestamp di decompressione: $unzipTimestamp")

    logger.warn(s"Cleaning tmp folders")
    cleanOldTmpFolder(PropertyUtility.getTmpOutputFolderOld)
    cleanOldTmpReportsFolder(PropertyUtility.getAmmissibilitaStandardPath)
    cleanOldTmpReportsFolder(PropertyUtility.getAmmissibilitaIgmgPath)
    cleanOldTmpReportsFolder(PropertyUtility.getAmmissibilitaIgmrPath)

    val rootPath: String = PropertyUtility.getUnzipInputPath
    logger.warn(s"Unzip root path: $rootPath")
    val dataRdd = getFilesWithMetadata(argsConfig, rootPath)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val correctFilesRDD = dataRdd
      .filter(meta => !meta.trackType.equals(NAMING_UNMATCH)) // naming convention matches
    val wrongFilesRDD = dataRdd
      .filter(meta => meta.trackType.equals(NAMING_UNMATCH)) // naming convention unmatches

    val unzipRDD = correctFilesRDD
      .repartition(correctFilesRDD.partitions.length)
      .flatMap(unzipXml(_, PropertyUtility.getTmpOutputFolder, PropertyUtility.getTmpOutputFolderOld))
    val repartitionedUnzipRDD = unzipRDD
      .repartition(unzipRDD.partitions.length)
      .persist(StorageLevel.MEMORY_AND_DISK)

    //add metadata about already transmitted rule
    // controlla per ogni file se è già stato decompresso. Se trova il file [[getTodayAlreadyTrasmittedFiles]]
    // viene valorizzato l'attributo alreadyTransmitted (booleano)
    logger.warn("Verifica file gia' trasmessi")
    val unzipWithCompleteMetaRdd = getTodayAlreadyTrasmittedFiles(
      repartitionedUnzipRDD
        .filter(meta => (meta.trackType.equals(STD) || meta.trackType.equals(IGMG) || meta.trackType.equals(IGMR)) && !meta.statusCode.equals(STATUS_2.toString))
    )
      .map({ case (fileName, alreadyTransmitted, meta) => meta.copy(alreadyTransmitted = alreadyTransmitted) })
      .persist(StorageLevel.MEMORY_AND_DISK)

    val flussiIGMGRdd = ValidateFileIGMG
      .validate(unzipWithCompleteMetaRdd.filter(meta => meta.trackType.equals(IGMG)), unzipTimestamp, isFileRecoveryEnabled)

    val flussiIGMRRdd = ValidateFileIGMR
      .validate(unzipWithCompleteMetaRdd.filter(meta => meta.trackType.equals(IGMR)), unzipTimestamp, isFileRecoveryEnabled)

    val flussiStandardRdd = ValidateFileStandard
      .validate(unzipWithCompleteMetaRdd.filter(meta => meta.trackType.equals(STD)), unzipTimestamp, isFileRecoveryEnabled)

    val oldFlussiRdd = repartitionedUnzipRDD
      .filter(meta => (meta.trackType.equals(OLD) || meta.trackType.equals(OLD_PRESTAZIONALE)) && meta.statusCode.equals(STATUS_0.toString))

    val flussiUnion = flussiStandardRdd
      .union(flussiIGMGRdd)
      .union(flussiIGMRRdd)
      .union(oldFlussiRdd)
      .persist(StorageLevel.MEMORY_AND_DISK)

    logger.warn("Writing XML")
    val flowsToXml = flussiUnion.filter(meta => meta.statusCode.equals(STATUS_0.toString) && meta.ammissibile)
    writeXml(flowsToXml)

    logger.warn("Write Log")
    writeLog(flussiUnion.union(wrongFilesRDD).union(
      repartitionedUnzipRDD.filter(meta =>
        ((meta.trackType.equals(OLD) || meta.trackType.equals(OLD_PRESTAZIONALE)) && !meta.statusCode.equals(STATUS_0.toString))
          || ((meta.trackType.equals(STD) || meta.trackType.equals(IGMG) || meta.trackType.equals(IGMR)) && meta.statusCode.equals(STATUS_2.toString)))
    )
      , unzipTimestamp)
  }

  /** *
   * Ritorna i file zip da elaborare applicando i filtri passati nei parametri ed in input, come RDD di [[GasUnzipMetadata]]
   */
  def getFilesWithMetadata(params: UnzipArgsConfig, inputPath: String): RDD[GasUnzipMetadata] = {
    logger.info(s"reading files into $inputPath")
    val rootFolder = new File(inputPath)

    // lista files zip da decomprimere a seconda del tipo di processo: se recovery, file recovery o non specificato
    val inputFiles = if (params.recovery) getCodice002FilesToRecover(params)
    else if (params.fileRecovery) getFilesToRecover(PropertyUtility.getRecoveryCsvPath)
    else getZipFiles(rootFolder, params)

    val filesWithMeta = inputFiles
      .filter(file => file.getName.toLowerCase.endsWith(".xml") || file.getName.toLowerCase.endsWith(".zip"))
      .map(file =>
        file.getName match {

          case FileUtility.gasIGMGPreFilterRegex(flusso, fileType) =>
            file.getName match {
              case FileUtility.gasIGMGRegex(pivaDistributore, pivaUtente, dataRiferimento, flusso, timestamp, progressivo, fileType) =>
                GasUnzipMetadata(
                  file = file
                  , pivaDistributore = pivaDistributore
                  , pivaUtente = pivaUtente
                  , anno = file.getParentFile.getParentFile.getName
                  , annoRiferimento = dataRiferimento.substring(0, 4)
                  , mese = file.getParentFile.getName.substring(0, 2)
                  , meseRiferimento = dataRiferimento.substring(4)
                  , giorno = file.getParentFile.getName.substring(2)
                  , flusso = flusso
                  , timestamp = timestamp
                  , progressivo = progressivo
                  , tS = ""
                  , trackType = IGMG
                )
              case _ =>
                GasUnzipMetadata(
                  file = file
                  , pivaDistributore = ""
                  , pivaUtente = ""
                  , anno = file.getParentFile.getParentFile.getName
                  , annoRiferimento = ""
                  , mese = file.getParentFile.getName.substring(0, 2)
                  , meseRiferimento = ""
                  , giorno = file.getParentFile.getName.substring(2)
                  , flusso = flusso
                  , timestamp = ""
                  , progressivo = ""
                  , tS = ""
                  , statusCode = STATUS_3
                  , statusMessage = "Nomenclatura tracciato non rispettata."
                  , statusType = PHASE_U
                  , trackType = IGMG
                  , fileError = IGMG_NAMING_UNMATCH
                )
            }

          case FileUtility.gasIGMRPreFilterRegex(flusso, fileType) =>
            file.getName match {
              case FileUtility.gasIGMRRegex(pivaDistributore, pivaUtente, dataRiferimento, flusso, timestamp, progressivo, fileType) =>
                GasUnzipMetadata(
                  file = file
                  , pivaDistributore = pivaDistributore
                  , pivaUtente = pivaUtente
                  , anno = file.getParentFile.getParentFile.getName
                  , annoRiferimento = dataRiferimento.substring(0, 4)
                  , mese = file.getParentFile.getName.substring(0, 2)
                  , meseRiferimento = dataRiferimento.substring(4)
                  , giorno = file.getParentFile.getName.substring(2)
                  , flusso = flusso
                  , timestamp = timestamp
                  , progressivo = progressivo
                  , tS = ""
                  , trackType = IGMR
                )

              case FileUtility.gasStandardRegex(_*) =>
                GasUnzipMetadata(
                  file = file
                  , pivaDistributore = ""
                  , pivaUtente = ""
                  , anno = file.getParentFile.getParentFile.getName
                  , annoRiferimento = ""
                  , mese = file.getParentFile.getName.substring(0, 2)
                  , meseRiferimento = ""
                  , giorno = file.getParentFile.getName.substring(2)
                  , flusso = flusso
                  , timestamp = ""
                  , progressivo = ""
                  , tS = ""
                  , statusCode = STATUS_3
                  , statusMessage = "Nomenclatura tracciato non rispettata."
                  , statusType = PHASE_U
                  , trackType = IGMR
                  , fileError = IGMR_NAMING_UNMATCH_BUT_STD_MATCH
                )

              case _ =>
                GasUnzipMetadata(
                  file = file
                  , pivaDistributore = ""
                  , pivaUtente = ""
                  , anno = file.getParentFile.getParentFile.getName
                  , annoRiferimento = ""
                  , mese = file.getParentFile.getName.substring(0, 2)
                  , meseRiferimento = ""
                  , giorno = file.getParentFile.getName.substring(2)
                  , flusso = flusso
                  , timestamp = ""
                  , progressivo = ""
                  , tS = ""
                  , statusCode = STATUS_3
                  , statusMessage = "Nomenclatura tracciato non rispettata."
                  , statusType = PHASE_U
                  , trackType = IGMR
                  , fileError = IGMR_NAMING_UNMATCH
                )
            }

          case FileUtility.gasStandardPreFilterRegex(fileType) =>
            file.getName match {
              case FileUtility.gasStandardRegex(pivaDistributore, pivaUtente, dataRiferimento, flusso, timestamp, progressivo, ts, fileType) =>
                GasUnzipMetadata(
                  file = file
                  , pivaDistributore = pivaDistributore
                  , pivaUtente = pivaUtente
                  , anno = file.getParentFile.getParentFile.getName
                  , annoRiferimento = dataRiferimento.substring(0, 4)
                  , mese = file.getParentFile.getName.substring(0, 2)
                  , meseRiferimento = dataRiferimento.substring(4)
                  , giorno = file.getParentFile.getName.substring(2)
                  , flusso = flusso
                  , timestamp = timestamp
                  , progressivo = progressivo
                  , tS = ts
                  , trackType = STD
                )
              case _ =>
                GasUnzipMetadata(
                  file = file
                  , pivaDistributore = ""
                  , pivaUtente = ""
                  , anno = file.getParentFile.getParentFile.getName
                  , annoRiferimento = ""
                  , mese = file.getParentFile.getName.substring(0, 2)
                  , meseRiferimento = ""
                  , giorno = file.getParentFile.getName.substring(2)
                  , flusso = ""
                  , timestamp = ""
                  , progressivo = ""
                  , tS = ""
                  , statusCode = STATUS_3
                  , statusMessage = "Nomenclatura tracciato non rispettata."
                  , statusType = PHASE_U
                  , trackType = STD
                  , fileError = STD_NAMING_UNMATCH
                )
            }

          case FileUtility.gasOldRegex(pivaDistributore, pivaUtente, dataRiferimento, flusso, timestamp, progressivo, fileType) =>
            GasUnzipMetadata(
              file = file
              , pivaDistributore = pivaDistributore
              , pivaUtente = pivaUtente
              , anno = file.getParentFile.getParentFile.getName
              , annoRiferimento = dataRiferimento.substring(0, 4)
              , mese = file.getParentFile.getName.substring(0, 2)
              , meseRiferimento = dataRiferimento.substring(4)
              , giorno = file.getParentFile.getName.substring(2)
              , flusso = flusso
              , timestamp = timestamp
              , progressivo = if (progressivo == null) "" else progressivo
              , tS = ""
              , trackType = OLD
            )

          case FileUtility.gasOldPrestazionaliRegex(flusso, fileType) =>
            GasUnzipMetadata(
              file = file
              , pivaDistributore = ""
              , pivaUtente = ""
              , anno = file.getParentFile.getParentFile.getName
              , annoRiferimento = ""
              , mese = file.getParentFile.getName.substring(0, 2)
              , meseRiferimento = ""
              , giorno = file.getParentFile.getName.substring(2)
              , flusso = flusso
              , timestamp = ""
              , progressivo = ""
              , tS = ""
              , trackType = OLD_PRESTAZIONALE
            )

          case _ =>
            GasUnzipMetadata(
              file = file
              , pivaDistributore = ""
              , pivaUtente = ""
              , anno = file.getParentFile.getParentFile.getName
              , annoRiferimento = ""
              , mese = file.getParentFile.getName.substring(0, 2)
              , meseRiferimento = ""
              , giorno = file.getParentFile.getName.substring(2)
              , flusso = ""
              , timestamp = ""
              , progressivo = ""
              , tS = ""
              , statusCode = STATUS_3
              , statusMessage = s"il file non matcha nessuna naming convention stabilita"
              , statusType = PHASE_U
              , trackType = NAMING_UNMATCH
            )
        })

    var filteredFilesWithMeta = filesWithMeta

    // filter keeping only required flows
    if (params.flows.isDefined) {
      val flows = params.flows.get.map(flow => flow.toLowerCase)
      // funzione extractRealCodFlusso usata per gestire correttamente i codici nei flussi gas vecchi non prestazionali (es. TAV.0150)
      //      filteredFilesWithMeta = filteredFilesWithMeta.filter(fileWithMeta => flows.exists(flow => extractRealCodFlusso(fileWithMeta.flusso).equalsIgnoreCase(flow)))
      filteredFilesWithMeta = filteredFilesWithMeta.filter(meta => flows.contains(meta.flusso.toLowerCase))
    }

    // filter keeping only old files if required
    if (params.oldOnly)
      filteredFilesWithMeta = filteredFilesWithMeta.filter(meta => meta.trackType.equals(OLD) || meta.trackType.equals(OLD_PRESTAZIONALE))

    // filter keeping standard and igmg and igmr files if required
    if (params.standardAndIgmgAndIgmr)
      filteredFilesWithMeta = filteredFilesWithMeta.filter(meta => meta.trackType.equals(STD) || meta.trackType.equals(IGMG) || meta.trackType.equals(IGMR))

    // filter keeping only standard files if required
    if (params.standardOnly)
      filteredFilesWithMeta = filteredFilesWithMeta.filter(meta => meta.trackType.equals(STD))

    // filter keeping only igmg files if required
    if (params.igmgOnly)
      filteredFilesWithMeta = filteredFilesWithMeta.filter(meta => meta.trackType.equals(IGMG))

    // filter keeping standard and igmg files if required
    if (params.standardAndIgmg)
      filteredFilesWithMeta = filteredFilesWithMeta.filter(meta => meta.trackType.equals(STD) || meta.trackType.equals(IGMG))

    // filter keeping igmg and igmr files if required
    if (params.igmgAndIgmr)
      filteredFilesWithMeta = filteredFilesWithMeta.filter(meta => meta.trackType.equals(IGMG) || meta.trackType.equals(IGMR))

    filteredFilesWithMeta
  }

  def unzipXml(metadata: GasUnzipMetadata, destRootPath: String, destRootPathOld: String): List[GasUnzipMetadata] = {

    // filtro naming unmatch STD e IGMG ottenute dalla funzione getFilesWithMetadata, propagando avanti l'informazione per l'ammissibilità file
    if (metadata.fileError.equals(STD_NAMING_UNMATCH) || metadata.fileError.equals(IGMG_NAMING_UNMATCH) || metadata.fileError.equals(IGMR_NAMING_UNMATCH)) {
      return List(metadata.copy())
    }

    val destFolderName = if (metadata.trackType.equals(STD))
      s"$destRootPath/Standard/${metadata.flusso}/${metadata.anno}/${metadata.mese}/${metadata.giorno}/"
    else if (metadata.trackType.equals(IGMG))
      s"$destRootPath/IGMG/IGMG/${metadata.anno}/${metadata.mese}/${metadata.giorno}/"
    else if (metadata.trackType.equals(IGMR))
      s"$destRootPath/IGMR/IGMR/${metadata.anno}/${metadata.mese}/${metadata.giorno}/"
    else
      s"$destRootPathOld${metadata.originalFolder}/"

    try {
      val fileExtension = metadata.file.getName.takeRight(3).toLowerCase
      fileExtension match {

        case "xml" => // casistica possibile solo per file vecchio tracciato (quindi non STD e IGMG e IGMR)
          val xml = XML.load(metadata.file.getPath)
          val newFilePath = destFolderName + metadata.file.getName
          List(metadata.copy(
            xmlNode = xml
            , outputFilePath = newFilePath
            , statusCode = STATUS_0
            , statusMessage = Constants.OK
            , statusType = PHASE_C))

        case "zip" =>

          val zipFile = new ZipFile(metadata.file) // in caso di mancanza di permessi e/o corruzione del file zip qui scatta l'eccezione

          // controllo di correttezza della struttura degli zip STD e IGMG
          if (metadata.trackType.equals(STD) || metadata.trackType.equals(IGMG)) {
            if ((zipFile.entries.asScala.toList.length != 1) || zipFile.entries.asScala.toList.head.isDirectory ||
              !zipFile.entries.asScala.toList.head.getName.toLowerCase.endsWith(".xml")) {
              return List(metadata.copy(
                statusCode = STATUS_4
                , statusMessage = "Struttura file non rispettata."
                , statusType = PHASE_C
                , fileError = FILE_XML_NOT_PRESENT))
            }

            val zipFileName = metadata.file.getName
            val xmlFileName = zipFile.entries.asScala.toList.head.getName
            // controllo che dentro lo zip ci sia un unico file xml con nomenclatura uguale allo zip a meno di estensioni
            if (!zipFileName.replaceAll("(?i)\\.xml\\.zip", "").replaceAll("(?i)\\.zip", "")
              .equals(xmlFileName.replaceAll("(?i)\\.xml", ""))) {
              return List(metadata.copy(
                statusCode = STATUS_4
                , statusMessage = "Struttura file non rispettata."
                , statusType = PHASE_C
                , fileError = ZIP_XML_NAMING_UNMATCH))
            }

            if (xmlFileName.endsWith(".xml.xml")) {
              return List(metadata.copy(
                statusCode = STATUS_4
                , statusMessage = "Struttura file non rispettata."
                , statusType = PHASE_C
                , fileError = ZIP_XML_NAMING_UNMATCH))
            }
          }

          if (metadata.trackType.equals(IGMR)) {
            if ((zipFile.entries.asScala.toList.length != 1) || zipFile.entries.asScala.toList.head.isDirectory) {
              return List(metadata.copy(
                statusCode = STATUS_4
                , statusMessage = "Struttura file non rispettata."
                , statusType = PHASE_C
                , fileError = FILE_XML_NOT_PRESENT))
            }

            if (zipFile.entries.asScala.toList.head.isDirectory || !zipFile.entries.asScala.toList.head.getName.toLowerCase.endsWith(".xml")) {
              return List(metadata.copy(
                statusCode = STATUS_4
                , statusMessage = "Struttura file non rispettata."
                , statusType = PHASE_C
                , fileError = FILE_NOT_XML))
            }

            val zipFileName = metadata.file.getName
            val xmlFileName = zipFile.entries.asScala.toList.head.getName
            // controllo che dentro lo zip ci sia un unico file xml con nomenclatura uguale allo zip a meno di estensioni
            if (!zipFileName.replaceAll("(?i)\\.xml\\.zip", "").replaceAll("(?i)\\.zip", "")
              .equals(xmlFileName.replaceAll("(?i)\\.xml", ""))) {
              return List(metadata.copy(
                statusCode = STATUS_4
                , statusMessage = "Struttura file non rispettata."
                , statusType = PHASE_C
                , fileError = ZIP_XML_NAMING_UNMATCH))
            }

            if (xmlFileName.endsWith(".xml.xml")) {
              return List(metadata.copy(
                statusCode = STATUS_4
                , statusMessage = "Struttura file non rispettata."
                , statusType = PHASE_C
                , fileError = ZIP_XML_NAMING_UNMATCH))
            }
          }

          // per ogni entry dentro il file zip:
          zipFile.entries.asScala.toList.map(entry => {
            if (entry.getName.toLowerCase.endsWith(".xml") && !entry.getName.contains("__MACOSX")
              && !entry.isDirectory) {

              val entryName = entry.getName.split("/").last // utile alla gestione della presenza di cartelle dentro al file zip

              // se tracciato STD o IGMG o IGMR prosegui, se analizziamo un flusso di vecchio tracciato estraiamo solo gli xml con nomenclatura conforme al vecchio formato
              // questo controllo permette di non estrarre xml del tracciato STD dentro a file zip di vecchio tracciato
              if (metadata.trackType.equals(STD) || metadata.trackType.equals(IGMG) || metadata.trackType.equals(IGMR)
                || FileUtility.gasOldRegex.pattern.matcher(entryName).matches
                || FileUtility.gasOldPrestazionaliRegex.pattern.matcher(entryName).matches) {

                val newFilePath = destFolderName + entryName
                try {
                  val xml = XML.load(zipFile.getInputStream(entry))
                  metadata.copy(
                    xmlNode = xml
                    , outputFilePath = newFilePath
                    , statusCode = STATUS_0
                    , statusMessage = Constants.OK
                    , statusType = PHASE_U)
                } catch {
                  case exception: SAXParseException =>
                    metadata.copy(
                      outputFilePath = entryName // loggare nome dell'xml corrotto dentro a file zip in report_decompressione
                      , statusCode = STATUS_6
                      , statusMessage = s"eccezione nella decompressione/copia: parsing XML fallito (${exception.getMessage})"
                      , statusType = PHASE_E
                      , fileError = CORRUPTED_XML)
                  case exception: Exception =>
                    // logger.warn(s"file ${metadata.file.getAbsolutePath} error", exception)
                    metadata.copy(
                      statusCode = STATUS_7
                      , statusMessage = s"eccezione nella decompressione/copia: errore generico ${exception.getMessage}"
                      , statusType = PHASE_E
                      , fileError = GENERIC_ERROR)
                }
              } else {
                metadata.copy(
                  outputFilePath = entryName
                  , statusCode = STATUS_4
                  , statusMessage = "Il file xml non è stato estratto poiché non è coerente al tipo di tracciato."
                  , statusType = PHASE_C)
              }
            } else {
              metadata.copy(
                outputFilePath = entry.getName
                , statusCode = STATUS_3
                , statusMessage = "Il file non è stato estratto poiché non è un xml."
                , statusType = PHASE_C)
            }
          })

        case _ => List(metadata.copy(
          statusCode = STATUS_3
          , statusMessage = s"il file non è un xml/zip"
          , statusType = PHASE_C
        ))
      }

    } catch {
      case exception: FileNotFoundException => // problemi di permessi in lettura dei file vengono catturati qui
        List(metadata.copy(
          statusCode = STATUS_2
          , statusMessage = s"eccezione nella decompressione/copia: impossibile leggere il file (${exception.getMessage})"
          , statusType = PHASE_E
          , fileError = FILE_READ_EXCEPTION))

      case exception: ZipException =>
        List(metadata.copy(
          statusCode = STATUS_5
          , statusMessage = s"eccezione nella decompressione/copia: apertura ZIP fallita (${exception.getMessage})"
          , statusType = PHASE_E
          , fileError = CORRUPTED_ZIP))

      case exception: SAXParseException =>
        List(metadata.copy(
          statusCode = STATUS_6
          , statusMessage = s"eccezione nella decompressione/copia: parsing XML fallito (${exception.getMessage})"
          , statusType = PHASE_E
          , fileError = CORRUPTED_XML))

      case exception: Exception =>
        List(metadata.copy(
          statusCode = STATUS_7
          , statusMessage = s"eccezione nella decompressione/copia: errore generico ${exception.getMessage}"
          , statusType = PHASE_E
          , fileError = GENERIC_ERROR))
    }
  }

  def writeXml(rdd: RDD[GasUnzipMetadata]): Unit = {
    rdd
      .foreach(fileWithMeta => {
        new File(fileWithMeta.outputFilePath).getParentFile.mkdirs()
        XML.save(fileWithMeta.outputFilePath, fileWithMeta.xmlNode, enc = "UTF-8", xmlDecl = true)
      })
  }

  def writeLog(rdd: RDD[GasUnzipMetadata], unzipTimestamp: Timestamp): Unit = {
    //import sqlContext.implicits._
    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._
    rdd.map(fileWithMeta => (
      fileWithMeta.statusCode,
      fileWithMeta.statusMessage,
      fileWithMeta.file.getPath,
      fileWithMeta.outputFilePath,
      fileWithMeta.anno + fileWithMeta.mese + fileWithMeta.giorno,
      fileWithMeta.statusType,
      unzipTimestamp,
      fileWithMeta.anno + fileWithMeta.mese
    )).toDF(
      LogSchema.getValues: _*
    ).write //.partitionBy(LogSchema.annomese) /* COMMENTED IN ORDER TO FIX THIS: insertInto() can't be used together with partitionBy().
      // Partition columns have already be defined for the table. It is not necessary to use partitionBy() */
      .insertInto(PropertyUtility.getUnzipLogTable)
  }

  /***
   * Aggiorna i dati per gli zip caricati che risultano già decompressi e presenti all'interno della tabella [[PropertyUtility.getUnzipLogTable]]
   */
  def getTodayAlreadyTrasmittedFiles(rddUnzipMeta: RDD[GasUnzipMetadata]): RDD[(String, Boolean, GasUnzipMetadata)] = {
    val rdd = rddUnzipMeta
      .map(meta => (
        meta.file.getName.replaceAll("(?i)\\.xml\\.zip", "")
          .replaceAll("(?i)\\.zip", ""), meta)
      ) //.partitionBy(new HashPartitioner(rddUnzipMeta.partitions.length))
    //    logger.info(rdd.map(x=> (x._1,1) ).reduceByKey(_+_).sortBy(_._2, false).take(10).toList)
    val reportDecompressione = Environment.getSpark.sqlContext.sql(s"SELECT filename_folder_dest, descrizione, annomese FROM ${PropertyUtility.getUnzipLogTable} WHERE descrizione ='OK' AND annomese >= '202101' AND (upper(filename_folder_dest) RLIKE '^[A-Z0-9_/]*_(M|R)\\.XML$$' OR filename_folder_dest LIKE '%IGMG%' OR filename_folder_dest LIKE '%IGMR%')").rdd
      .coalesce(rdd.partitions.length)
      .repartition(rdd.partitions.length)
      .map(row => (row.getAs[String]("filename_folder_dest").split(File.separatorChar).last.replaceAll("(?i)\\.xml", ""), row.getAs[String]("descrizione")))
      .reduceByKey({ case (ok1, ok2) => ok1 }) // .reduceByKey(new HashPartitioner(rddUnzipMeta.partitions.length), (ok1: String, ok2: String) => ok1)
      .rightOuterJoin(rdd)
    reportDecompressione.map({ case (fileName, (desc, meta)) => (fileName, desc.isDefined, meta) })
  }

  def getZipFiles(rootFolder: File, params: UnzipArgsConfig): RDD[File] = {
    val startDate = params.fromDate.get
    val endDate = params.toDate.getOrElse(startDate)
    logger.warn(f"getZipFiles from $startDate to $endDate")

    // /TMG_DISTR
    val distributori = rootFolder.listFiles.toList.filter(_.isDirectory)

    // /TMG_DISTR/DISTRIBUTORE/TMG_DISTR_UTENTE
    val sottesi = distributori
      .flatMap(_.listFiles().filter(elementName => elementName.isDirectory && elementName.getName.equals("DISTRIBUTORE")))
      .flatMap(_.listFiles().filter(_.isDirectory))

    val filesNumber = sottesi.length / 100 + 1
    val defParalTwice = Environment.getSpark.sparkContext.defaultParallelism * 4
    val paral = if (filesNumber > defParalTwice) filesNumber else defParalTwice

    val sottesiRdd = Environment.getSpark.sparkContext.parallelize(sottesi, paral)

    val unzipFolders = sottesiRdd
      .flatMap(_.listFiles().filter(f => f.isDirectory && FileUtility.isFolderBetweenYears(f, startDate, endDate)))
      .flatMap(_.listFiles().filter(f => f.isDirectory && FileUtility.isFolderBetweenDates(f, startDate, endDate)))
    unzipFolders.flatMap(_.listFiles().filter(f => f.isFile))
  }

  def getFilesToRecover(recoveryCsvPath: String): RDD[File] = {
    val zipFiles = Environment.getSpark
      .read
      .option("header", "true")
      .schema(CsvRecoverySchema.schema)
      .csv(recoveryCsvPath)
      .distinct
      .rdd
      .map(row => row.getAs[String](CsvRecoverySchema.filename_src))
      .map(fileName => new File(fileName))

    zipFiles.filter(_.lastModified() != 0) //this ensures the file exists
  }

  /** *
   * Ritorna la lista dei file che sono andati in errore con codice 002 (errore di permesso accesso al file) nei calcoli precedenti
   * trovati all'interno della tabella [[PropertyUtility.getUnzipLogTable]]
   */
  def getCodice002FilesToRecover(params: UnzipArgsConfig): RDD[File] = {
    val leadColName: String = "lead_col"
    val fileDate: String = "file_date"
    val partitioningColumnFilter = if (params.toDate.isDefined)
      s"${LogSchema.annomese.toString} >= '" + params.fromDate.get.format(DateTimeFormatter.ofPattern("yyyyMM")) + "' AND " +
        s"${LogSchema.annomese.toString} <= '" + params.toDate.get.format(DateTimeFormatter.ofPattern("yyyyMM")) + "'"
    else
      s"${LogSchema.annomese.toString} = '" + params.fromDate.get.format(DateTimeFormatter.ofPattern("yyyyMM")) + "'"

    val reportDecompressioneDF = Environment.getSpark.sqlContext.table(PropertyUtility.getUnzipLogTable)
    val windowSpec = Window.partitionBy(LogSchema.filename_src).orderBy(LogSchema.dataelaborazione)
    val reportDecompressioneLeadDF = reportDecompressioneDF
      .filter(reportDecompressioneDF.col(LogSchema.annomese) >= "202101")
      .filter(partitioningColumnFilter)
      .withColumn(leadColName, lead(LogSchema.dataelaborazione, 1).over(windowSpec))
      .withColumn(fileDate, from_unixtime(unix_timestamp(reportDecompressioneDF.col(LogSchema.annomesegiornodir).cast(StringType), ANNOMESEGIORNO_PATTERN)))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val startDate = params.fromDate.get.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val endDate = if (params.toDate.isDefined) params.toDate.get.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    else params.fromDate.get.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    reportDecompressioneLeadDF.where(
      reportDecompressioneLeadDF.col(leadColName).isNull &&
        reportDecompressioneLeadDF.col(LogSchema.codice) === "002" &&
        to_date(reportDecompressioneLeadDF.col(fileDate)).between(startDate, endDate)
    ).select(LogSchema.filename_src)
      .rdd
      .map(_.getAs[String](LogSchema.filename_src))
      .map(filename => new File(filename))
  }

  def cleanOldTmpFolder(destRootPathOld: String): Unit = {
    val tempOldPath = new File(destRootPathOld)

    if (tempOldPath.exists() && tempOldPath.listFiles().length > 0) {
      val oldTmpFoldersClientsRdd = Environment.getSpark.sparkContext.parallelize(tempOldPath.listFiles().flatMap(_.listFiles()).flatMap(_.listFiles())) // parallelizzo sui clienti per migliori performance
      oldTmpFoldersClientsRdd.foreach(basePath => FileUtils.deleteDirectory(basePath))
      Environment.getSpark.sparkContext.parallelize(tempOldPath.listFiles()).foreach(basePath => FileUtils.deleteDirectory(basePath))
    }
  }

  def cleanOldTmpReportsFolder(destReportPathTemp: String): Unit = {
    val tempReportPath = new File(destReportPathTemp)

    if (tempReportPath.exists() && tempReportPath.listFiles().length > 0) {
      val tempReportsFoldersRdd = Environment.getSpark.sparkContext.parallelize(tempReportPath.listFiles())
      tempReportsFoldersRdd.foreach(basePath => FileUtils.deleteDirectory(basePath))
    }
  }

  //  def extractRealCodFlusso(codFlusso: String) : String = {
  //
  //    for (flusso <- flusso2List ) { // fondamentale controllare prima flussi di rettifica e dopo quelli di misura (per presenza sottostringhe uguali es. V01 e V01R)
  //      if (codFlusso.toLowerCase.contains(flusso))
  //        return flusso.toUpperCase
  //    }
  //    for (flusso <- flusso1List ) {
  //      if (codFlusso.toLowerCase.contains(flusso))
  //        return flusso.toUpperCase
  //    }
  //    for (flusso <- flussoOldList ) {
  //      if (codFlusso.toLowerCase.contains(flusso))
  //        return flusso.toUpperCase
  //    }
  //
  //    codFlusso
  //  }

}


