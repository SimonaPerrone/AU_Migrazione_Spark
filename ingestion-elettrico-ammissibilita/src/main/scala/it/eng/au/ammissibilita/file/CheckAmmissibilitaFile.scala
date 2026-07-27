package it.eng.au.ammissibilita.file

import it.eng.au.ammissibilita.{CheckAmmissibilitaEE, CheckAmmissibilitaRules}
import it.eng.au.args.AmmissibilitaParameters
import it.eng.au.model.{ReportEsitoFILEMessage, XMLMetadata}
import it.eng.au.schema._
import it.eng.au.schema.ammissibilita.AmmissibilitaFileSchema
import it.eng.au.schema.ammissibilita.AmmissibilitaFileSchema._
import it.eng.au.schema.report_decompressione.ReportDecompressioneSchema
import it.eng.au.utility.Constants.OK
import it.eng.au.utility._
import it.eng.au.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, lit, when}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.storage.StorageLevel

import java.io.File
import java.time.LocalDateTime
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory
import scala.collection.SortedSet
import scala.util.Try
import scala.xml.XML

object CheckAmmissibilitaFile extends CheckAmmissibilitaEE {
  def run(args: Array[String]): (RDD[XMLMetadata], DataFrame, DataFrame) = {
    val params = parseAmmissibilitaArgs(args)

    val sc = Environment.getSpark.sparkContext
    val (inputPath: String, outputPath: String) = getInputOutput(params)

    log.info(s"args: ${args.mkString(";")}")
    log.info(s"Reading from: $inputPath")
    log.info(s"Writing into: $outputPath")

    val rddXmlFolderDays = getRDDDaysFolder(inputPath, params)

    val fileNameRegexToUse = if (params.isSmis) filenameRegexSMIS else filenameRegex //if is a smis flow then it use smis regex else it use original regex
    val fileWithMatches = rddXmlFolderDays.flatMap(xmlFolder => {
      xmlFolder.listFiles().toList
    }).map(
      file => (file, fileNameRegexToUse.findFirstMatchIn(file.getName))
    ).filter({ case (_, matches) => matches.isDefined })

    //    val filesMapBC = if (params.isSmis) null else null //sc.broadcast( getFolderFilesMap(rddXmlFolderDays) )

    val rcuAziendaPDF = ReaderUtility.readAndPrepareRcuAzienda
      .persist(StorageLevel.MEMORY_AND_DISK)
    val rcuUddPDF = ReaderUtility.readAndPrepareRcuUddP
      .persist(StorageLevel.MEMORY_AND_DISK)

    val (mapPivaRcu, mapCodDPRcuPivaUdd) = getRCUMaps(rcuAziendaPDF, rcuUddPDF, params)
    val mapPivaRcuBC = sc.broadcast(mapPivaRcu)
    val mapCodDPRcuPivaUddBC = sc.broadcast(mapCodDPRcuPivaUdd)
    val listPivaRcuDistr = if (params.isSmis) sc.broadcast(getListPivaRcuDistr(rcuAziendaPDF)) else null
    val listPivaRcuEmt = if (params.isSmis) sc.broadcast(getListPivaRcuEmt(rcuAziendaPDF)) else null

    if (!params.isSmis) rcuAziendaPDF.unpersist()

    val rddWithMeta = fileWithMatches.repartition(fileWithMatches.partitions.length)
      .map({ case (file, matches) =>
        //if is a smis flow then it create smis XMLMetadata else it create original XMLMetadata
        if (params.isSmis) {
          getXMLMetadataSMIS(file, matches, params).copy(
            flusso1XSDBroad = null,
            flusso2XSDBroad = null,
            flussoSMISXSDBroad = null,
            mapFileNames = null,
            mapCodDPRcuPivaUdd = mapCodDPRcuPivaUddBC,
            mapPivaRcu = mapPivaRcuBC,
            listPivaRcuDistr = listPivaRcuDistr,
            listPivaRcuEmt = listPivaRcuEmt
          )
        }
        else {
          getXMLMetadata(file, matches, params).copy(
            flusso1XSDBroad = null,
            flusso2XSDBroad = null,
            //            mapFileNames = filesMapBC,
            mapCodDPRcuPivaUdd = mapCodDPRcuPivaUddBC,
            mapPivaRcu = mapPivaRcuBC
          )
        }
      }
      )

    val rddWithAlreadyTransmittedMeta = getAlreadyTransmittedFiles(rddWithMeta, params)
      .map({ case (fileName, alreadyTransmitted, meta) => meta.copy(alreadyTransmitted = alreadyTransmitted) })
      .mapPartitions(metas => { //add validator after shuffles ti avoid serialization issues
        val factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
        //if is a smis flow then it create smis XMLMetadata else it create original XMLMetadata
        if (params.isSmis) {
          val flussoSMISXSDBroad = factory.newSchema(new StreamSource(PropertyUtility.getXsdSMISPath)).newValidator()
          metas.map(meta => meta.copy(flussoSMISXSDBroad = flussoSMISXSDBroad))
        }
        else {
          val flusso1XSDBroad = factory.newSchema(new StreamSource(PropertyUtility.getXsdPeriodicoPath)).newValidator()
          val flusso2XSDBroad = factory.newSchema(new StreamSource(PropertyUtility.getXsdRettificaPath)).newValidator()
          metas.map(meta => meta.copy(flusso1XSDBroad = flusso1XSDBroad, flusso2XSDBroad = flusso2XSDBroad))
        }
      })

    // rddWithAlreadyTransmittedMeta.persist(StorageLevel.MEMORY_AND_DISK_SER)
    // log.info(s"count di rddWithAlreadyTransmittedMeta: " + rddWithAlreadyTransmittedMeta.count())

    val fileWithMessages = rddWithAlreadyTransmittedMeta.map(fileXmlWithMeta => {
      //if is a smis flow then it use smis ammissibilita else it use original ammissibilita
      val checker: CheckAmmissibilitaRules[ReportEsitoFILEMessage] = if (params.isSmis) CheckAmmissibilitaFileRulesSMIS else CheckAmmissibilitaFileRules
      val file = Try(XML.loadFile(fileXmlWithMeta.file))
      val message = if (file.isFailure)
        getExceptionMessage(fileXmlWithMeta)
      else
        checker.check(file.get, fileXmlWithMeta).copy(flusso = fileXmlWithMeta.flusso)
      //      val tryMessage = checker.check(file, fileXmlWithMeta).copy(flusso = fileXmlWithMeta.flusso)
      //      val message = tryMessage.getOrElse(getExceptionMessage(fileXmlWithMeta))
      (fileXmlWithMeta, message)
    }).map({ case (meta, message) => (meta, getMessageWithNewLogic(message).asInstanceOf[ReportEsitoFILEMessage]) })
      .persist(StorageLevel.MEMORY_AND_DISK)

    /* val dataMisuraDateTime = Try(DateTime.parse("17/06/2021", DateTimeFormat.forPattern("dd/MM/yyyy"))).getOrElse(DateTime.parse("31/12/9999", DateTimeFormat.forPattern("dd/MM/yyyy")))
    val originalFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val dataMisuraTest = LocalDate.parse(dataMisuraDateTime.toString("yyyy-MM-dd"), originalFormat)

    log.info(s"count di fileWithMessages dataMisuraIsNull: " + fileWithMessages.filter(f => f._1.dataMisura == null).count())
    log.info(s"count di fileWithMessages checkDataMisura: " + fileWithMessages.filter(f => f._1.dataMisura == dataMisuraTest).count())
    log.info(s"count di fileWithMessages podIsEmpty: " + fileWithMessages.filter(f => f._1.pod != null && f._1.pod.isEmpty).count())
    log.info(s"count di fileWithMessages podIsNull: " + fileWithMessages.filter(f => f._1.pod == null).count())
    log.info(s"count di fileWithMessages checkPod: " + fileWithMessages.filter(f => f._1.pod == "IT026E00004835").count()) */

    fileWithMessages.map({ case (xmlMeta, message) => (xmlMeta.file.getParent, message) }).groupByKey.foreach({ case (xmlFolder, messages) =>
      val outFolder: String = getAmmissibilitaOutputFolder(new File(xmlFolder), inputPath, outputPath)
      //if is a smis flow then it use smis output file else it use original output file
      val outputPathFile = if (params.isSmis) s"$outFolder/ReportAmmissibilitàFileSMIS.txt" else s"$outFolder/ReportAmmissibilitàFileEE.txt"
      FileUtility.writeCsv(outputPathFile, ReportEsitoFILEMessage.header, messages.toList.map(_.toStringRow), appendMode = true)
    })


    val messagesRDD = fileWithMessages.map({ case (xmlMeta, message) => message.copy(anno = xmlMeta.params.year, mese = xmlMeta.params.month, giorno = xmlMeta.params.day) })
    writeOnHive(messagesRDD)

    (fileWithMessages.filter({ case (xmlMeta, message) => message.ammissibilita.equalsIgnoreCase(Constants.YES) })
      .map({ case (xmlMeta, message) => xmlMeta }),
      rcuAziendaPDF,
      rcuUddPDF
    )
  }

  def writeOnHive(rdd: RDD[ReportEsitoFILEMessage]): Unit = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    import SQLContext.implicits._
    val codice_inammissibilita = "codiceInamissibilita"
    val replaceEmptyStringWithNullExpr = when(col(codice_inammissibilita) =!= lit(""), col(codice_inammissibilita))
    rdd.toDF
      .withColumn(codice_inammissibilita, replaceEmptyStringWithNullExpr)
      .withColumn(d_caricamento, current_timestamp)
      .withColumnRenamed("cartellaCloud", AmmissibilitaFileSchema.cartella_cloud)
      .withColumnRenamed("nomeFile", AmmissibilitaFileSchema.nome_file)
      .withColumnRenamed("codiceInamissibilita", AmmissibilitaFileSchema.codice_inamissibilita)
      .repartition(10)
      .selectExpr(AmmissibilitaFileSchema.getValues: _*)
      .write
      //.partitionBy(anno, mese, giorno)  --> /* COMMENTED IN ORDER TO FIX THIS: insertInto() can't be used together with partitionBy().
      //      //      // Partition columns have already be defined for the table. It is not necessary to use partitionBy() */
      //      //
      .insertInto(PropertyUtility.getAmmissibilitaFileLogTable)
  }

  def getRCUMaps(rcuAziendaPDF: DataFrame, rcuUddPDF: DataFrame, params: AmmissibilitaParameters): (Map[String, (LocalDateTime, LocalDateTime)], Map[String, Set[String]]) = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val joinRcuUddAzienda = rcuUddPDF.join(rcuAziendaPDF, rcuUddPDF(RcuUddPSchema.n_id_udd) === rcuAziendaPDF(RcuAziendaPSchema.n_id_azienda))
      .select(RcuAziendaPSchema.t_piva, RcuUddPSchema.d_inizio, RcuUddPSchema.d_fine, RcuUddPSchema.t_codice_terna)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val mapPivaRcu: Map[String, (LocalDateTime, LocalDateTime)] = joinRcuUddAzienda.select(RcuAziendaPSchema.t_piva, RcuUddPSchema.d_inizio, RcuUddPSchema.d_fine)
      .rdd.map(row =>
      (row.getAs[String](RcuAziendaPSchema.t_piva),
        (DateTimeUtility.getDateTimeOr(row.getAs[String](RcuUddPSchema.d_inizio), "yyyy-MM-dd HH:mm:ss.S", "min"),
          DateTimeUtility.getDateTimeOr(row.getAs[String](RcuUddPSchema.d_fine), "yyyy-MM-dd HH:mm:ss.S", "max"))
      )
    ).collectAsMap().toMap

    val mapCodDPRcuPivaUdd: Map[String, Set[String]] = joinRcuUddAzienda.select(RcuUddPSchema.t_codice_terna, RcuAziendaPSchema.t_piva)
      .rdd.map(row =>
      (row.getAs[String](RcuUddPSchema.t_codice_terna), row.getAs[String](RcuAziendaPSchema.t_piva))
    ).groupByKey()
      .map({ case (codDp, pivaIterable) => (codDp, pivaIterable.toSet) })
      .collectAsMap().toMap

    joinRcuUddAzienda.unpersist()

    (mapPivaRcu, mapCodDPRcuPivaUdd)
  }

  def getFolderFilesMap(rdd: RDD[File]): Map[String, Map[String, Set[String]]] = {
    val fileRdd = rdd.flatMap(xmlFolder => {
      xmlFolder.listFiles().toList
    }).filter(_.getName.takeRight(3).toLowerCase.equalsIgnoreCase("xml"))

    fileRdd.map(file => {

      val baseFolder = file.getParent
      // 1. get file format ignoring case
      val rawFormat = file.getName.takeRight(3)

      // 2. Transform filename into canonicalFileName.
      //    2.1 get raw file name
      //    canonicalFileName means file name with extension to lower case: fileA.XML --to canonical--> fileA.xml
      val (canonicalFileName, rawFileName) = (file.getName.replace(rawFormat, "xml"), file.getName)

      ((baseFolder, canonicalFileName), rawFileName) //baseflorer,canonicalFileName identifies only
    }).groupByKey()
      .map({ case ((basefolder, canonicalName), rowNamesIter) => (basefolder, (canonicalName, SortedSet(rowNamesIter.toList: _*).toSet)) }) //Sorted set required since in rules we are going to use .head that requires an ordered collection
      .groupByKey()
      .map(tpl => (tpl._1, tpl._2.toMap))
      .collectAsMap().toMap
  }

  def getAlreadyTransmittedFiles(rddMeta: RDD[XMLMetadata], params: AmmissibilitaParameters): RDD[(String, Boolean, XMLMetadata)] = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val rdd = rddMeta.map(meta => (meta.file.getName, meta))
    val today = params.year + params.month + params.day
    val reportDecompressione = SQLContext.table(PropertyUtility.getUnzipReportTable)
    reportDecompressione.select(ReportDecompressioneSchema.filename_folder_dest, ReportDecompressioneSchema.descrizione, ReportDecompressioneSchema.annomesegiornodir)
      .where(col(ReportDecompressioneSchema.descrizione) === OK)
      .where(col(ReportDecompressioneSchema.annomesegiornodir) < lit(today).cast(IntegerType))
      .rdd
      .map(row => (row.getAs[String](ReportDecompressioneSchema.filename_folder_dest).split(File.separatorChar).last, row.getAs[String](ReportDecompressioneSchema.descrizione)))
      .reduceByKey({ case (ok1, ok2) => ok1 })
      .rightOuterJoin(rdd)
      .map({ case (fileName, (desc, meta)) => (fileName, desc.isDefined, meta) })
  }


  def getExceptionMessage(meta: XMLMetadata): ReportEsitoFILEMessage = {
    ReportEsitoFILEMessage(cartellaCloud = meta.file.getParent
      , nomeFile = meta.file.getName
      , flusso = meta.flusso
      , ammissibilita = Constants.FILE
      , bloccante = Constants.BLOCCANTE
      , codiceInamissibilita = Constants.COD904
      , descrizione = Constants.ERROR_FILE_STRUCTURE
    )
  }


  def unionRcusUddP(rcuAziendaPDF: DataFrame, joinRcuUddAzienda: DataFrame): DataFrame = {
    val rcusUddP = ReaderUtility.readAndPrepareRcusUddP

    val rcusAziendaPDF = rcusUddP.join(rcuAziendaPDF, rcusUddP(RcusUddPSchema.n_id_udd) === rcuAziendaPDF(RcuAziendaPSchema.n_id_azienda))
      .select(RcuAziendaPSchema.t_piva, RcusUddPSchema.d_inizio, RcusUddPSchema.d_fine, RcusUddPSchema.t_codice_terna)

    joinRcuUddAzienda.select(RcuAziendaPSchema.t_piva, RcuUddPSchema.d_inizio, RcuUddPSchema.d_fine, RcuUddPSchema.t_codice_terna)
      .union(rcusAziendaPDF).coalesce(joinRcuUddAzienda.rdd.getNumPartitions)
  }

  def getListPivaRcuDistr(rcuAziendaPDF: DataFrame): List[String] = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val rcuDistrP = ReaderUtility.readAndPrepareRcuDistrP

    val joinRcuDistPAzienda = rcuDistrP.join(rcuAziendaPDF, rcuDistrP(RcuDistrPSchema.n_id_distr) === rcuAziendaPDF(RcuAziendaPSchema.n_id_azienda))
      .select(RcuAziendaPSchema.t_piva)

    joinRcuDistPAzienda
      .rdd.map(row =>
      row.getAs[String](RcuAziendaPSchema.t_piva)
    ).collect().toList
  }

  def getListPivaRcuEmt(rcuAziendaPDF: DataFrame): List[String] = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext

    val rcuEmtP = ReaderUtility.readAndPrepareRcuEmtP

    val joinRcuDistPAzienda = rcuEmtP.join(rcuAziendaPDF,
      rcuEmtP(RcuEmtPSchema.n_id_emt) === rcuAziendaPDF(RcuAziendaPSchema.n_id_azienda) and
        (rcuAziendaPDF(RcuAziendaPSchema.t_piva).notEqual("05877611003") or rcuEmtP(RcuEmtPSchema.n_id_emt).notEqual("1383")) //filter is used by ID13 file rules smis
    )
      .select(RcuAziendaPSchema.t_piva)

    joinRcuDistPAzienda
      .rdd.map(row =>
      row.getAs[String](RcuAziendaPSchema.t_piva)
    ).collect().toList
  }

 /* def recoverPodAndDataMisura(xml: Elem, fileXmlWithMeta: XMLMetadata): XMLMetadata = {
    val ITALIAN_DATE_PATTERN = "dd/MM/yyyy"
    val DB_DATE_PATTERN = "yyyy-MM-dd"
    val pod = (xml \\ FlussoMisure \\ DatiPod \\ Pod).text

    val dataMisuraDateTime = Try(DateTime.parse((xml \\ FlussoMisure \\ DatiPod \\ Montaggio \\ DataMisura).text, DateTimeFormat.forPattern(ITALIAN_DATE_PATTERN))).getOrElse(DateTime.parse("31/12/9999", DateTimeFormat.forPattern(ITALIAN_DATE_PATTERN)))
    val originalFormat = DateTimeFormatter.ofPattern(DB_DATE_PATTERN)
    val dataMisura = LocalDate.parse(dataMisuraDateTime.toString(DB_DATE_PATTERN), originalFormat)

    fileXmlWithMeta.copy(pod = pod, dataMisura = dataMisura)
  }*/
}
