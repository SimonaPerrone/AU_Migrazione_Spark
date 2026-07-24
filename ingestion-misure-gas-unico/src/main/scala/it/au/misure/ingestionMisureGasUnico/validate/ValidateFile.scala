package it.au.misure.ingestionMisureGasUnico.validate

import it.au.misure.ingestionMisureGasUnico.model.GasUnzipMetadata
import it.au.misure.ingestionMisureGasUnico.model.schema.ammissibilita.AmmissibilitaFileSchema
import it.au.misure.ingestionMisureGasUnico.model.schema.ammissibilita.AmmissibilitaFileSchema._
import it.au.misure.ingestionMisureGasUnico.model.schema.rcu.{RcuAziendaPSchema, RcuGasUDDPSchema}
import it.au.misure.ingestionMisureGasUnico.model.validate.ReportEsitoFILEMessage
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import it.au.misure.ingestionMisureGasUnico.utility.{Constants, DateTimeUtility, FileUtility, PropertyUtility}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.storage.StorageLevel

import java.io.File
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory
import scala.collection.SortedSet

trait ValidateFile extends Serializable {

  val rcuAziendaPTableName: String

  val rcuGasUDDPTableName: String

  val ammissibilitaLogTableName: String

  val csvReportFileName: String

  val ammissibilitaFolder: String

  val checkAmm: CheckAmmissibilitaRules[ReportEsitoFILEMessage]


  def validate(rdd: RDD[GasUnzipMetadata], unzipTimestamp: Timestamp, isFileRecoveryEnabled: Boolean) /*(implicit sc: SparkContext, sqlContext: SQLContext)*/ : RDD[GasUnzipMetadata] = {
    val uDDActivePeriodsMap = getUDDActivePeriodsMap

    val fileWithReport = calcReportMessages(rdd, uDDActivePeriodsMap).persist(StorageLevel.MEMORY_AND_DISK)

    val fileWithReportUsingNewLogic = getReportMessagesNewLogic(fileWithReport)
    writeAmmissibilitaReportsCsv(fileWithReportUsingNewLogic, unzipTimestamp, isFileRecoveryEnabled)
    writeAmmissibilitaReportsHive(fileWithReportUsingNewLogic, unzipTimestamp)

    fileWithReport.map({ case (gasXmlMetada, message) => gasXmlMetada.copy(ammissibile = message.bloccante != Constants.BLOCCANTE) })
  }

  def getUDDActivePeriodsMap /*(implicit sc: SparkContext, sqlContext: SQLContext)*/ : Map[String, (LocalDateTime, LocalDateTime)] = {
    val rcuAziendaPDF = Environment.getSpark.sqlContext.table(rcuAziendaPTableName)
    val rcuGasUDDPDF = Environment.getSpark.sqlContext.table(rcuGasUDDPTableName)

    val res = rcuAziendaPDF.join(rcuGasUDDPDF, rcuAziendaPDF(RcuAziendaPSchema.n_id_azienda) === rcuGasUDDPDF(RcuGasUDDPSchema.n_id_azienda))
      .select(
        rcuAziendaPDF(RcuAziendaPSchema.t_piva),
        rcuGasUDDPDF(RcuGasUDDPSchema.d_data_inizio),
        rcuGasUDDPDF(RcuGasUDDPSchema.d_data_fine)
      )

    res.rdd.map(row => (
      row.getAs[String](RcuAziendaPSchema.t_piva),
      (
        DateTimeUtility.getDateTimeOr(row.getAs[String](RcuGasUDDPSchema.d_data_inizio), "yyyy-MM-dd HH:mm:ss.S", "min"),
        DateTimeUtility.getDateTimeOr(row.getAs[String](RcuGasUDDPSchema.d_data_fine), "yyyy-MM-dd HH:mm:ss.S", "max")
      )
    )).collectAsMap().toMap
  }

  def calcReportMessages(rdd: RDD[GasUnzipMetadata], uDDActivePeriodsMap: Map[String, (LocalDateTime, LocalDateTime)]) /*(implicit sc: SparkContext)*/ : RDD[(GasUnzipMetadata, ReportEsitoFILEMessage)] = {
    //    val mapFilesName = sc.broadcast(getFolderFilesMap(rdd).collectAsMap().toMap)
    val mapFilesName = null
    val uDDActivePeriodsMapBroad = Environment.getSpark.sparkContext.broadcast(uDDActivePeriodsMap)
    val checkAmmBC = Environment.getSpark.sparkContext.broadcast(checkAmm)


    rdd.mapPartitions(filesWithMeta => {

      val factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
      val flusso1XSD = factory.newSchema(new StreamSource(PropertyUtility.getXsdPeriodicoPath)).newValidator()
      val flusso2XSD = factory.newSchema(new StreamSource(PropertyUtility.getXsdRettificaPath)).newValidator()
      val flussoIGMGXSD = factory.newSchema(new StreamSource(PropertyUtility.getXsdIgmgPath)).newValidator()
      val flussoIGMRXSD = factory.newSchema(new StreamSource(PropertyUtility.getXsdIgmrPath)).newValidator()


      filesWithMeta.map(fileWithMeta => {
        val externalInfo = fileWithMeta.externalInfo.copy(
          flusso1XSD = flusso1XSD,
          flusso2XSD = flusso2XSD,
          flussoIGMGXSD = flussoIGMGXSD,
          flussoIGMRXSD = flussoIGMRXSD,
          mapFilesName = mapFilesName,
          uDDActivePeriodsMap = uDDActivePeriodsMapBroad
        )

        val message = checkAmmBC.value.check(fileWithMeta.xmlNode, fileWithMeta.copy(externalInfo = externalInfo))
        (fileWithMeta, message)
      })
    })
  }

  def writeAmmissibilitaReportsHive(fileWithReport: RDD[(GasUnzipMetadata, ReportEsitoFILEMessage)], unzipTimestamp: Timestamp) /*(implicit sc: SparkContext, sqlContext: SQLContext)*/ : Unit = {
    //import sqlContext.implicits._
    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._
    fileWithReport.map({ case (gasXmlMetada, message) =>
      message.copy(flusso = gasXmlMetada.flusso, anno = gasXmlMetada.anno, mese = gasXmlMetada.mese, giorno = gasXmlMetada.giorno)
    }).toDF
      .withColumnRenamed("cartellaCloud", cartella_cloud)
      .withColumnRenamed("nomeFile", nome_file)
      .withColumnRenamed("codiceInamissibilita", codice_inamissibilita)
      .withColumn(d_caricamento, lit(unzipTimestamp))
      .selectExpr(AmmissibilitaFileSchema.getValues: _*)
      .write //.partitionBy(anno, mese, giorno)  --> /* COMMENTED IN ORDER TO FIX THIS: insertInto() can't be used together with partitionBy().
      // Partition columns have already be defined for the table. It is not necessary to use partitionBy() */
      .insertInto(ammissibilitaLogTableName)
  }

  def writeAmmissibilitaReportsCsv(fileWithReport: RDD[(GasUnzipMetadata, ReportEsitoFILEMessage)], unzipTimestamp: Timestamp, isFileRecoveryEnabled: Boolean): Unit = {
    fileWithReport
      .map({ case (fileWithMeta, message) => (fileWithMeta.file.getParent, message) })
      .groupByKey().foreach({ case (folder, messages) =>
      val outputFolder = FileUtility.getAmmissibilitaOutputFolder(folder, PropertyUtility.getUnzipInputPath, ammissibilitaFolder)

      val csvReportName = if (isFileRecoveryEnabled)
        csvReportFileName.replace(".txt", s"_${unzipTimestamp.toLocalDateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))}.txt")
      else csvReportFileName

      val outputPathFile = s"$outputFolder/$csvReportName"
      FileUtility.writeCsv(outputPathFile, ReportEsitoFILEMessage.header, messages.toList.map(_.toStringRow))
    })
  }

  def getFolderFilesMap(rdd: RDD[GasUnzipMetadata]): RDD[(String, Map[String, Set[String]])] = {

    rdd.map(fileMetadata => {

      val baseFolder = fileMetadata.file.getParent
      // 1. get file format ignoring case
      val rawFormat = fileMetadata.file.getName.takeRight(3)

      // 2. Transform filename into canonicalFileName.
      //    2.1 get raw file name
      //    canonicalFileName means file name with extension to lower case: fileA.XML --to canonical--> fileA.xml
      val (canonicalFileName, rawFileName) = rawFormat.toLowerCase match {
        case "xml" => (fileMetadata.file.getName.replace(rawFormat, "xml"), fileMetadata.file.getName)
        case "zip" => val xmlFile = new File(fileMetadata.outputFilePath)
          val xmlExt = xmlFile.getName.takeRight(3)
          (xmlFile.getName.replace(xmlExt, "xml"), fileMetadata.file.getName)
      }

      ((baseFolder, canonicalFileName), rawFileName)
    }).groupByKey()
      .map({ case ((basefolder, canonicalName), rowNamesIter) => (basefolder, (canonicalName, SortedSet(rowNamesIter.toList: _*).toSet)) }) //Sorted set required since in rules we are going to use .head that requires an ordered collection
      .groupByKey()
      .map(tpl => (tpl._1, tpl._2.toMap))
  }

  def getReportMessagesNewLogic(rdd: RDD[(GasUnzipMetadata, ReportEsitoFILEMessage)]): RDD[(GasUnzipMetadata, ReportEsitoFILEMessage)] = {
    rdd.map({ case (meta, message) =>
      val ammissibile = if (message.bloccante.equals(Constants.BLOCCANTE) || message.bloccante.equals(Constants.NON_BLOCCANTE)) "N" else "S"
      val bloccante = if (message.bloccante.equals(Constants.BLOCCANTE)) "S" else "N"
      val pathArray = message.cartellaCloud.split(File.separatorChar)
      val nuovaCartellaCloud = pathArray.slice(pathArray.length - 5, pathArray.length).mkString(File.separator)
      (meta, message.copy(cartellaCloud = nuovaCartellaCloud, bloccante = bloccante, ammissibilita = ammissibile))
    })
  }
}
