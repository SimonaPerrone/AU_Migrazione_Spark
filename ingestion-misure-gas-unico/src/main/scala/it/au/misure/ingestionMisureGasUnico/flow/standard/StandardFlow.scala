package it.au.misure.ingestionMisureGasUnico.flow.standard

import java.io.File
import it.au.misure.ingestionMisureGasUnico.flow.Flow
import it.au.misure.ingestionMisureGasUnico.model.GasXmlMetadata
import it.au.misure.ingestionMisureGasUnico.model.schema.{CommonColumnsSchema, IngestionReportSchema}
import it.au.misure.ingestionMisureGasUnico.model.schema.ammissibilita.AmmissibilitaPDRSchema._
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.TracciatoStandardEnum.TracciatoStandardEnum
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.MisuraXMLSchema
import it.au.misure.ingestionMisureGasUnico.model.validate.ReportEsitoPDRMessage
import it.au.misure.ingestionMisureGasUnico.utility.PropertyUtility
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import it.au.misure.ingestionMisureGasUnico.validate.CheckAmmissibilitaPDRRules
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel

import scala.xml.XML

trait StandardFlow extends Flow {
  val tS: TracciatoStandardEnum
  val renamedColumns: Map[String, String]
  override val flowType: String = "Standard"
  override val flowDataPath: String = s"$rootPath/$flowType/$flowName"
  override val ammissPath: String = PropertyUtility.getAmmissibilitaStandardPath

  override def run(unzipTimestamp: String)/*(implicit sc: SparkContext, SQLContext: SQLContext)*/: Unit = {
    cleanOldTmpReportFiles(flowName) // fondamentale che la pulizia sia eseguita prima del prossimo if
    if (checkInputPathFiles > 0) {
      val inputRdd = loadData
      val (validationInfoRdd, validatedRdd) = validate(inputRdd)
      val inputDf = parse(validatedRdd)
      val inputDfRenamed = renameColumns(inputDf)
      val inputDfWithCommon = addCommonColumns(inputDfRenamed, unzipTimestamp)
      val fullDf = addNullColumns(inputDfWithCommon)
      fullDf.persist(StorageLevel.MEMORY_AND_DISK)
      write(fullDf)
      writeReport(fullDf)
      writeValidationReports(validationInfoRdd, unzipTimestamp)
      deleteData()
    } else {
      logger.warn(s"$flowDataPath does not have XML files.")
    }
  }

  def renameColumns(df: DataFrame): DataFrame = {
    renamedColumns.foldLeft(df) {
      (df, mapEntry) => df.withColumnRenamed(mapEntry._1, mapEntry._2)
    }
  }

  def addNullColumns(df: DataFrame): DataFrame = {
    schema.getValues.toSet[String]
      .filterNot(df.columns.toSet[String])
      .foldLeft(df)((df, columnName) =>
        df.withColumn(columnName, lit(null).cast(StringType)))
  }

  def validate(inputRdd: RDD[GasXmlMetadata])/*(implicit sc: SparkContext, sqlContext: SQLContext)*/: (RDD[(GasXmlMetadata, List[ReportEsitoPDRMessage])], RDD[GasXmlMetadata]) = {
    val checkAmm = Environment.getSpark.sparkContext.broadcast(new CheckAmmissibilitaPDRRules)

    val pdrRDD = inputRdd.flatMap(gasXmlMetada => {
      (gasXmlMetada.xmlNode \\ MisuraXMLSchema.FlussoMisure \\ MisuraXMLSchema.DatiPdr).toList.map(pdr => (gasXmlMetada.copy(xmlNode = null),pdr)) // mettiamo xmlNode = null così lo shuffle successivo deve scambiare meno dati che non sono necessari
    }).repartition(inputRdd.partitions.length)

    val xmlWithMessages = pdrRDD.map({case (meta,pdr)=>
     ( meta.file.getPath,( meta,checkAmm.value.check(pdr, meta)) )
    }).groupByKey()
      .map({case (filename,iterable)=>(iterable.toList.head._1,iterable.toList.map(_._2))})
      .map({case (meta, messageList)=> (meta.copy(xmlNode = XML.loadFile(meta.file)),messageList)})
      .persist(StorageLevel.MEMORY_AND_DISK)

    val outputRddData = xmlWithMessages.map({ case (gasXmlMetada, messages) =>
      val ammissibilitaMap = messages.map(message => (message.pdr, message.bloccante)).toMap
      //System.err.println(s"File: ${gasXmlMetada.file.getName}\t\tammissibilitaMap:  $ammissibilitaMap")
      gasXmlMetada.copy(ammissibilita = ammissibilitaMap)
    })

    (xmlWithMessages, outputRddData)
  }

  def writeValidationReports(xmlWithMessages: RDD[(GasXmlMetadata, List[ReportEsitoPDRMessage])], unzipTimestamp:String)/*(implicit sc: SparkContext, sqlContext: SQLContext)*/: Unit = {
    val xmlWithMessagesNewLogic = getReportMessagesNewLogic(xmlWithMessages)

    writeAmmissibilitaReportsHive(xmlWithMessagesNewLogic, unzipTimestamp)
    writeAmmissibilitaReportsCsv(xmlWithMessagesNewLogic)
  }
}
