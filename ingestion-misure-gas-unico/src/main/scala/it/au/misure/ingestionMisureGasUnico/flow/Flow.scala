package it.au.misure.ingestionMisureGasUnico.flow

import it.au.misure.ingestionMisureGasUnico.model._
import it.au.misure.ingestionMisureGasUnico.model.schema.CommonColumnsSchema._
import it.au.misure.ingestionMisureGasUnico.model.schema.ammissibilita.AmmissibilitaPDRSchema
import it.au.misure.ingestionMisureGasUnico.model.schema.{CommonColumnsSchema, IngestionReportSchema, SchemaEnum}
import it.au.misure.ingestionMisureGasUnico.model.validate.ReportEsitoPDRMessage
import it.au.misure.ingestionMisureGasUnico.utility.{Constants, FileUtility, PropertyUtility}
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import java.io.File
import java.nio.file.NoSuchFileException
import it.au.misure.ingestionMisureGasUnico.model.schema.ammissibilita.AmmissibilitaPDRSchema.{cartella_cloud, codice_inamissibilita, d_caricamento, nome_file}

trait Flow {
  val rootPath: String = PropertyUtility.getTmpOutputFolder

  val flowDataPath: String

  val hiveDatabaseName: String = PropertyUtility.getCmgGasDb

  val schema: SchemaEnum

  val hiveTableName: String

  val flowType: String

  def flowName: String

  val partitioningColumns: List[String]

  val ammissPath: String

  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def run(unzipTimestamp: String)/*(implicit sc: SparkContext, sqlContext: SQLContext)*/: Unit

  def checkInputPathFiles: Int = {
    val inputFile = new File(rootPath)
    if (!inputFile.exists || !inputFile.isDirectory || !inputFile.canExecute) {
      throw new NoSuchFileException(rootPath, null, s"$rootPath does not exist, is not a directory or it cannot be accessed.")
    }

    val srcFile = new File(flowDataPath)
    if (!srcFile.exists || !srcFile.isDirectory || !srcFile.canExecute) {
      0
    } else {
      srcFile.listFiles().filter(_.isDirectory)
        .flatMap(dist => dist.listFiles().filter(_.isDirectory))
        .flatMap(dist => dist.listFiles().filter(_.isDirectory))
        .flatMap(dist => dist.listFiles().filter(_.isFile))
        .length
    }
  }

  def loadData(/*implicit sc: SparkContext*/): RDD[GasXmlMetadata] = {

    val files = new File(flowDataPath)
      .listFiles().filter(_.isDirectory)
      .flatMap(dist => dist.listFiles().filter(_.isDirectory))
      .flatMap(dist => dist.listFiles().filter(_.isDirectory))
      .flatMap(dist => dist.listFiles().filter(_.isFile))
    logger.info(s"Loading files from $flowDataPath")

    val filesNumber = files.length / 100 + 1
    val defParalTwice = Environment.getSpark.sparkContext.defaultParallelism * 2
    val paral = if (filesNumber > defParalTwice) filesNumber else defParalTwice
    Environment.getSpark.sparkContext.parallelize(files, paral)
      .map(textData => FileUtility.xmlToMetadata(textData))
  }

  def deleteData(): Unit = {
    FileUtils.deleteDirectory(new File(flowDataPath))
  }

  def parse(inputRdd: RDD[GasXmlMetadata]): DataFrame

  def addCommonColumns(df: DataFrame, unzipTimestamp: String): DataFrame = {
    // t_name_file created by parse()
    df
      .withColumn(n_id, md5(df(t_name_file)))
      .withColumn(n_id_file, md5(df(local_file)))
      .withColumn(annomese_riferimento, concat(df(anno_riferimento), df(mese_riferimento)))
      .withColumn(d_caricamento, lit(unzipTimestamp))
      .withColumn(dataelaborazione, lit(unzipTimestamp))
  }

  def write(df: DataFrame): Unit = {
    logger.info(s"Writing df to $hiveDatabaseName.$hiveTableName")
    df
      .selectExpr(schema.getValues: _*)
      .write
      .mode(SaveMode.Append)
      //.partitionBy(partitioningColumns: _*) --> /* COMMENTED IN ORDER TO FIX THIS: insertInto() can't be used together with partitionBy().
      // Partition columns have already be defined for the table. It is not necessary to use partitionBy() */
      .insertInto(s"$hiveDatabaseName.$hiveTableName")
    logger.info(s"The df was appended to $hiveDatabaseName.$hiveTableName")
  }

  def writeReport(df: DataFrame): Unit = {
    val reportTable = PropertyUtility.getFlowLogTable
    val reportDf = df
      .withColumn(CommonColumnsSchema.flow_name, lit(flowName).cast(StringType))
      .withColumn(CommonColumnsSchema.annomese, concat(df(CommonColumnsSchema.anno), df(CommonColumnsSchema.mese))) // sostituzione del valore di annomese con annomese caricamento cartella cloud
      .withColumnRenamed(CommonColumnsSchema.anno, IngestionReportSchema.t_anno_caricamento)
      .withColumnRenamed(CommonColumnsSchema.mese, IngestionReportSchema.t_mese_caricamento)
      .withColumnRenamed(CommonColumnsSchema.giorno, IngestionReportSchema.t_giorno_caricamento)
      .selectExpr(IngestionReportSchema.getValues:_*)
      .dropDuplicates()

    logger.info(s"Writing report to $reportTable")
    reportDf
      .write
      .mode(SaveMode.Append)
      //.partitionBy(CommonColumnsSchema.annomese) --> /* COMMENTED IN ORDER TO FIX THIS: insertInto() can't be used together with partitionBy().
      //      // Partition columns have already be defined for the table. It is not necessary to use partitionBy() */
      //
      .insertInto(reportTable)
    logger.info(s"The report was appendend to $reportTable")
  }

  def writeAmmissibilitaReportsHive(xmlWithMessages: RDD[(GasXmlMetadata, List[ReportEsitoPDRMessage])], unzipTimestamp:String)/*(implicit sc: SparkContext, sqlContext: SQLContext)*/: Unit = {
    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._
    xmlWithMessages.flatMap({ case (gasXmlMetada, messages) =>
      messages.map(_.copy(flusso = gasXmlMetada.flusso, anno = gasXmlMetada.anno, mese = gasXmlMetada.mese, giorno = gasXmlMetada.giorno))
    }).toDF
      .withColumnRenamed("cartellaCloud", cartella_cloud)
      .withColumnRenamed("nomeFile", nome_file)
      .withColumnRenamed("codiceInamissibilita", codice_inamissibilita)
      .withColumn(d_caricamento, lit(unzipTimestamp))
      .selectExpr(AmmissibilitaPDRSchema.getValues: _*)
      .write//.partitionBy(anno, mese, giorno) --> /* COMMENTED IN ORDER TO FIX THIS: insertInto() can't be used together with partitionBy().
      // Partition columns have already be defined for the table. It is not necessary to use partitionBy() */
      .insertInto(PropertyUtility.getAmmissibilitaPdrLogTable)
  }

  def writeAmmissibilitaReportsCsv(xmlWithMessages: RDD[(GasXmlMetadata, List[ReportEsitoPDRMessage])]): Unit = {
    val path = ammissPath
    xmlWithMessages.foreach({ case (gasXmlMetada, messages) =>
      val not_ammissible_messages = messages.filter(_.ammissibilita.equals("N"))
      if(not_ammissible_messages.nonEmpty) {
        val inputFile = new File(PropertyUtility.getUnzipInputPath + "/" + gasXmlMetada.originalRelativePath)
        val outputFolder = FileUtility.getAmmissibilitaOutputFolder(inputFile.getParentFile.getPath, PropertyUtility.getUnzipInputPath, path)
        val outputPathFile = s"$outputFolder/ReportEsitoPDR_${inputFile.getName.replace(".zip", "")}.txt"
        FileUtility.writeCsv(outputPathFile, ReportEsitoPDRMessage.header, not_ammissible_messages.map(_.toStringRow), Some(PropertyUtility.getAmmissibilitaPdrFileMaxLength))
      }
    })
  }

  def getReportMessagesNewLogic(rdd: RDD[(GasXmlMetadata, List[ReportEsitoPDRMessage])]): RDD[(GasXmlMetadata, List[ReportEsitoPDRMessage])] = {
    rdd.map({ case (meta, messageList) =>
      val newMessages = messageList.map(message => {
        val ammissibile = if(message.bloccante.equals(Constants.BLOCCANTE) || message.bloccante.equals(Constants.NON_BLOCCANTE)) "N" else "S"
        val bloccante = if(message.bloccante.equals(Constants.BLOCCANTE) ) "S" else "N"
        message.copy(cartellaCloud = meta.originalFolder, bloccante = bloccante, ammissibilita = ammissibile)
      })
      (meta, newMessages)
    })
  }

  def cleanOldTmpReportFiles(flowName: String)/*(implicit sc: SparkContext)*/ : Unit = {
    val tmpReportFilesFolder = new File(ammissPath)

    if (tmpReportFilesFolder.exists() && tmpReportFilesFolder.listFiles().length > 0) {
      val oldReportFiles = tmpReportFilesFolder
        .listFiles().filter(_.isDirectory)
        .flatMap(_.listFiles().filter(_.isDirectory))
        .flatMap(_.listFiles().filter(_.isDirectory))
        .flatMap(_.listFiles().filter(_.isDirectory))
        .flatMap(_.listFiles().filter(_.isDirectory))
        .flatMap(_.listFiles().filter(elementName =>
          elementName.isFile &&
            elementName.getName.startsWith("ReportEsitoPDR_") &&
            elementName.getName.contains("_" + flowName + "_") && // fondamentale mettere gli _ (per evitare match report misura/rettifica es. V01/V01R)
            elementName.getName.endsWith(".txt")))

      Environment.getSpark.sparkContext.parallelize(oldReportFiles).foreach(oldReportFileToRemove => oldReportFileToRemove.delete())
    }
  }
}
