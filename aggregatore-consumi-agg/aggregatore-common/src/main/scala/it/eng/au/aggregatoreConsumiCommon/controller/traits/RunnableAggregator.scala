package it.eng.au.aggregatoreConsumiCommon.controller.traits

import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, InfoOutputSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.{Environment, FileUtility}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.{BufferedInputStream, File, FileInputStream, FileOutputStream}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.immutable.ListMap
import scala.sys.process._
import scala.util.Try

@deprecated("Use RunnableAggregatorPerfomance")
trait RunnableAggregator extends RunnableAggregatorPerfomanceOld {
  val separator = ";"

  val keyFields: List[String]
  val aggregatoColumns: ListMap[String, String]
  val baseNumber: String
  val operationName: String

  override val writeCsvHeader: Boolean = true

  override def run(df: DataFrame): Unit = {
    val dfAggregato = getAggregato(df)
    val fields = getCsvFields(dfAggregato)
    write(dfAggregato, fields)
    val rddInfo = mergeAndZipFiles(fields)
    writeInfoInTable(rddInfo)
  }

  def getCsvFields(dfAggregato: DataFrame): List[String] = {
    dfAggregato.columns.toList.diff(List(DailyConsumptionAggSchema.annoMese.toString))
  }

  def getAggregato(df: DataFrame): DataFrame

  def getCsvOutputPath(pathAnnoMese: String, baseName: String, keyFields: List[String], operationName: String, sessionName: String, date: LocalDateTime): String

  def write(df: DataFrame, fields: List[String]): Unit = {
    val tmpHdfsOutput = getTmpHdfsOutput

    Environment.fs.delete(new Path(tmpHdfsOutput), true)
    Environment.fs.deleteOnExit(new Path(tmpHdfsOutput))
    val partitionedCols = keyFields :+ DailyConsumptionAggSchema.annoMese.toString
    df.select(df.columns.map(c => col(c).cast(StringType)): _*)
      .na.fill("")
      .select(partitionedCols.map(col) :+ concat_ws(separator, fields.map(col): _*): _*)
      .repartition(partitionedCols.map(col): _*)
      .write.partitionBy(partitionedCols: _*).mode(SaveMode.Overwrite).text(tmpHdfsOutput)
  }

  override def writeInfoInTable(rdd: RDD[(String, String, String, String, Timestamp, Long)]): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    rdd
      .toDF(InfoOutputSchema.getValues: _*)
      .repartition(10)
      .write.partitionBy(InfoOutputSchema.partition_date).mode(SaveMode.Append).parquet(getHdfsOutputBasepathInfoLog)
  }

  def mergeAndZipFiles(fields: List[String]): RDD[(String, String, String, String, Timestamp, Long)] = {
    val tmpHdfsOutput =  getTmpHdfsOutput
    val tmpCsvOutput = getTmpCsvOutput
    val pathZipOutput = getPathZipOutput
    val listPivaFolder = Environment.fs.listStatus(new Path(tmpHdfsOutput)).filter(_.isDirectory)
    val localBasePath = tmpCsvOutput
    val keyFieldsVal = keyFields
    val pathFileOutputVal = pathZipOutput
    val operationNameVal = operationName
    val sessionNameVal = getSessionName
    val maxDimensionZipFile = getMaxDimensionZipFile
    val maxNumRowFile = getCsvMaxRowLength.get
    val maxSizeThresholdZip = getMaxSizeThresholdZip.toLong
    val year = getYear
    val baseName = getPublicationType + baseNumber

    val today = LocalDateTime.now()
    val executionId = Environment.getDailyConsumptionExecutionid
    val timestampRun = Timestamp.valueOf(getDateToRun)

    FileUtility.setYarn777toTmpFolder(tmpCsvOutput)
    FileUtils.deleteDirectory(new File(localBasePath))

    val rdd = Environment.sparkContext.parallelize(listPivaFolder.map(_.getPath.toString), Environment.sparkContext.defaultParallelism)
    val rddInfo = rdd.flatMap(pivaFolder => {
      val tmpCsvFolder: String = localCopyMerge(pivaFolder, localBasePath, keyFieldsVal, baseName, operationNameVal, sessionNameVal, fields, today)
      val outputFolder = new File(tmpCsvFolder.replaceAll(localBasePath, pathFileOutputVal)).getParentFile

      var zipName: String = ""
      var numFile = 0

      if (outputFolder.exists()) {
        zipName = getZipOutputName(pivaFolder, keyFieldsVal, sessionNameVal, year, today)
        val tmpOutputFolder = new File(tmpCsvFolder).getParentFile
        zipFolder(tmpCsvFolder, tmpOutputFolder.getPath, zipName)
        numFile = splitBigFileZip(tmpCsvFolder, tmpOutputFolder.getPath, zipName, maxDimensionZipFile, maxNumRowFile, maxSizeThresholdZip)
        moveZipInOutputFolder(tmpCsvFolder, outputFolder.getPath)
      }
      else {
        zipName = s"Couldn't write to $outputFolder, the path does not exits"
        logger.warn(s"Couldn't write to $outputFolder, the path does not exits.")
      }

      val result = (1 to numFile).toList.map { num =>
        (executionId, operationNameVal, baseName, outputFolder.getPath + zipName.replace("_1.zip", s"_$num.zip"), timestampRun, timestampRun.getTime)
      }
      //      FileUtils.deleteDirectory(new File(tmpCsvFolder).getParentFile)
      result
    })

    FileUtility.setYarn777toTmpFolder(tmpCsvOutput)

    rddInfo
  }

  def getZipOutputName(pivaFolder: String, keyFieldsVal: List[String], sessionName: String, year: String, today: LocalDateTime): String

  def zipFolder(tmpCsvFolder: String, outputFolder: String, zipName: String): Unit = {
    //    new File(outputFolder).mkdirs()
    val zip = new ZipOutputStream(new FileOutputStream(FileUtility.create777File(outputFolder + zipName)))

    new File(tmpCsvFolder).getParentFile.listFiles().filter(value => value.getName.substring(value.getName.length - 4).equals(".csv")).foreach { name =>
      putIntoZip(zip, name)
    }
    zip.close()
  }

  override def putIntoZip(zip: ZipOutputStream, name: File): Unit = {
    zip.putNextEntry(new ZipEntry(name.getName))
    val in = new BufferedInputStream(new FileInputStream(name.getPath))
    var b = in.read()
    while (b > -1) {
      zip.write(b)
      b = in.read()
    }
    in.close()
    zip.closeEntry()
  }

  def splitBigFileZip(tmpCsvFolder: String, outputFolder: String, zipName: String, maxDimensionZipFile: Long, maxNumRowFile: Long, maxSizeThresholdZip: Long): Int = {

    var count = 1
    //    println("tmpCsvFolder: " + tmpCsvFolder)
    new File(tmpCsvFolder).getParentFile.listFiles().filter(value => value.getName.substring(value.getName.length - 4).equals(".zip")).foreach { inputFileZip =>
      val dimensionFile = inputFileZip.length()
      //      println("name zip: " + inputFileZip.getName)
      //      println("dimension file: " + dimensionFile)
      if (dimensionFile >= maxDimensionZipFile) {
        //        inputFileZip.delete()
        val zipFileOld = new File(inputFileZip.getPath.replace("_1.zip", "_old.zip"))
        inputFileZip.renameTo(zipFileOld)
        zipFileOld.delete()
        var zip = new ZipOutputStream(new FileOutputStream(FileUtility.create777File(outputFolder + zipName)))
        zipFileOld.getParentFile.listFiles().filter(value => value.getName.substring(value.getName.length - 4).equals(".csv")).foreach(inputFileCsv => {

          val inputPath = inputFileCsv.getPath
          val newNameFile = new File(inputPath.replace("_1.csv", ".csv"))

          inputFileCsv.renameTo(newNameFile)
          //          inputFileCsv.delete()
          val temporaneo = "temporaneo"
          val inputNewPath = newNameFile.getPath
          val outputNewPath = inputNewPath.replace(".csv", "_" + temporaneo + "_")
          Try(s"split -l $maxNumRowFile -a1 --numeric-suffixes=1 --additional-suffix=.csv $inputNewPath $outputNewPath" !)

          val nameStringContains = newNameFile.getName.replace(".csv", "_" + temporaneo + "_")
          inputFileCsv.getParentFile.listFiles().filter(_.getName.contains(nameStringContains)).foreach(splitFileCsv => {
            val outputPathName = splitFileCsv.getPath.replace("_" + temporaneo, "")
            val outputFile = new File(outputPathName)
            //Only first file (_temporaneo_1.csv) the header is present and then just to rename file
            if (splitFileCsv.getName.contains("_" + temporaneo + "_1.csv")) splitFileCsv.renameTo(outputFile)
            else {
              if (writeCsvHeader) {
                Try(s"head -1 $newNameFile " #> outputFile !)
                Try(s"cat $splitFileCsv " #>> outputFile !)
                splitFileCsv.delete()
              } else splitFileCsv.renameTo(outputFile)
            }

            val readZip = new File(outputFolder + zipName.replace("_1.zip", "_" + count + ".zip"))
            val dimensionZipFile = readZip.length()
            //            println("count: "+count)
            //            println("dimension zip file: "+ dimensionZipFile)
            //            println("name zip: "+ readZip.getName)
            //            println("csv name: "+ outputPathName)
            if (dimensionZipFile < maxSizeThresholdZip) putIntoZip(zip, outputFile)
            else {
              zip.close()
              count += 1
              zip = new ZipOutputStream(new FileOutputStream(FileUtility.create777File(outputFolder + zipName.replace("_1.zip", "_" + count + ".zip"))))
              putIntoZip(zip, outputFile)
            }
          })
          newNameFile.delete()
        })
        zip.close()
      }
    }
    count
  }

  def moveZipInOutputFolder(tmpCsvFolder: String, outputFolder: String): Unit = {
    new File(outputFolder).mkdirs()
    val outputFileZip = new File(outputFolder)
    new File(tmpCsvFolder).getParentFile.listFiles().filter(value => value.getName.substring(value.getName.length - 4).equals(".zip") && !value.getName.substring(value.getName.length - 8).equals("_old.zip")).foreach { inputFileZip =>
      FileUtils.copyFileToDirectory(inputFileZip, outputFileZip)
    }
  }

  def localCopyMerge(pivaFolder: String, localBasePath: String, keyFieldsVal: List[String], baseName: String, operationNameVal: String, sessionNameVal: String, fields: List[String], today: LocalDateTime): String = {
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    val localFs = FileSystem.getLocal(conf)
    val header = fields.mkString(separator)

    var listFolders: List[String] = List(pivaFolder)
    keyFieldsVal.foreach(_ => listFolders = listFolders.flatMap(folder => fs.listStatus(new Path(folder)).toList.filter(_.isDirectory).map(_.getPath.toString)))

    val tmpCsvFolder = listFolders
      .map(dayFolder => {
        //create a file with only header
        if (writeCsvHeader) {
          val f = fs.create(new Path(dayFolder + "/header.csv"))
          f.write((header + "\n").getBytes)
          f.close()
        }

        val outputRelativePath = localBasePath + getCsvOutputPath(dayFolder, baseName, keyFieldsVal, operationNameVal, sessionNameVal, today)
        localFs.delete(new Path(outputRelativePath), true)
        FileUtil.copyMerge(fs, new Path(dayFolder), localFs, new Path(outputRelativePath), false, conf, "")

        //        import sys.process._
        //        Try(s"chmod 777 -R $localBasePath" !)

        outputRelativePath
      }).head

    tmpCsvFolder
  }

  override val keyPiva1: String = "" //throw new Exception("not supported")
  override val keyPiva2: String = "" //throw new Exception("not supported")
  override val header: String = "" //throw new Exception("not supported")
  override val csvFields: List[String] = List() //throw new Exception("not supported")
}

