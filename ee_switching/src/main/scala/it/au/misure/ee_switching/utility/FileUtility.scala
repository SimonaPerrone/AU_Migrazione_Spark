package it.au.misure.ee_switching.utility

import java.io.{BufferedInputStream, File, FileInputStream, FileOutputStream}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.{ZipEntry, ZipOutputStream}
import it.au.misure.ee_switching.model.schema.hive.ReportEntry
import it.au.misure.ee_switching.model.schema.xml.FileXml
import it.au.misure.ee_switching.utility.Constants.FILENAME_TIMESTAMP_PATTERN
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.xml.{Elem, XML}
import Control._
import it.au.misure.ee_switching.utility.environment.Environment

import scala.io.Source

object FileUtility {

  def getPIvaDistributore(filename: String) : String = filename.split("_")(0)
  def getPIvaUtente(filename: String) : String =  filename.split("_")(1)
  def getAnnoMeseSW(filename: String): String = filename.split("_")(2)
  def getCodFlusso(filename: String) : String =  filename.split("_")(3)
  def getTimestamp(filename: String) : String =  filename.split("_")(4)
  def getCodContrDisp(filename: String) : String =  filename.split("_").last.substring(filename.split("_").last.indexOf("DP"), filename.split("_").last.length - 4) // 4: lunghezza ".xml"/".zip"

  def cleanTmpFolder(flowName: String): Unit = {
    val sc = Environment.getSpark.sparkContext
    val sqlContext = Environment.getSpark.sqlContext

    val xmlTmpFolder = new File(s"${PropertyUtility.getXmlTmpRootPath}/${flowName}")

    if (xmlTmpFolder.exists() && xmlTmpFolder.listFiles().length > 0) {
      val flowsXmlTmpFolders = sc.parallelize(xmlTmpFolder.listFiles()) // parallelizzazione sui tipi di flusso
      flowsXmlTmpFolders.foreach(path => FileUtils.deleteDirectory(path))
      sc.parallelize(xmlTmpFolder.listFiles()).foreach(path => FileUtils.deleteDirectory(path))
    }
  }

  def writeXmlFiles(rdd: RDD[(String, List[String], String)], flowName: String): RDD[FileXml] = {
    rdd.map( { case (chunkName, podsList, chunkData) => {
      val xmlElem = XML.loadString(FlowUtility.addCommonInfo(chunkData, FileUtility.getPIvaDistributore(chunkName),
        FileUtility.getPIvaUtente(chunkName), FileUtility.getCodFlusso(chunkName), FileUtility.getCodContrDisp(chunkName)))
      val codFlusso = FileUtility.getCodFlusso(chunkName)
      val tmpFilePath = s"${PropertyUtility.getXmlTmpRootPath}/${flowName}/${codFlusso}/${chunkName}"

      FileUtility.writeXml(xmlElem, tmpFilePath)
      FileXml(chunkName, new File(tmpFilePath), podsList, outputZipPath = FileUtility.getZipOutputFilePath(chunkName, flowName))
    } })
  }

  def writeXml(xmlElem: Elem, filePath: String): Unit = {
    new File(filePath).getParentFile.mkdirs()
    val printer = new scala.xml.PrettyPrinter(1000000, 2)
    XML.save(filePath, XML.loadString(printer.format(xmlElem)), enc="UTF-8", xmlDecl = true)
  }

  def writeZipFiles(xmlChunksRdd: RDD[FileXml]): RDD[ReportEntry] = {
    xmlChunksRdd.flatMap(xmlChunk => {
      FileUtility.zipFile(xmlChunk.file, xmlChunk.outputZipPath)

      val chunkName = xmlChunk.file.getName
      xmlChunk.podsList.map(pod => ReportEntry(FileUtility.getPIvaDistributore(chunkName), FileUtility.getPIvaUtente(chunkName),
        pod, FileUtility.getCodFlusso(chunkName), xmlChunk.outputZipPath, xmlChunk.errorListXSD,
        Timestamp.valueOf(LocalDateTime.parse(FileUtility.getTimestamp(chunkName), DateTimeFormatter.ofPattern(FILENAME_TIMESTAMP_PATTERN))),
        FileUtility.getAnnoMeseSW(chunkName)))
    })
  }

  def getZipOutputFilePath(chunkName: String, flowName: String): String =
    s"${PropertyUtility.getzipOutputFileRootPath}/output_${flowName}_${FileUtility.getTimestamp(chunkName)}/${FileUtility.getCodFlusso(chunkName)}/${chunkName.replace(".xml", ".zip")}"

  def zipFile(inputFile: File, outputZipPath: String): Unit = {
    new File(outputZipPath).getParentFile.mkdirs()

    val Buffer = 2 * 1024
    val data = new Array[Byte](Buffer)
    val zip = new ZipOutputStream(new FileOutputStream(outputZipPath))
    zip.putNextEntry(new ZipEntry(inputFile.getName))
    val in = new BufferedInputStream(new FileInputStream(inputFile.getPath), Buffer)
    var b = in.read(data, 0, Buffer)
    while (b != -1) {
      zip.write(data, 0, b)
      b = in.read(data, 0, Buffer)
    }
    in.close()
    zip.closeEntry()
    zip.close()
  }

  def readTextFile(filename: String): Option[List[String]] = {
    try {
      val lines = using(Source.fromFile(filename)) { source =>
        (for (line <- source.getLines) yield line).toList
      }
      Some(lines)
    } catch {
      case e: Exception => None
    }
  }
}
