package it.eng.au.pubblicazioneIndennizzi.utility

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment

import java.io.{File, FileWriter, PrintWriter}
import scala.language.postfixOps
import scala.util.Try

object FileUtility {
  def setYarn777toTmpFolder(path: String = Properties.getIsilonBasepathTmp + "/tmp/"): Unit = {
    // horrible mode to use user YARN on driver
    Environment.sparkContext.parallelize(List(1), 1).foreach(_ => {
      import sys.process._
      Try(s"chmod 777 -R $path" !)
    })
  }

  def create777File(path: String): File = {
    val f = new File(path)
    f.setReadable(true, false)
    f.setExecutable(true, false)
    f.setWritable(true, false)
    f
  }

  def set777(file: File): Unit = {
    Try(file.setExecutable(true, false))
    Try(file.setWritable(true, false))
    Try(file.setReadable(true, false))
  }

  def writeCsv(path: String, header: Option[String], content: List[String], byteLimit: Option[Long] = None, appendMode: Boolean = false): Unit = {
    var fileOut = new File(path)

    if (!fileOut.getParentFile.exists()) fileOut.getParentFile.mkdirs()
    set777(fileOut)
    set777(fileOut.getParentFile) // /MESE
    set777(fileOut.getParentFile.getParentFile) // ANNO/MESE
    set777(fileOut.getParentFile.getParentFile.getParentFile) // BASENAME_PIVA/ANNO/MESE
    set777(fileOut.getParentFile.getParentFile.getParentFile.getParentFile) // OPEATIONNAME/BASENAME_PIVA/ANNO/MESE
    set777(fileOut.getParentFile.getParentFile.getParentFile.getParentFile.getParentFile) // FLOWBASENAME/OPEATIONNAME/BASENAME_PIVA/ANNO/MESE
    set777(fileOut.getParentFile.getParentFile.getParentFile.getParentFile.getParentFile.getParentFile) // CIG/FLOWBASENAME/OPEATIONNAME/BASENAME_PIVA/ANNO/MESE

    var pw = new PrintWriter(new FileWriter(fileOut, appendMode))
    if (fileOut.length() == 0 && header.isDefined) pw.write(header.get + "\n")

    content.zipWithIndex.foreach({ case (row, index) =>
      pw.write(row + "\n")
      // every 1000 rows => flush; for local test modify from 1000 in 2
      if (index != 0 && index % 1000 == 0) {
        pw.flush()
      }
    })

    pw.flush()
    pw.close()
  }
}

