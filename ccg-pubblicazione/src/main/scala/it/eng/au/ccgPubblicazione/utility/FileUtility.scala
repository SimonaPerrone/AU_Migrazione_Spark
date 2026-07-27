package it.eng.au.ccgPubblicazione.utility

import org.apache.spark.sql.DataFrame

import java.io.{BufferedInputStream, File, FileInputStream, FileWriter, PrintWriter}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.util.Try

object FileUtility {
  def setYarn777toTmpFolder(path: String = Environment.getIsilonBasepathTmp + "/tmp/"): Unit = {
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
    val fileOut = new File(path)

    if (!fileOut.getParentFile.exists()) fileOut.getParentFile.mkdirs()
    set777(fileOut)
    set777(fileOut.getParentFile) // /IDRICHIESTA
    set777(fileOut.getParentFile.getParentFile) // MESE/IDRICHIESTA
    set777(fileOut.getParentFile.getParentFile.getParentFile) // ANNO/MESE/IDRICHIESTA
    set777(fileOut.getParentFile.getParentFile.getParentFile.getParentFile) // BASENAME_PIVA/ANNO/MESE/IDRICHIESTA
    set777(fileOut.getParentFile.getParentFile.getParentFile.getParentFile.getParentFile) // OPEATIONNAME/BASENAME_PIVA/ANNO/MESE/IDRICHIESTA
    set777(fileOut.getParentFile.getParentFile.getParentFile.getParentFile.getParentFile.getParentFile) // BASENAME/OPEATIONNAME/BASENAME_PIVA/ANNO/MESE/IDRICHIESTA
    set777(fileOut.getParentFile.getParentFile.getParentFile.getParentFile.getParentFile.getParentFile.getParentFile) // CDP/BASENAME/OPEATIONNAME/BASENAME_PIVA/ANNO/MESE/IDRICHIESTA

    val pw = new PrintWriter(new FileWriter(fileOut, appendMode))
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

  def putIntoZip(zip: ZipOutputStream, name: File): Unit = {
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

  /**
   * Determina se per un certo dataframe ha senso effettuare l'operazione di broadcast o meno (utilizzando la soglia 10000000)
   * @param df dataframe
   * @return booleano che indica se il dataframe è broadcastabile o meno
   */
  def isBroadcast(df: DataFrame): Boolean = {
    df.foreach(_ => ())
    val catalyst_plan = df.queryExecution.logical
    val df_size_in_bytes = Environment.spark.sessionState.executePlan(catalyst_plan).optimizedPlan.stats.sizeInBytes

    df_size_in_bytes <= 10000000
  }

  def convertStringTimestampToLocalDateTime(localDateTime: String): LocalDateTime = {
    Timestamp.valueOf(localDateTime).toLocalDateTime
  }
}
