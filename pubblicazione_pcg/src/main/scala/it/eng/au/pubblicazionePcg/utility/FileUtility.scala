package it.eng.au.pubblicazionePcg.utility

import java.io.{File, FileWriter, PrintWriter}
import scala.util.Try

import scala.language.postfixOps

object FileUtility {
  def set777(file: File): Unit = {
    Try(file.setExecutable(true, false))
    Try(file.setWritable(true, false))
    Try(file.setReadable(true, false))
  }

  def writeCsv(path: String, header: String, content: List[String], appendMode: Boolean = false): Unit = {
    val fileOut = new File(path)

    val pw = new PrintWriter(new FileWriter(fileOut, appendMode))
    if (fileOut.length() == 0) pw.write(header + "\n")

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
