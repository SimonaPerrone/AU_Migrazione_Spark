package it.eng.au.ammissibilitaRendiconti.utility.file

import java.io.{File, FileWriter, PrintWriter}
import scala.util.Try

object FileUtility {
  def set777(file: File): Unit = {
    Try(file.setExecutable(true, false))
    Try(file.setWritable(true, false))
    Try(file.setReadable(true, false))
  }

  def writeCsv(path: String, header: String, content: List[String], isTmpFolder: Boolean, appendMode: Boolean = false): Unit = {
    val fileOut = new File(path)

    if (isTmpFolder) {
      if (!fileOut.getParentFile.exists()) fileOut.getParentFile.mkdirs()
      set777(fileOut)
      set777(fileOut.getParentFile) // MESE
      set777(fileOut.getParentFile.getParentFile)  // ANNO/MESE
      set777(fileOut.getParentFile.getParentFile.getParentFile) // BASENAME_PIVA/ANNO/MESE
      set777(fileOut.getParentFile.getParentFile.getParentFile.getParentFile) // OPEATIONNAME/BASENAME_PIVA/ANNO/MESE
      set777(fileOut.getParentFile.getParentFile.getParentFile.getParentFile.getParentFile) // BASENAME/OPEATIONNAME/BASENAME_PIVA/ANNO/MESE
      set777(fileOut.getParentFile.getParentFile.getParentFile.getParentFile.getParentFile.getParentFile) // RZG/BASENAME/OPEATIONNAME/BASENAME_PIVA/ANNO/MESE
    }

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
