package it.eng.au.utility

import scala.util.Try
import java.io.{File, FileWriter, PrintWriter}

object FileUtility {
  def getRecursiveListOfFiles(dir: File): List[File] = {
    val these = dir.listFiles.toList
    these ++ these.filter(_.isDirectory).flatMap(getRecursiveListOfFiles)
  }

  def set777(file: File): Unit = {
    Try(file.setExecutable(true, false))
    Try(file.setWritable(true, false))
    Try(file.setReadable(true, false))
  }

  def writeCsv(path: String, header: String, content: List[String], byteLimit: Option[Long] = None, appendMode: Boolean = false): Unit = {
    var fileOut = new File(path)

    if(!fileOut.getParentFile.exists()) fileOut.getParentFile.mkdirs()
    set777(fileOut)
    set777(fileOut.getParentFile) // /MESEGIORNO
    set777(fileOut.getParentFile.getParentFile) // ANNO/MESEGIORNO
    set777(fileOut.getParentFile.getParentFile.getParentFile) // TME_PIVAD_PIVAUDD/ANNO/MESEGIORNO
    set777(fileOut.getParentFile.getParentFile.getParentFile.getParentFile) // DISTRIBUTORI/TME_PIVAD_PIVAUDD/ANNO/MESEGIORNO
    set777(fileOut.getParentFile.getParentFile.getParentFile.getParentFile.getParentFile) // TME_PIVAD/DISTRIBUTORI/TME_PIVAD_PIVAUDD/ANNO/MESEGIORNO

    var indexFile = 1
    var pw = new PrintWriter( new FileWriter(fileOut, appendMode) )
    if(fileOut.length() == 0) pw.write(header+"\n")

    if(byteLimit.isDefined) {
      content.zipWithIndex.foreach({case (row, index) =>
        pw.write(row+"\n")
        // every 1000 rows => flush
        if(index != 0 && index % 1000 == 0) {
          pw.flush()
          if(fileOut.length() >= byteLimit.get) {
            pw.close()
            // se non è l'ultima riga crea un nuovo file
            if(index < content.length -1 ) {
              fileOut = new File(path.replace(".txt", s"_$indexFile.txt"))
              indexFile+=1
              pw = new PrintWriter(fileOut)
              pw.write(header+"\n")
            }
          }
        }
      })
    } else {
      content.foreach(row => pw.write(row+"\n"))
    }

    pw.flush()
    pw.close()
  }
}
