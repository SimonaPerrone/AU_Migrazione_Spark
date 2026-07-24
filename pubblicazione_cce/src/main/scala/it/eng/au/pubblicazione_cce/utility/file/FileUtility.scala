package it.eng.au.pubblicazione_cce.utility.file

import java.io.File
import scala.reflect.io.Directory
import scala.util.Try

object FileUtility {

  def set777(file: File): Unit = {
    Try(file.setExecutable(true, false))
    Try(file.setWritable(true, false))
    Try(file.setReadable(true, false))
  }

  def deleteContents(directory: File): Unit = {
    if (directory.isDirectory) {
      Option(directory.listFiles).map(_.toList).getOrElse(Nil).foreach { file =>
        if (file.isDirectory)
          new Directory(file).deleteRecursively()
        else
          file.delete
      }
    }
  }

}
