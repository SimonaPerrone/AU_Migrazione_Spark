package it.eng.au.sgsFlussoStoricoGas.utility.file

import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io._
import scala.util.Try

object FileUtility {
  def set777(file: File): Unit = {
    Try(file.setExecutable(true, false))
    Try(file.setWritable(true, false))
    Try(file.setReadable(true, false))
  }

  def safeReadParquet(path: String): Boolean = {
    val fs = FileSystem.get(Environment.getSpark.sparkContext.hadoopConfiguration)
    val dir = new Path(path)

    // Controlla se il path esiste ed è una directory
    if (fs.exists(dir) && fs.isDirectory(dir)) {
      // Ottiene la lista dei file e directory all'interno
      val files = fs.listStatus(dir)

      // Se ci sono file o sottodirectory (partizioni), restituisce true
      files.nonEmpty
    } else {
      false
    }
  }

}
