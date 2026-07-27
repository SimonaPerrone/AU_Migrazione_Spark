package it.eng.au.mid.common

import java.io._
import scala.util.Try

object FileUtility {

  def set777(file: File): Unit = {
    Try(file.setExecutable(true, false))
    Try(file.setWritable(true, false))
    Try(file.setReadable(true, false))
  }

}
