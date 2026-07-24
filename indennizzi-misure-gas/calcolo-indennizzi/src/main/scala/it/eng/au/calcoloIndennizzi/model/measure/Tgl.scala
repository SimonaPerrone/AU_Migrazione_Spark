package it.eng.au.calcoloIndennizzi.model.measure

import org.apache.hadoop.fs.Path
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

import it.eng.au.calcoloIndennizzi.controller.TglController
import java.io.File
import scala.util.Try

/** Case class per le misure TGL. Sono inoltre definiti una serie di lazy val utilizzati nei vari processi nel [[TglController]]. */
case class Tgl(
                pdr: String,
                date: Option[DateTime],
                measure: Option[Double],
                converted: Option[Double],
                serialNumberMis: Option[String],
                serialNumberConv: Option[String],
                pivaUtente: Option[String],
                pivaDistr: Option[String],
                localFile: Option[String],
                readType: Option[Char],
                dataCaricamento: Option[DateTime],
                isValid: Option[String],
                ammissibilita: Option[String]
              ) {

  lazy val dateLoadFromLocalFile: Int = {
    // /mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml
    localFile match {
      case Some(path) =>
        val meseGiorno = new Path(path).getParent.getName
        val anno = new Path(path).getParent.getParent.getName
        (anno + meseGiorno).toInt
      case None => 0
    }
  }

  lazy val progressiveLocalFile: Int = {
    val progressiveOld = Try(localFile.get.split("_").last.replaceAll("\\.\\w+$", "").toInt)
    val progressiveStd =
      Try({
        val fileNameSplitted = localFile.get.split("_")
        fileNameSplitted(fileNameSplitted.size - 2).replaceAll("\\.\\w+$", "").toInt
      })

    progressiveOld.getOrElse(progressiveStd.getOrElse(0))
  }

  lazy val timestampLocalFile: DateTime = {
    localFile match {
      case Some(path) =>
        val timestampStringPart = new Path(path).getName.split('_')
        val tsStringOld = timestampStringPart(timestampStringPart.length - 2)
        val tsStringStd = timestampStringPart(timestampStringPart.length - 3)
        Try(DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(tsStringOld))
          .getOrElse(Try(DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(tsStringStd)).getOrElse(Tgl.minDateTime))
      case None => Tgl.minDateTime
    }
  }

  lazy val fileName: Option[String] = {
    localFile.map(new File(_).getName.split("/").last.toUpperCase)
  }

  lazy val pivaUddFromLocalPath: Option[String] = {
    localFile.map(new File(_).getParentFile.getParentFile.getParentFile.getName.split("_").last)
  }
}

object Tgl {
  val minDateTime = new DateTime(0, 1, 1, 0, 0, 0, DateTimeZone.UTC)

  val orderingFlowsByDateTime: Ordering[DateTime] = new Ordering[DateTime] {
    override def compare(x: DateTime, y: DateTime): Int = x.compareTo(y)
  }

  val orderingSameDayFlows: Ordering[Tgl] = new Ordering[Tgl] {
    override def compare(x: Tgl, y: Tgl): Int = {
      val comparesRules = List(
        x.dateLoadFromLocalFile.compareTo(y.dateLoadFromLocalFile),
        x.timestampLocalFile.compareTo(y.timestampLocalFile),
        x.progressiveLocalFile.compareTo(y.progressiveLocalFile),
        x.dataCaricamento.getOrElse(new DateTime()).compareTo(y.dataCaricamento.getOrElse(new DateTime()))
      )
      comparesRules.find(_ != 0).getOrElse(0)
    }
  }
}