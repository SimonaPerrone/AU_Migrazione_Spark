package it.eng.au.aggiustamentoGas.model.measure

import it.eng.au.aggiustamentoGas.utility.constants.{ForcingFlags, Treatment}
import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO
import org.apache.hadoop.fs.Path
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

import java.io.File
import scala.util.Try


trait Flow extends Product {
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
  lazy val dCaricamentoFromLocalFile: DateTime = {
    localFile match {
      case Some(path) =>
        val meseGiorno = new Path(path).getParent.getName
        val anno = new Path(path).getParent.getParent.getName
        Try(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(anno + meseGiorno).withTimeAtStartOfDay())
          .getOrElse(MeasureDAO.genericDateTimeFormatter.parseDateTime("01/01/2700"))
      case None => MeasureDAO.genericDateTimeFormatter.parseDateTime("01/01/2700")
    }
  }

  lazy val timestampLocalFile: DateTime = {
    localFile match {
      case Some(path) =>
        val timestampStringPart = new Path(path).getName.split('_')
        val tsStringOld = timestampStringPart(timestampStringPart.length - 2)
        val tsStringStd = timestampStringPart(timestampStringPart.length - 3)
        Try(DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(tsStringOld))
          .getOrElse(Try(DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(tsStringStd)).getOrElse(Flow.minDateTime))
      case None => Flow.minDateTime
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

  lazy val pivaUddFromLocalPath: Option[String] = {
    localFile.map(new File(_).getParentFile.getParentFile.getParentFile.getName.split("_").last)
  }

  lazy val pivaUddFromLocalFile: Option[String] = {
    localFile.map(new File(_).getName.split("_").apply(1))
  }

  lazy val fileName: Option[String] = {
    localFile.map(new File(_).getName.split("/").last.toUpperCase)
  }

  val service: String //flusso
  val pdr: String //cod_pdr
  val date: Option[DateTime] //date
  val measure: Option[Double] //Dato Misura 1
  val converted: Option[Double] //Dato Misura 2
  val serialNumberMis: Option[String] //matricola misuratore
  val serialNumberConv: Option[String] //matricola convertitore
  val localFile: Option[String]
  val dataCaricamento: Option[DateTime]
  val pivaDistr: Option[String]
  val pivaUtente: Option[String]
  val isCorrected: Boolean // flag true iff an im1/igmg is object of correction
  var dimTypeForced: Option[ForcingFlags.Value] = None // flag valued iff FlowWithInfo.dimensionalType is forced due to im1/igmg special case for that pdr
  val isValid: Option[String] = None
  val outcome: Option[Char] = None
  val readType: Option[Char] = None
  val motivation: Option[Int] = None
  var ammissibilita: Option[String] = None
  var treatment: Treatment.Value = Treatment.N
  var activationFlow: Option[Flow] = None

  override def toString: String = {
    getClass.getSimpleName + "(" + getClass.getDeclaredFields
      .zip(productIterator.toSeq)
      .map({ case (a, b) => s"${a.getName}=$b" })
      .mkString(", ") + ", ammissibilita=" + ammissibilita + ")"
  }

  def setTreatment(treat: Treatment.Value): Flow = {
    treatment = treat
    this
  }

  def setDimTypeForced(forcing: ForcingFlags.Value): Flow = {
    dimTypeForced = Some(forcing)
    this
  }

  def setAmmissibilita(ammiss: Option[String]): Flow = {
    ammissibilita = ammiss
    this
  }

  def setActivationFlow(actFlow: Option[Flow]): Flow = {
    activationFlow = actFlow
    this
  }
}

object Flow {
  val minDateTime = new DateTime(0, 1, 1, 0, 0, 0, DateTimeZone.UTC)
  val orderingSameDayFlows: Ordering[Flow] = new Ordering[Flow] {
    override def compare(x: Flow, y: Flow): Int = {
      val comparesRules = List(
        x.dateLoadFromLocalFile.compareTo(y.dateLoadFromLocalFile),
        x.timestampLocalFile.compareTo(y.timestampLocalFile),
        x.progressiveLocalFile.compareTo(y.progressiveLocalFile),
        x.dataCaricamento.getOrElse(new DateTime()).compareTo(y.dataCaricamento.getOrElse(new DateTime()))
      )
      comparesRules.find(_ != 0).getOrElse(0)
    }
  }
  val orderingFlowsByDateTime: Ordering[Flow] = new Ordering[Flow] {
    override def compare(x: Flow, y: Flow): Int = {
      val datesAreDefined: Boolean = x.date.isDefined && y.date.isDefined
      val xDateIsDefined: Boolean = x.date.isDefined && y.date.isEmpty
      val yDateIsDefined: Boolean = y.date.isDefined && x.date.isEmpty
      //Null dates are the lowest possible value, null dates are equals
      if (xDateIsDefined || (datesAreDefined && x.date.get.isAfter(y.date.get))) {
        10
      } else if (yDateIsDefined || (datesAreDefined && x.date.get.isBefore(y.date.get))) {
        -10
      }
      else {
        0
      }
    }
  }

}
