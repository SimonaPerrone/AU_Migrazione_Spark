package it.sferanet.au.model

import it.sferanet.au.controller.visitor._
import it.sferanet.au.model.flowTypes.Rettifica
import it.sferanet.au.model.prestazionale.{IgmgPost, IgmgPre, IgmrPost, IgmrPre, Im1Post, Im1Pre}
import it.sferanet.au.utilities.Constants.BLOCCANTE
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

import java.io.File
import java.util.Date
import scala.util.Try

// Campi principali
trait Flow {
  val service: String //flusso
  val pdr: String //cod_pdr
  val date: Option[Date] //date
  val pivaDistr: Option[String] //pivaDistr
  val pivaUtente: Option[String] //pivaUtente
  val measure: Option[Double] //Dato Misura 1
  val converted: Option[Double] //Dato Misura 2
  val serialNumberMis: Option[String] //matricola misuratore
  val serialNumberConv: Option[String] //matricola convertitore
  val local_file: Option[String]
  val d_caricamento: Option[Date]
  val isNewRoute: Boolean
  val isValid: Option[String] = None
  val outcome: Option[Char] = None
  val readType: Option[Char] = None
  val ammissibilita: Option[String] = None
  val motivation: Option[Int] = None
  val coefCorr: Option[Double] = None //valorizzato solo per IM1/IGMG/IGMR
  var coef: Option[Double] = None //valorizzato in fase di calcolo dei consumi se presenti IM1/IGMG/IGMR nel periodo di calcolo
  var forcing: Option[String] = None
  val fileRettifica: Option[String] = None

  def accept(visitor: IFlowVisitor)

  def accept[TReturnValue](visitor: IFlowWithReturnVisitor[TReturnValue]): TReturnValue

  lazy val dateLoadFromLocalFile: Int = {
    // /mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml
    val meseGiorno = new Path(local_file.get).getParent.getName
    val anno = new Path(local_file.get).getParent.getParent.getName
    Try((anno + meseGiorno).toInt).getOrElse(-1)
  }

  lazy val timestampLocalFile: Date = {
    try {
      val timestampStringPart = new Path(local_file.get).getName.split('_')
      val timestampIndex = if (isNewRoute && (!service.toUpperCase.startsWith("IGMG") && !service.toUpperCase.startsWith("IGMR"))) timestampStringPart.length - 3 else timestampStringPart.length - 2
      val tsString = timestampStringPart(timestampIndex)
      Constants.FORMAT_DATE_CLOUD_FILENAME.parse(tsString.padTo(14, '0'))
    } catch {
      case ex: Throwable =>
        new Date(0)
      // throw new java.text.ParseException("Error extract timestamp from %s. Errore message: %s".format(local_file.get,ex.getMessage), ex.getErrorOffset)
    }
  }

  lazy val progressiveLocalFile: Int = {
    val progressive = if (isNewRoute && !service.toUpperCase.startsWith("IGMG") && !service.toUpperCase.startsWith("IGMR"))
      Try(local_file.get.split("_").takeRight(2).head.toInt)
    else
      Try(local_file.get.split("_").last.replaceAll("\\.\\w+$", "").toInt)
    progressive.getOrElse(0)
  }

  def changeCoeff(coeff: Option[Double]): Flow = {
    coef = coeff
    this
  }

  def changeForcing(measureType: Option[String]): Flow = {
    forcing = measureType
    this
  }

  lazy val fileNameLocalFile: String = {
    if (local_file.isDefined)
      local_file.get.split("/").last.toUpperCase
    else
      ""
  }

  lazy val pivaUddFromLocalPath: Option[String] = {
    local_file.map(new File(_).getParentFile.getParentFile.getParentFile.getName.split("_").last)
  }
}

object Flow {
  val schema: StructType =
    StructType(
      StructField("service", StringType, nullable = true) ::
        StructField("pdr", StringType, nullable = true) ::
        StructField("dat", TimestampType, nullable = false) ::
        StructField("measure", DoubleType, nullable = false) ::
        StructField("converted", DoubleType, nullable = false) ::
        StructField("readTypeVisitor", StringType, nullable = false) ::
        StructField("serialNumberMis", StringType, nullable = false) ::
        StructField("serialNumberConv", StringType, nullable = false) ::
        StructField("timestampLocalFile", TimestampType, nullable = false) ::
        StructField("d_caricamento", IntegerType, nullable = false) ::
        StructField("local_file", StringType, nullable = false) ::
        StructField("cat_uso", StringType, nullable = true) ::
        StructField("classe_prelievo", StringType, nullable = true) ::
        StructField("data_creazione", TimestampType, nullable = true) ::
        StructField("motivazione_rettifica", IntegerType, nullable = true) ::
        StructField("cau_int_mis", IntegerType, nullable = true) ::
        StructField("cau_int_cor", IntegerType, nullable = true) ::
        StructField("file_rettifica", StringType, nullable = true) ::
        StructField("n_coeff_correzione", DoubleType, nullable = true) :: Nil
    )

  /**
   *
   * @param rowMeasure row corrispondente alla misura considerata di un particolare flusso
   * @param fieldCheck campo da utilizzare come discriminante per la verifica della versione del tracciato (nuovo o vecchio)
   * @return true se la misura è del nuovo tracciato, false altrimenti
   */
  def getIsNewRouteVersion(rowMeasure: Row, fieldCheck: String): Boolean = {
    rowMeasure.getAs(fieldCheck) != null
  }

  implicit class FlowExtension(val typesFlow: Iterable[String]) extends AnyVal {
    def containsTypes(types: Iterable[String]): Boolean = {
      typesFlow.toList.distinct.diff(types.toList.distinct).isEmpty
    }
  }

  implicit class ServiceName(val clazz: Class[_]) extends AnyVal {
    def getSimpleNameUpperCase: String = {
      clazz.getSimpleName.replaceAll("\\$", "").toUpperCase()
    }
  }

  def createSparkSchema(fields: List[String]): StructType =
    StructType(fields.distinct.map(StructField(_, StringType, nullable = true)))

  // ordinamendo dei flussi per PDR e data delle misure fissata, fatto sulla base della priorità di caricamento delle misure
  val priorityOrderingFlows: Ordering[Flow] = new Ordering[Flow] {
    override def compare(x: Flow, y: Flow): Int = {
      val comparesRules = List(
        x.dateLoadFromLocalFile.compareTo(y.dateLoadFromLocalFile),
        x.timestampLocalFile.compareTo(y.timestampLocalFile),
        (!x.isNewRoute).compareTo(!y.isNewRoute), // tracciato standard viene prima del vecchio tracciato
        (!x.isInstanceOf[Rettifica]).compareTo(!y.isInstanceOf[Rettifica]), // rettifiche vengono prima degli altri flussi di misura
        x.progressiveLocalFile.compareTo(y.progressiveLocalFile),
        (!x.service.toUpperCase.contains("PRE")).compareTo(!y.service.toUpperCase.contains("PRE")) // gestione flussi cambio misuratore IM1, IGMG e IGMR: prima PRE e dopo POST
      )
      comparesRules.find(_ != 0).getOrElse(0)
    }
  }

  // ordinamendo dei flussi per PDR e data delle misure fissata, fatto sulla base della priorità di caricamento delle misure
  val cancelingOrderingFlows: Ordering[Flow] = new Ordering[Flow] {
    override def compare(x: Flow, y: Flow): Int = {
      val comparesRules = List(
        x.dateLoadFromLocalFile.compareTo(y.dateLoadFromLocalFile),
        x.timestampLocalFile.compareTo(y.timestampLocalFile),
        (!x.isNewRoute).compareTo(!y.isNewRoute), // tracciato standard viene prima del vecchio tracciato
        (x.isInstanceOf[Rettifica]).compareTo(y.isInstanceOf[Rettifica]), // rettifiche vengono dopo degli altri flussi di misura
        x.progressiveLocalFile.compareTo(y.progressiveLocalFile),
        (!x.service.toUpperCase.contains("PRE")).compareTo(!y.service.toUpperCase.contains("PRE")) // gestione flussi cambio misuratore IM1, IGMG e IGMR: prima PRE e dopo POST
      )
      comparesRules.find(_ != 0).getOrElse(0)
    }
  }

  // ordinamendo dei flussi per PDR nel periodo di calcolo, fatto sulla base dell'ordine temporale delle misure
  val temporalOrderingFlows: Ordering[Flow] = new Ordering[Flow] {
    override def compare(x: Flow, y: Flow): Int = {
      val comparesRules = List(
        x.date.getOrElse(new Date(0L)).compareTo(y.date.getOrElse(new Date(0L))),
        (!x.service.toUpperCase.contains("PRE")).compareTo(!y.service.toUpperCase.contains("PRE")) // gestione flussi cambio misuratore IM1, IGMG e IGMR: prima PRE e dopo POST
      )
      comparesRules.find(_ != 0).getOrElse(0)
    }
  }

  def getCauIntMis(flow: Flow): Option[Int] = flow match {
    case f: IgmgPre => f.cau_int_mis
    case f: IgmgPost => f.cau_int_mis
    case f: IgmrPre => f.cau_int_mis
    case f: IgmrPost => f.cau_int_mis
    case f: Im1Pre => f.cau_int_mis
    case f: Im1Post => f.cau_int_mis
    case _ => None
  }

  def getCauIntCor(flow: Flow): Option[Int] = flow match {
    case f: IgmgPre => f.cau_int_cor
    case f: IgmgPost => f.cau_int_cor
    case f: IgmrPre => f.cau_int_cor
    case f: IgmrPost => f.cau_int_cor
    case f: Im1Pre => f.cau_int_cor
    case f: Im1Post => f.cau_int_cor
    case _ => None
  }

  /***
   * Filtro da applicare per il flusso di misura.
   * La misura deve essere compresa tra la startDate ed endDate presente nel file di properties
   * e se possiede la colonna ammissibilita' allora questa deve essere NULL o valore diverso di bloccante
   */
  def flowFilter(partitionName: String, partition: Column, ammissibilita: Column): Column = {
    val startDate = Environment.getFlowStartDate.toInt
    val endDate = Environment.getFlowEndDate.toInt

    // annomese in formato intero estratto da parititon column
    val ts = partitionName match {
      case "annomese" =>
        partition.cast(IntegerType)
      case "mese_comp" =>
        when(length(partition) === 5, concat(substring(partition, 2, 4), lit("0"), substring(partition, 1, 1)))
          .otherwise(concat(substring(partition, 3, 4), substring(partition, 1, 2))).cast(IntegerType)
    }
    // se colonna ammissibilita esiste in sorgente
    if (ammissibilita != null) {
      // check campo ammissibilita in modalità NULL safe (NULL per vecchio tracciato)
      (ts >= startDate) and (ts <= endDate) and (ammissibilita.isNull or (ammissibilita =!= BLOCCANTE))
    } else {
      // se colonna ammissibilita non esiste
      (ts >= startDate) and (ts <= endDate)
    }
  }

}