package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.utility.constants.Treatment
import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{AMMISSIBILITA, TRATTAMENTO}
import it.eng.au.aggiustamentoGas.model.measure.Flow
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, DateTimeFormatterBuilder, DateTimeParser}

import scala.annotation.tailrec
import scala.util.Try

/** Contiene i metodi comuni a tutti i DAO (lettura e mapping delle misure) */
trait MeasureDAO extends Dao {
  /** Campo di partizionamento delle tabelle */
  val partitionDateColumn: Column
  /** Funzione di mapping da record su tabella [[Row]] a flusso [[Flow]]; ogni misura (DAO) implementa la versione specifica. */
  val mapFunc: Row => Flow
  /** Funzione di filtro delle misure; ogni misura implementa la versione specifica */
  val filterFlow: Flow => Boolean

  /**
   * Estrae i record dalla tabella delle misure, effettuando un partition pruning tramite [[startDate]] e [[endDate]],
   * e successivamente filtra le misure tramite [[filterFlow]]
   * @param startDate estremo sinitro di lettura delle misure
   * @param endDate estremo destro di lettura delle misure
   * @param getTreatment booleano che indica se estrarre o meno il trattamento
   * @return RDD[ [[Flow]] ] contenente le misure
   */
  def get(startDate: String, endDate: String, getTreatment: Boolean): RDD[Flow] = {
    val df = readParquet
    val func = mapFunc
    df.filter(filterPartitions(startDate, endDate))
      .coalesce(600) //to avoid OOM due to bad hdfs partitioning
      .rdd
      .map(r => {
        val flow = func(r)  //funzione di mapping
        flow.ammissibilita = Option(r.getAs[String](AMMISSIBILITA))
        if (getTreatment) {
          val treatment = Treatment.values.find(_.toString == r.getAs[String](TRATTAMENTO)).getOrElse(Treatment.N)
          flow.setTreatment(treatment)
        } else flow
      }).filter(filterFlow)  //funzione di filtro
  }

  override def readParquet: DataFrame = {
    val cols = columns :+ TRATTAMENTO :+ AMMISSIBILITA
    val schema = StructType(cols.map(c => StructField(c, StringType)))
    Environment.getSpark.sqlContext.read.schema(schema).parquet(parquetPath)
      .selectExpr(cols: _*)
  }

  def filterPartitions(startDate: String, endDate: String): Column = {
    val dateFrom = to_date(unix_timestamp(lit(startDate), "yyyyMM").cast(TimestampType))
    val dateTo = to_date(unix_timestamp(lit(endDate), "yyyyMM").cast(TimestampType))

    partitionDateColumn.between(dateFrom, dateTo)
  }
}

object MeasureDAO {
  val MESE_COMP_COL_NAME = "mese_comp"
  val ANNO_MESE_COL_NAME = "annomese"
  val meseCompColumn: Column = to_date(unix_timestamp(col(MESE_COMP_COL_NAME), "MMyyyy").cast(TimestampType))
  val annoMeseColumn: Column = to_date(unix_timestamp(col(ANNO_MESE_COL_NAME), "yyyyMM").cast(TimestampType))

  val dateLoadFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
  val genericDateTimeParsers: Array[DateTimeParser] = Array(
    DateTimeFormat.forPattern("dd/MM/yyyy").getParser,
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").getParser
  )
  val genericDateTimeFormatter: DateTimeFormatter = new DateTimeFormatterBuilder().append(null, genericDateTimeParsers).toFormatter

  val TRATTAMENTO = "trattamento"
  val AMMISSIBILITA = "ammissibilita"


  /**
   * Return the first valid value, if any. Using this method we are able to pick the right field value and differentiate
   * between old and standard flows.
   *
   * @param dateStandardFlow value present only for standard flows
   * @param dateOldFlow      value present for old flows
   * @return dataRacc if not null nor empty, dataComAutoletCf otherwise as Option[org.joda.time.DateTime]
   */
  def getDate(dateStandardFlow: String, dateOldFlow: String = ""): Option[DateTime] = {
    (isEmptyOrNull(dateStandardFlow), isEmptyOrNull(dateOldFlow)) match {
      case (true, true) => None
      case (true, false) => parseDateToOption(dateOldFlow.trim)
      case (false, _) => parseDateToOption(dateStandardFlow.trim)
    }
  }

  def parseDateToOption(value: String, formatter: DateTimeFormatter = genericDateTimeFormatter): Option[DateTime] = Try(formatter.parseDateTime(value)).toOption

  /**
   * Used in this.getData method.
   *
   * @param str an input string representing a flow field
   * @return true if trimmed input is either null or empty or equals to the string "null"
   */
  def isEmptyOrNull(str: String): Boolean = {
    str == null || str.trim.equalsIgnoreCase("null") || str.trim.equalsIgnoreCase("")
  }

  /**
   * Return the first valid value, if any. Using this method we are able to pick the right field value and differentiate
   * between old and standard flows.
   *
   * @param fieldStdFlow value present only for standard flows
   * @param filedOldFlow value present for old flows
   * @return fieldStdFlow if not null, filedOldFlow otherwise
   */
  def getDoubleField(fieldStdFlow: String, filedOldFlow: String = ""): Option[Double] = {
    (isEmptyOrNull(fieldStdFlow), isEmptyOrNull(filedOldFlow)) match {
      case (true, true) => None
      case (true, false) => Try(Some(filedOldFlow.toDouble)).getOrElse(None)
      case (false, _) => Try(Some(fieldStdFlow.toDouble)).getOrElse(None)
    }
  }

  /**
   * Return the first valid value, if any. Using this method we are able to pick the right field value and differentiate
   * between old and standard flows.
   *
   * @param fieldStdFlow value present only for standard flows
   * @param filedOldFlow value present for old flows
   * @return fieldStdFlow if not null, filedOldFlow otherwise
   */
  def getStringField(fieldStdFlow: String, filedOldFlow: String): Option[String] = {
    (isEmptyOrNull(fieldStdFlow), isEmptyOrNull(filedOldFlow)) match {
      case (true, true) => None
      case (true, false) => Some(filedOldFlow.trim)
      case (false, _) => Some(fieldStdFlow.trim)
    }
  }
}
