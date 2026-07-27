package it.eng.au.ERP.trasformations.NO

import it.eng.au.ERP.model.tratt_pod.TrattPodAnnomesePartitionedModel
import it.eng.au.ERP.schema.au.{flussiTeniciSchema, flussoMisureNoAggrSchema, flussoMisureSmisSchema}
import it.eng.au.ERP.schema.tratt_pod.trattPodAllAnnomesePartitionedSchema
import it.eng.au.ERP.utility.functions.{Constants, argumentsUtilities}
import org.apache.spark.sql.functions.{col, udf, upper}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAdjusters

object CalcoloPrelevatoPuntiPrelievoNonOrari {

  // Common formatter
  val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  // UDF: Last day of previous month
  val startInUDF = udf((dateStr: String) => {
    try {
      val date = LocalDate.parse(dateStr, formatter)
      val firstOfMonth = date.withDayOfMonth(1)
      val lastDayPreviousMonth = firstOfMonth.minusDays(1)
      lastDayPreviousMonth.format(formatter)
    } catch {
      case _: Exception => null
    }
  })

  // UDF: Last day of the same month
  val stopInUDF = udf((dateStr: String) => {
    try {
      val date = LocalDate.parse(dateStr, formatter)
      val lastDayOfMonth = date.`with`(TemporalAdjusters.lastDayOfMonth())
      lastDayOfMonth.format(formatter)
    } catch {
      case _: Exception => null
    }
  })

  //include anche la partizione anno/mese precedente a quella che stiamo anaalizzando
  def flussiPeriodiciSegmentoSinistraEDestro(dfFlussoMisureNoAggr: Dataset[Row],
                                             year: Option[Int],
                                             month: Option[Int],
                                             podExcluded: List[String]
                                            )
                                            (implicit spark: SparkSession): DataFrame = {

    val finalDf =
      (year, month) match {
        case (Some(yearValue), Some(monthValue)) => {
          val (previousYear, previousMonth) = argumentsUtilities.yearMonthMinusOneMonth(yearValue, monthValue)
          dfFlussoMisureNoAggr
            .filter(
              (col(flussoMisureNoAggrSchema.anno) === yearValue &&
                col(flussoMisureNoAggrSchema.mese) === monthValue) ||
                (col(flussoMisureNoAggrSchema.anno) === previousYear &&
                  col(flussoMisureNoAggrSchema.mese) === previousMonth)
            )
        }
        case _ =>
          dfFlussoMisureNoAggr
      }

    finalDf
      .filter(!(col(flussoMisureNoAggrSchema.pod).isin(podExcluded: _*)))

  }

  def flussiPeriodiciSegmentoDestro(dfFlussoMisureNoAggr: Dataset[Row],
                                    year: Option[Int],
                                    month: Option[Int],
                                    podExcluded: List[String]
                                   )
                                   (implicit spark: SparkSession): DataFrame = {

    val dfFilteredYear = year match {
      case Some(yearValue) => dfFlussoMisureNoAggr
        .filter(col(flussoMisureNoAggrSchema.anno) === yearValue)
      case None => dfFlussoMisureNoAggr
    }

    val dfFilteredMonth = month match {
      case Some(monthValue) => dfFilteredYear
        .filter(col(flussoMisureNoAggrSchema.mese) === monthValue)
      case None => dfFilteredYear
    }

    dfFilteredMonth
      .filter(!(col(flussoMisureNoAggrSchema.pod).isin(podExcluded: _*)))

  }


  def flussiPeriodiciSegmentoSinistro(dfFlussoMisureNoAggr: Dataset[Row],
                                      year: Option[Int],
                                      month: Option[Int],
                                      podExcluded: List[String]
                                     )
                                     (implicit spark: SparkSession): DataFrame = {

    val finalDf =
      (year, month) match {
        case (Some(yearValue), Some(monthValue)) => {
          val (previousYear, previousMonth) = argumentsUtilities.yearMonthMinusOneMonth(yearValue, monthValue)
          dfFlussoMisureNoAggr
            .filter(
              col(flussoMisureNoAggrSchema.anno) === previousYear &&
                col(flussoMisureNoAggrSchema.mese) === previousMonth
            )

        }
        case _ =>
          dfFlussoMisureNoAggr

      }

    finalDf
      .filter(!(col(flussoMisureNoAggrSchema.pod).isin(podExcluded: _*)))

  }


  def trattPodllAnnomesePartitionedPreparedSegmentoSinistroEDestro(
                                                                    trattPodllAnnomesePartitioned: Dataset[TrattPodAnnomesePartitionedModel],
                                                                    year: Option[Int], month: Option[Int],
                                                                    podExcluded: List[String]
                                                                  )
                                                                  (implicit spark: SparkSession): Dataset[TrattPodAnnomesePartitionedModel] = {

    val dfFiltered = (year, month) match {
      case (Some(yearValue), Some(monthValue)) => {
        val (previousYear, previousMonth) = argumentsUtilities.yearMonthMinusOneMonth(yearValue, monthValue)

        trattPodllAnnomesePartitioned
          .filter((
            col(trattPodAllAnnomesePartitionedSchema.anno) === yearValue &&
              col(trattPodAllAnnomesePartitionedSchema.mese) === monthValue) ||
            col(trattPodAllAnnomesePartitionedSchema.anno) === previousYear &&
              col(trattPodAllAnnomesePartitionedSchema.mese) === previousMonth
          )
      }
      case _ =>
        trattPodllAnnomesePartitioned
    }

    dfFiltered
      .filter(upper(col(trattPodAllAnnomesePartitionedSchema.is_t_trattamento)).isin(Constants.IsTTrattamento: _*))
      .filter(!(col(trattPodAllAnnomesePartitionedSchema.pod14).isin(podExcluded: _*)))

  }


  def trattPodllAnnomesePartitionedPreparedSegmentoDestro(
                                                           trattPodllAnnomesePartitioned: Dataset[TrattPodAnnomesePartitionedModel],
                                                           year: Option[Int], month: Option[Int],
                                                           podExcluded: List[String]
                                                         )
                                                         (implicit spark: SparkSession): Dataset[TrattPodAnnomesePartitionedModel] = {

    val dfFilteredYear = year match {
      case Some(yearValue) => trattPodllAnnomesePartitioned
        .filter(col(trattPodAllAnnomesePartitionedSchema.anno) === yearValue)
      case None => trattPodllAnnomesePartitioned
    }

    val dfFilteredMonth = month match {
      case Some(monthValue) => dfFilteredYear
        .filter(col(trattPodAllAnnomesePartitionedSchema.mese) === monthValue)
      case None => dfFilteredYear
    }

    dfFilteredMonth.filter(upper(col(trattPodAllAnnomesePartitionedSchema.is_t_trattamento)).isin(Constants.IsTTrattamento: _*))
      .filter(!(col(trattPodAllAnnomesePartitionedSchema.pod14).isin(podExcluded: _*)))

  }


  def trattPodllAnnomesePartitionedPreparedSegmentoSinistro(
                                                             trattPodllAnnomesePartitioned: Dataset[TrattPodAnnomesePartitionedModel],
                                                             year: Option[Int], month: Option[Int],
                                                             podExcluded: List[String]
                                                           )
                                                           (implicit spark: SparkSession): Dataset[TrattPodAnnomesePartitionedModel] = {

    val dfFiltered = (year, month) match {
      case (Some(yearValue), Some(monthValue)) => {
        val (previousYear, previousMonth) = argumentsUtilities.yearMonthMinusOneMonth(yearValue, monthValue)

        trattPodllAnnomesePartitioned
          .filter(
            col(trattPodAllAnnomesePartitionedSchema.anno) === previousYear &&
              col(trattPodAllAnnomesePartitionedSchema.mese) === previousMonth
          )
      }
      case _ =>
        trattPodllAnnomesePartitioned
    }

    dfFiltered
      .filter(upper(col(trattPodAllAnnomesePartitionedSchema.is_t_trattamento)).isin(Constants.IsTTrattamento: _*))
      .filter(!(col(trattPodAllAnnomesePartitionedSchema.pod14).isin(podExcluded: _*)))

  }

  //include anche la partizione anno/mese precedente a quella che stiamo anaalizzando
  def flussiTecniciSegmentoSinistraEDestro(dfFlussiTecnici: Dataset[Row],
                                           year: Option[Int],
                                           month: Option[Int],
                                           podExcluded: List[String]
                                          )
                                          (implicit spark: SparkSession): DataFrame = {

    val finalDf = {
      (year, month) match {
        case (Some(yearValue), Some(monthValue)) => {
          val (previousYear, previousMonth) = argumentsUtilities.yearMonthMinusOneMonth(yearValue, monthValue)
          dfFlussiTecnici
            .filter(
              (col(flussiTeniciSchema.anno) === yearValue &&
                col(flussiTeniciSchema.mese) === monthValue) ||
                (col(flussiTeniciSchema.anno) === previousYear &&
                  col(flussiTeniciSchema.mese) === previousMonth)
            )
        }
        case _ =>
          dfFlussiTecnici
      }
    }
    finalDf.filter(!(col(flussiTeniciSchema.pod).isin(podExcluded: _*)))

  }

  //include anche la partizione anno/mese precedente a quella che stiamo anaalizzando
  def flussiMisureSmisSegmentoSinistraEDestro(dfFlussiTecnici: Dataset[Row],
                                              year: Option[Int],
                                              month: Option[Int],
                                              podExcluded: List[String]
                                             )
                                             (implicit spark: SparkSession): DataFrame = {

    val finalDf = {
      (year, month) match {
        case (Some(yearValue), Some(monthValue)) => {
          val (previousYear, previousMonth) = argumentsUtilities.yearMonthMinusOneMonth(yearValue, monthValue)
          dfFlussiTecnici
            .filter(
              (col(flussoMisureSmisSchema.anno_dtms) === yearValue &&
                col(flussoMisureSmisSchema.mese_dtms) === monthValue) ||
                (col(flussoMisureSmisSchema.anno_dtms) === previousYear &&
                  col(flussoMisureSmisSchema.mese_dtms) === previousMonth)
            )
        }
        case _ =>
          dfFlussiTecnici
      }
    }

    finalDf.filter(!(col(flussoMisureSmisSchema.pod).isin(podExcluded: _*)))

  }

}
