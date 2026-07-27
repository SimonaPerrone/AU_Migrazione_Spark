package it.sferanet.au.filterPdr

import it.sferanet.au.model.Flow
import it.sferanet.au.schema.{CaOutputSchema, CaPreFinalSchema, PdrMassivoSchema}
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import java.time.{LocalDate, Month}

trait FilterPdr {
  protected def getPdrs: RDD[String]

  def filter(measures: RDD[Flow]): RDD[Flow] = {
    val pdrRDD = getPdrs.distinct.map((_, ()))

    measures.keyBy(_.pdr).join(pdrRDD).values.map(_._1)
  }

  def filterPdrMassivo(pdrMassivo: DataFrame): DataFrame = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val pdrDF = getPdrs.distinct.toDF("cod_pdr")

    pdrMassivo.join(pdrDF, pdrDF("cod_pdr") === pdrMassivo(PdrMassivoSchema.codice_pdr), "inner")
      .drop(pdrDF.col("cod_pdr"))
  }

  def betweenDates(date: Column, dateFrom: Column, dateTo: Column): Column = date.between(
    to_utc_timestamp(coalesce(dateFrom, lit("1900-01-01")), "yyyy-MM-dd"),
    to_utc_timestamp(coalesce(dateTo, lit("2900-01-01")), "yyyy-MM-dd")
  )

  /**
   * Get the current <b>thermal year</b>. <br>
   * A thermal year is between 1-October-YYYY and 30-September-(YYYY+1). <br>
   * It is computed as follows:<br>
   * if today is between 1-Jan to 30 September then the current year is returned, otherwise the next year is returned.<br>
   * For example, assume today is 16-03-2021 the returned thermal year will be 2021. Assume today is 10-10-2021 the
   * returned thermal year will be 2022
   *
   * @param today telling today date
   * @return the <b>thermal year</b> where "today" param fall into
   */
  def getCurrentThermalYear(today: LocalDate): Int = {
    if (today.getMonth.compareTo(Month.OCTOBER) < 0) {
      today.getYear
    }
    else {
      today.plusYears(1).getYear
    }
  }

  def dataCreazioneTimestampToDate(dataCreazione: Column): Column = {
    to_date(from_unixtime(unix_timestamp(substring(dataCreazione, 0, 8), Constants.HALF_YEAR_FORMAT_DATE)))
  }

  def getCa: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getCaPath).where(col(CaOutputSchema.session) === "CDP")
  }

  def getCaFinal: DataFrame = {
    Environment.getSqlContext.table(Environment.getCaFinalTableName)
  }

  def getCaPreFinal: DataFrame = {
    if (Environment.getSession.equals("CCG"))
      Environment.getSqlContext.table(Environment.getCaPreFinalTableName)
        .where(col(CaPreFinalSchema.executionid) === Environment.getCaPreFinalExecutionId.toLong)
    else Environment.getSqlContext.table(Environment.getCaPreFinalTableName)
      .where(col(CaPreFinalSchema.session) === "CDP")
  }

  def getSettleGasGasTds: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getGasTdsPath)
  }

  def getRcuGasMassivoP: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getRcugasMassivoPPath)
  }

  def getRcuGasBilanciamentoP: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getRcugasBilanciamentoPath)
  }

  def getRcuAziendaP: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getRcuAziendaPath)
  }

  def getRcuGasUdbP: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getRcuGasUdbPath)
  }

  def getRcuGasConnessioniDistr2P: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getRcugasConnessioniDistr2Path)
  }

  def getVRcuGasDistributoreP: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getRcugasDistributorePath)
  }

  def getCodProfStdDaTds: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getCodProfStdDaTdsPath)
  }

}
