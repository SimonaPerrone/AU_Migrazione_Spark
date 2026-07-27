package it.sferanet.au.utilities

import it.sferanet.au.model.prestazionale.{IgmgPost, IgmgPre, IgmrPost, IgmrPre, Im1Post, Im1Pre}
import it.sferanet.au.schema.RcuGasMassivoPSchema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row}

object DataframeUtils {

  def same(col1: Column, col2: Column): Column = {
    col1 === col2
  }

  def isSamePdr(pdr1ColName: String, pdr2ColName: String): Column = {
    same(col(pdr1ColName), col(pdr2ColName))
  }

  def isIgmgPre(service: Column): Column = {
    service === IgmgPre.serviceName
  }

  def isIgmgPost(service: Column): Column = {
    service === IgmgPost.serviceName
  }

  def isIgmrPre(service: Column): Column = {
    service === IgmrPre.serviceName
  }

  def isIgmrPost(service: Column): Column = {
    service === IgmrPost.serviceName
  }

  def isIm1Pre(service: Column): Column = {
    service === Im1Pre.serviceName
  }

  def isIm1Post(service: Column): Column = {
    service === Im1Post.serviceName
  }

  def isPre(service: Column): Column = {
    isIm1Pre(service) || isIgmgPre(service) || isIgmrPre(service)
  }

  def isPost(service: Column): Column = {
    isIm1Post(service) || isIgmgPost(service) || isIgmrPost(service)
  }

  def isIm1(service: Column): Column = {
    isIm1Pre(service) || isIm1Post(service)
  }

  def isIgmg(service: Column): Column = {
    isIgmgPre(service) || isIgmgPost(service)
  }

  def isIgmr(service: Column): Column = {
    isIgmrPre(service) || isIgmrPost(service)
  }

  /**
   * Controlla se la colonna service è di un flusso di cambio misuratore (IM1PRE,IM1POST,IGMGPRE,IGMGPOST)
   *
   * @param service [[Column]] la colonna con il codice del servizio da controllare
   * @return [[Column]] se il codice servizio è di cambio misuratore
   */
  def isMeterChangeMeasure(service: Column): Column = {
    isIgmgPre(service) || isIgmgPost(service) || isIm1Post(service) || isIm1Pre(service) || isIgmrPre(service) || isIgmrPost(service)
  }

  /**
   * Filtra il dataframe di tutte le misure in modo tale da considerare tutte le misure di un pdr se e solo se
   * quel pdr ha una misura di tipo cambio misuratore (Im1/Igmg) nel periodo considerato
   *
   * @param dfMeasures     [[Dataframe]] tutte le misure di tutti i pdr considerati
   * @param pdrColName     opzionale [[String]] il nome della colonna che identifica il codice pdr (default "pdr")
   * @param serviceColName opzionale [[String]] il nome della colonna che identifica il codice servizio (default "service")
   * @return [[DataFrame]] Tutte le misure dei soli pdr per i quali in dfMeasures c'è almeno una misura di tipo Im1/Igmg
   */
  def filterPDRsWithCMMeasure(dfMeasures: DataFrame, zInfExt: String, zSupExt: String, pdrColName: String = "pdr", serviceColName: String = "service"): DataFrame = {
    val windowSpec = Window.partitionBy(col(pdrColName))
    val changeMisCountColName = "changeMisCount"
    val zInfColName = "z_inf"
    val zSupColName = "z_sup"
    val dateColName = "date"
    val supExt = lit(zSupExt).cast(DateType)
    val infExt = lit(zInfExt).cast(DateType)
    val minDate = date_sub(infExt, 1)

    val measuresWithCount = dfMeasures
      .withColumn(zInfColName, max(when(col(dateColName) < infExt, col(dateColName))).over(windowSpec))
      .withColumn(zInfColName, coalesce(col(zInfColName), minDate))
      .withColumn(zSupColName, max(when((col(dateColName) <= supExt) and (col(dateColName) >= infExt), col(dateColName))).over(windowSpec))
      .withColumn(zSupColName, coalesce(col(zSupColName), supExt))
      .withColumn(changeMisCountColName, sum(when(isMeterChangeMeasure(col(serviceColName)) and (col(dateColName).cast(DateType) >= col(zInfColName)) and (col(dateColName).cast(DateType) <= col(zSupColName)), lit(1)).otherwise(lit(0))).over(windowSpec))

    measuresWithCount.where(col(changeMisCountColName) > 0)
      .drop(col(changeMisCountColName))
      .drop(col(zInfColName))
      .drop(col(zSupColName))

  }

  /**
   * Converte le misure in input di tipo [[RDD[Rows]]] in un [[Dataframe]]
   *
   * @param rows   [[RDD[Row]]] misure in input
   * @param schema [[StructType]] schema per la creazione del dataframe
   * @return
   */
  def toDataFrame(rows: RDD[Row], schema: StructType): DataFrame = {
    Environment.getSqlContext.createDataFrame(rows, schema)
  }

  def getMassivoExecutionId(): String = {
    val listPartitions = Environment.getSpark
      .sql(s"SHOW PARTITIONS ${Environment.getRcugasMassivoTableName}")
      .collect
      .toList
      .map(_.getString(0))

    val valuePartition = listPartitions
      .filter(value => value.contains("session=CCG"))
      .map(
        _.split("/")
          .filter(_.contains("execution_id"))
          .head
          .split("=").last
      )
      .sorted
      .reverse
      .head

    valuePartition
  }
}
