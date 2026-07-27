package it.au.misure.ee_switching.filterPod

import java.time.LocalDate
import java.sql.Timestamp
import org.apache.spark.sql.DataFrame

trait FilterPod {

  val partitioningColumns: Seq[String]

  def partitioningColumnsFilter(df: DataFrame, listaDate: Seq[LocalDate]): DataFrame

  def ordinaryRunFilter(df: DataFrame, firstDate: LocalDate, secondDate: LocalDate): DataFrame

  def timestampFilter(df: DataFrame, timestamp: Timestamp): DataFrame =
    df.filter(df.col("d_caricamento") === timestamp)

  def podFilter(df: DataFrame, listaPod: Seq[String]): DataFrame =
     df.filter(df.col("pod14").isin(listaPod:_*))

  def distributoreFilter(df: DataFrame, listaDistributori: Seq[String]): DataFrame =
    df.filter(df.col("piva_distr").isin(listaDistributori:_*))

  def uddFilter(df: DataFrame, listaUdd: Seq[String]): DataFrame =
    df.filter(df.col("piva_udd").isin(listaUdd:_*))

  def dateFilter(df: DataFrame, listaDateSW: Seq[LocalDate], listaDateNA: Seq[LocalDate]): DataFrame

  def coppieDistrUddFilter(df: DataFrame, listaCoppieDistrUdd: Seq[(String, String)]): DataFrame = {
    df.filter(listaCoppieDistrUdd.map( { case (distr, udd) =>
      s"(piva_distr='${distr}'AND piva_udd='${udd}')" }).mkString("OR"))
  }

  def entriesExclusionFilter(df: DataFrame): DataFrame

}
