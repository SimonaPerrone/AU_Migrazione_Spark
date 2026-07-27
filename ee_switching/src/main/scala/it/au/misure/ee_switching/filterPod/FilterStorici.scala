package it.au.misure.ee_switching.filterPod

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import it.au.misure.ee_switching.model.schema.hive.StoriciSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import Array.range

object FilterStorici extends FilterPod {

  override val partitioningColumns: Seq[String] = Seq(StoriciSchema.annomese_sw, StoriciSchema.nome_flusso)

  override def partitioningColumnsFilter(df: DataFrame, listaDate: Seq[LocalDate]): DataFrame = {
    val listaAnnoMese = listaDate.map(d => d.format(DateTimeFormatter.ofPattern("yyyyMM")))
    df.filter(df.col(partitioningColumns(0)).isin(listaAnnoMese:_*))
  }

  override def ordinaryRunFilter(df: DataFrame, firstDate: LocalDate, secondDate: LocalDate): DataFrame = {
    val annoMeseFirstDate = firstDate.format(DateTimeFormatter.ofPattern("yyyyMM"))
    val annoMeseSecondDate = secondDate.format(DateTimeFormatter.ofPattern("yyyyMM"))

    val previousYearMonth: String = firstDate.minusMonths(1).getYear.toString + firstDate.minusMonths(1).getMonthValue.toString
    val measuresYearMonthsList: Seq[String] = range(1, 13)
      .map(monthToSubstract =>
        firstDate.minusMonths(monthToSubstract).getYear.toString + firstDate.minusMonths(monthToSubstract).getMonthValue.toString).toSeq

    df.filter(
      (df.col(StoriciSchema.trattamento_online) === "O"
        && df.col(StoriciSchema.annomese_sw) === annoMeseSecondDate
        && concat(df.col(StoriciSchema.anno_misura), df.col(StoriciSchema.mese_misura)).isin(measuresYearMonthsList:_*))
      ||
      (df.col(StoriciSchema.trattamento_online) === "O"
        && df.col(StoriciSchema.annomese_sw) === annoMeseFirstDate
        && concat(df.col(StoriciSchema.anno_misura), df.col(StoriciSchema.mese_misura)) === previousYearMonth)
      ||
      ((df.col(StoriciSchema.trattamento_online) !== "O") && df.col(StoriciSchema.annomese_sw) === annoMeseFirstDate
        && concat(df.col(StoriciSchema.anno_misura), df.col(StoriciSchema.mese_misura)).isin(measuresYearMonthsList:_*))
      )
  }

  override def dateFilter(df: DataFrame, listaDateSW: Seq[LocalDate], listaDateNA: Seq[LocalDate]): DataFrame = {
    // la Seq listaDateNA sarà sicuramente vuota per i flussi storici (vedi check parametri iniziale)
    if (listaDateSW.length == 1) {
      val dataFormattata = listaDateSW.head.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
      val measuresYearMonthsList: Seq[String] = range(1, 13)
        .map(monthToSubstract =>
          listaDateSW.head.minusMonths(1 + monthToSubstract).getYear.toString + listaDateSW.head.minusMonths(1 + monthToSubstract).getMonthValue.toString).toSeq
      df.filter(df.col(StoriciSchema.d_data_decorrenza) === dataFormattata
        && concat(df.col(StoriciSchema.anno_misura), df.col(StoriciSchema.mese_misura)).isin(measuresYearMonthsList:_*))
    } else if (listaDateSW.length == 2) // (vedi check parametri iniziale)
      ordinaryRunFilter(df, listaDateSW(0), listaDateSW(1))
    else
      throw new IllegalArgumentException("I flussi storici possono ricevere al massimo due date come parametro per il filtraggio")
  }

  override def entriesExclusionFilter(df: DataFrame): DataFrame =
    df.filter((df.col(StoriciSchema.nome_flusso) !== "XXX") && (df.col(StoriciSchema.dp).isNotNull))

}
