package it.au.misure.ee_switching.filterPod

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import it.au.misure.ee_switching.model.schema.hive.FunzionaliSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.not

object FilterFunzionali extends FilterPod {

  override val partitioningColumns: Seq[String] = Seq(FunzionaliSchema.annomese_sw)

  override def partitioningColumnsFilter(df: DataFrame, listaDate: Seq[LocalDate]): DataFrame = {
    val listaAnnoMese = listaDate.map(d => d.format(DateTimeFormatter.ofPattern("yyyyMM")))
    df.filter(df.col(partitioningColumns(0)).isin(listaAnnoMese:_*))
  }

  override def ordinaryRunFilter(df: DataFrame, firstDate: LocalDate, secondDate: LocalDate): DataFrame = {
    val annoMeseFirstDate = firstDate.format(DateTimeFormatter.ofPattern("yyyyMM"))
    val annoMeseSecondDate = secondDate.format(DateTimeFormatter.ofPattern("yyyyMM"))

    df.filter((not(df.col(FunzionaliSchema.is_nuova_attivazione)) &&
                df.col(FunzionaliSchema.annomese_sw) === annoMeseSecondDate)
              ||
              (df.col(FunzionaliSchema.is_nuova_attivazione) &&
                df.col(FunzionaliSchema.annomese_sw) === annoMeseFirstDate))
  }

  override def dateFilter(df: DataFrame, listaDateSW: Seq[LocalDate], listaDateNA: Seq[LocalDate]): DataFrame = {
    val listaDateFormattate = listaDateSW.map(date => date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))
    val listaDateNAFormattate = listaDateNA.map(date => date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))

    if (listaDateSW.nonEmpty && listaDateNA.nonEmpty) {
      df.filter((not(df.col(FunzionaliSchema.is_nuova_attivazione)) &&
                  df.col(FunzionaliSchema.d_data_decorrenza).isin(listaDateFormattate:_*))
                ||
                (df.col(FunzionaliSchema.is_nuova_attivazione) &&
                  df.col(FunzionaliSchema.d_data_decorrenza).isin(listaDateNAFormattate:_*)))
    } else if (listaDateSW.nonEmpty)
      df.filter(not(df.col(FunzionaliSchema.is_nuova_attivazione)) &&
                  df.col(FunzionaliSchema.d_data_decorrenza).isin(listaDateFormattate:_*))
    else
      df.filter(df.col(FunzionaliSchema.is_nuova_attivazione) &&
                  df.col(FunzionaliSchema.d_data_decorrenza).isin(listaDateNAFormattate:_*))
  }

  override def entriesExclusionFilter(df: DataFrame): DataFrame =
    df.filter((df.col(FunzionaliSchema.nome_flusso) !== "XXX") && (df.col(FunzionaliSchema.t_cod_contr_disp).isNotNull))

}
