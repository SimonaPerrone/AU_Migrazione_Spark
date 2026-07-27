package it.au.misure.ee_switching.filterPod

import it.au.misure.ee_switching.args.FlowArgsConfig
import it.au.misure.ee_switching.utility.Constants.{FUNZIONALI, STORICI}

import org.apache.spark.sql.DataFrame

import java.time.LocalDate

object FilterPodFactory {

  def filter(df: DataFrame, flowName: String, params: FlowArgsConfig): DataFrame = {

    val filter = flowName match {
      case FUNZIONALI => FilterFunzionali
      case STORICI => FilterStorici
    }

    var filteringDf: DataFrame = df

    if (params.runOrdinaria) {

      val today: LocalDate = LocalDate.now
      filteringDf = filter.partitioningColumnsFilter(filteringDf, Seq(today, today.plusMonths(1)))
      filteringDf = filter.ordinaryRunFilter(filteringDf, today, today.plusMonths(1))

    } else {

      if (params.listaDateSW.nonEmpty || params.listaDateNA.nonEmpty) {
        filteringDf = filter.partitioningColumnsFilter(filteringDf, params.listaDateSW ++ params.listaDateNA)
        filteringDf = filter.dateFilter(filteringDf, params.listaDateSW, params.listaDateNA)
      }
      if (params.timestamp != null)
        filteringDf = filter.timestampFilter(filteringDf, params.timestamp)

    }

    if (params.listaPod.nonEmpty)
      filteringDf = filter.podFilter(filteringDf, params.listaPod)
    if (params.listaDistributori.nonEmpty)
      filteringDf = filter.distributoreFilter(filteringDf, params.listaDistributori)
    if (params.listaUdd.nonEmpty)
      filteringDf = filter.uddFilter(filteringDf, params.listaUdd)
    if (params.listaCoppieDistrUdd.nonEmpty)
      filteringDf = filter.coppieDistrUddFilter(filteringDf, params.listaCoppieDistrUdd)

    filteringDf = filter.entriesExclusionFilter(filteringDf)

    filteringDf
  }

}
