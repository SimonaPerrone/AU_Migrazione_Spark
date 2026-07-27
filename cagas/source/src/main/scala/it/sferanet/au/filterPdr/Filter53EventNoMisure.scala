package it.sferanet.au.filterPdr

import it.sferanet.au.schema.CaPreFinalSchema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDate

class Filter53EventNoMisure extends FilterPdr {
  override def getPdrs: RDD[String] = {
    val today: LocalDate = LocalDate.now()
    val currentThermalYear = getCurrentThermalYear(today)
    val caPreFinal: DataFrame = getCaPreFinal

    caPreFinal.where(col(CaPreFinalSchema.anno_competenza) < lit(currentThermalYear))
      .where(col(CaPreFinalSchema.is_ca_calculated) === true)
      .where(col(CaPreFinalSchema.trattamento) === lit("Y"))
      .select(CaPreFinalSchema.codice_pdr)
      .distinct()
      .rdd
      .map(_.getString(0))
  }
}
