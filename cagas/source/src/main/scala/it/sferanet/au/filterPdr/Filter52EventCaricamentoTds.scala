package it.sferanet.au.filterPdr

import it.sferanet.au.schema.SettleGasGasTdsSchema
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Filter52EventCaricamentoTds extends FilterPdr {
  override def getPdrs: RDD[String] = {
    val settleGasGasTds: DataFrame = getSettleGasGasTds
    val dataGhigliottina: String = Environment.getTdsReceiveEndDate

    settleGasGasTds
      .where(col(SettleGasGasTdsSchema.valid) === true)
      .where(dataCreazioneTimestampToDate(settleGasGasTds(SettleGasGasTdsSchema.data_creazione)).between(lit(dataGhigliottina), current_date()))
      .select(SettleGasGasTdsSchema.cod_pdr)
      .distinct()
      .rdd
      .map(_.getString(0))
  }
}
