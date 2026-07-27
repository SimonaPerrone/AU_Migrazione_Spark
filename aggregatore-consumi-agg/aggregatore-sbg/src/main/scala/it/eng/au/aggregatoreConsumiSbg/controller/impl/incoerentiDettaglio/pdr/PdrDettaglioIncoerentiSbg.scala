package it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio.pdr

import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.pdr.PdrDettaglioIncoerenti
import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

trait PdrDettaglioIncoerentiSbg extends PdrDettaglioIncoerenti {
  override def specificFilterForIncoerentiGdm: Column = col(DailyConsumptionAggSchema.treatment).isin("G","M")
}