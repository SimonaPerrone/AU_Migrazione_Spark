package it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio

import it.eng.au.aggregatoreConsumiCommon.controller.traits.IncoerentiDettaglioTrait
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, IncoerentiDettaglioSchema}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio.elencoFlussi.{ElencoFlussiDettaglioIncoerentiSbg, UdbElencoFlussiIncoerentiDettaglioSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio.pdr.{PdrDettaglioIncoerentiSbg, UdbPdrIncoerentiDettaglioSbg}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object UdbIncoerentiDettaglioSbg extends IncoerentiDettaglioTrait {
  override val baseNumber: String = "5"
  override val pdrDettaglioIncoerenti: PdrDettaglioIncoerentiSbg = UdbPdrIncoerentiDettaglioSbg
  override val elencoFlussiDettaglioIncoerenti: ElencoFlussiDettaglioIncoerentiSbg = UdbElencoFlussiIncoerentiDettaglioSbg
  override val keyFields: List[String] = List(IncoerentiDettaglioSchema.piva_udb)
  override val mainPiva: String = keyFields.head

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaUdb).isNotNull
}
