package it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio

import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.elencoFlussi.{ElencoFlussiDettaglioIncoerenti, UdbElencoFlussiIncoerentiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.pdr.{PdrDettaglioIncoerenti, UdbPdrIncoerentiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.IncoerentiDettaglioTrait
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, IncoerentiDettaglioSchema}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object UdbIncoerentiDettaglio extends IncoerentiDettaglioTrait {
  override val baseNumber: String = "5"
  override val pdrDettaglioIncoerenti: PdrDettaglioIncoerenti = UdbPdrIncoerentiDettaglio
  override val elencoFlussiDettaglioIncoerenti: ElencoFlussiDettaglioIncoerenti = UdbElencoFlussiIncoerentiDettaglio
  override val keyFields: List[String] = List(IncoerentiDettaglioSchema.piva_udb)
  override val mainPiva: String = keyFields.head

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaUdb).isNotNull
}
