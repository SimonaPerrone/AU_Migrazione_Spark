package it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio

import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.elencoFlussi.{ElencoFlussiDettaglioIncoerenti, ItElencoFlussiIncoerentiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.pdr.{ItPdrIncoerentiDettaglio, PdrDettaglioIncoerenti}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.IncoerentiDettaglioTrait
import it.eng.au.aggregatoreConsumiCommon.schema.IncoerentiDettaglioSchema

object ItIncoerentiDettaglio extends IncoerentiDettaglioTrait {
  override val baseNumber: String = "3"
  override val pdrDettaglioIncoerenti: PdrDettaglioIncoerenti = ItPdrIncoerentiDettaglio
  override val elencoFlussiDettaglioIncoerenti: ElencoFlussiDettaglioIncoerenti = ItElencoFlussiIncoerentiDettaglio
  override val keyFields: List[String] = List(IncoerentiDettaglioSchema.piva_it)
  override val mainPiva: String = keyFields.head
}
