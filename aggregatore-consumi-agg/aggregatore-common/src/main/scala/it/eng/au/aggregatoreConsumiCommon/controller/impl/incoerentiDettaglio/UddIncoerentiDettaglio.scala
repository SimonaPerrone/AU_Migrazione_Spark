package it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio

import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.elencoFlussi.{ElencoFlussiDettaglioIncoerenti, UddElencoFlussiIncoerentiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.pdr.{PdrDettaglioIncoerenti, UddPdrIncoerentiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.IncoerentiDettaglioTrait
import it.eng.au.aggregatoreConsumiCommon.schema.IncoerentiDettaglioSchema

object UddIncoerentiDettaglio extends IncoerentiDettaglioTrait {
  override val baseNumber: String = "1"
  override val pdrDettaglioIncoerenti: PdrDettaglioIncoerenti = UddPdrIncoerentiDettaglio
  override val elencoFlussiDettaglioIncoerenti: ElencoFlussiDettaglioIncoerenti = UddElencoFlussiIncoerentiDettaglio
  override val keyFields: List[String] = List(IncoerentiDettaglioSchema.piva_udd)
  override val mainPiva: String = keyFields.head
}
