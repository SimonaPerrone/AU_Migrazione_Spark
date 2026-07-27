package it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio

import it.eng.au.aggregatoreConsumiCommon.controller.traits.IncoerentiDettaglioTrait
import it.eng.au.aggregatoreConsumiCommon.schema.IncoerentiDettaglioSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio.elencoFlussi.{ElencoFlussiDettaglioIncoerentiSbg, UddElencoFlussiIncoerentiDettaglioSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio.pdr.{PdrDettaglioIncoerentiSbg, UddPdrIncoerentiDettaglioSbg}

object UddIncoerentiDettaglioSbg extends IncoerentiDettaglioTrait {
  override val baseNumber: String = "1"
  override val pdrDettaglioIncoerenti: PdrDettaglioIncoerentiSbg = UddPdrIncoerentiDettaglioSbg
  override val elencoFlussiDettaglioIncoerenti: ElencoFlussiDettaglioIncoerentiSbg = UddElencoFlussiIncoerentiDettaglioSbg
  override val keyFields: List[String] = List(IncoerentiDettaglioSchema.piva_udd)
  override val mainPiva: String = keyFields.head
}
