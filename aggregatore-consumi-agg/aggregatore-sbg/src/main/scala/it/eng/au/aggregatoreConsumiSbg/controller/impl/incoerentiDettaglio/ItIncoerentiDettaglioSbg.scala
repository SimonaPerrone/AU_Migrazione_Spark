package it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio

import it.eng.au.aggregatoreConsumiCommon.controller.traits.IncoerentiDettaglioTrait
import it.eng.au.aggregatoreConsumiCommon.schema.IncoerentiDettaglioSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio.elencoFlussi.{ElencoFlussiDettaglioIncoerentiSbg, ItElencoFlussiIncoerentiDettaglioSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio.pdr.{ItPdrIncoerentiDettaglioSbg, PdrDettaglioIncoerentiSbg}

object ItIncoerentiDettaglioSbg extends IncoerentiDettaglioTrait {
  override val baseNumber: String = "3"
  override val pdrDettaglioIncoerenti: PdrDettaglioIncoerentiSbg = ItPdrIncoerentiDettaglioSbg
  override val elencoFlussiDettaglioIncoerenti: ElencoFlussiDettaglioIncoerentiSbg = ItElencoFlussiIncoerentiDettaglioSbg
  override val keyFields: List[String] = List(IncoerentiDettaglioSchema.piva_it)
  override val mainPiva: String = keyFields.head
}
