package it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio

import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.elencoFlussi.{ElencoFlussiDettaglioIncoerenti, IdElencoFlussiIncoerentiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.pdr.{IdPdrIncoerentiDettaglio, PdrDettaglioIncoerenti}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.IncoerentiDettaglioTrait
import it.eng.au.aggregatoreConsumiCommon.schema.IncoerentiDettaglioSchema

object IdIncoerentiDettaglio extends IncoerentiDettaglioTrait {
  override val baseNumber: String = "4"
  override val pdrDettaglioIncoerenti: PdrDettaglioIncoerenti = IdPdrIncoerentiDettaglio
  override val elencoFlussiDettaglioIncoerenti: ElencoFlussiDettaglioIncoerenti = IdElencoFlussiIncoerentiDettaglio
  override val keyFields: List[String] = List(IncoerentiDettaglioSchema.piva_distr)
  override val mainPiva: String = keyFields.head
}
