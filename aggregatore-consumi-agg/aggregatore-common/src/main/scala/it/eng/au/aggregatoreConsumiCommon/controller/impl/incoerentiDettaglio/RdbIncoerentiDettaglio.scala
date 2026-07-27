package it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio

import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.elencoFlussi.{ElencoFlussiDettaglioIncoerenti, RdbElencoFlussiIncoerentiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.pdr.{PdrDettaglioIncoerenti, RdbPdrIncoerentiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.IncoerentiDettaglioTrait
import it.eng.au.aggregatoreConsumiCommon.schema.IncoerentiDettaglioSchema

object RdbIncoerentiDettaglio extends IncoerentiDettaglioTrait {
  override val baseNumber: String = "2"
  override val pdrDettaglioIncoerenti: PdrDettaglioIncoerenti = RdbPdrIncoerentiDettaglio
  override val elencoFlussiDettaglioIncoerenti: ElencoFlussiDettaglioIncoerenti = RdbElencoFlussiIncoerentiDettaglio
  override val keyFields: List[String] = List(IncoerentiDettaglioSchema.piva_rdb)
  override val mainPiva: String = keyFields.head
}
