package it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio

import it.eng.au.aggregatoreConsumiCommon.controller.traits.IncoerentiDettaglioTrait
import it.eng.au.aggregatoreConsumiCommon.schema.IncoerentiDettaglioSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio.elencoFlussi.{ElencoFlussiDettaglioIncoerentiSbg, IdElencoFlussiIncoerentiDettaglioSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio.pdr.{IdPdrIncoerentiDettaglioSbg, PdrDettaglioIncoerentiSbg}

object IdIncoerentiDettaglioSbg extends IncoerentiDettaglioTrait {
  override val baseNumber: String = "4"
  override val pdrDettaglioIncoerenti: PdrDettaglioIncoerentiSbg = IdPdrIncoerentiDettaglioSbg
  override val elencoFlussiDettaglioIncoerenti: ElencoFlussiDettaglioIncoerentiSbg = IdElencoFlussiIncoerentiDettaglioSbg
  override val keyFields: List[String] = List(IncoerentiDettaglioSchema.piva_distr)
  override val mainPiva: String = keyFields.head
}
