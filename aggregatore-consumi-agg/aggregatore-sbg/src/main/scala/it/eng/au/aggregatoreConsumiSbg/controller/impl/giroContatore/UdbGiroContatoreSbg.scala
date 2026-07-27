package it.eng.au.aggregatoreConsumiSbg.controller.impl.giroContatore

import it.eng.au.aggregatoreConsumiCommon.schema.GiroContatoreDettaglioSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.giroContatore.elencoFlussi.{ElencoFlussiDettaglioGiroContatoreSbg, UdbElencoFlussiDettaglioGiroContatoreSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.giroContatore.pdr.{PdrDettaglioGiroContatoreSbg, UdbPdrDettaglioGiroContatoreSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.GiroContatoreTraitSbg

object UdbGiroContatoreSbg extends GiroContatoreTraitSbg {
  override val baseNumber: String = "5"
  override val pdrDettaglioGiroContatore: PdrDettaglioGiroContatoreSbg = UdbPdrDettaglioGiroContatoreSbg
  override val elencoFlussiDettaglioGiroContatore: ElencoFlussiDettaglioGiroContatoreSbg = UdbElencoFlussiDettaglioGiroContatoreSbg
  override val keyPiva1: String = GiroContatoreDettaglioSchema.piva_udb
  override val keyPiva2: String = GiroContatoreDettaglioSchema.piva_udd
  override val mainPiva: String = keyPiva1
}
