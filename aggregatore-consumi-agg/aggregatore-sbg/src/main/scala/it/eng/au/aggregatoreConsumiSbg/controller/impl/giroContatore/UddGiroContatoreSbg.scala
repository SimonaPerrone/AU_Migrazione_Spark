package it.eng.au.aggregatoreConsumiSbg.controller.impl.giroContatore

import it.eng.au.aggregatoreConsumiCommon.schema.GiroContatoreDettaglioSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.giroContatore.elencoFlussi.{ElencoFlussiDettaglioGiroContatoreSbg, UddElencoFlussiDettaglioGiroContatoreSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.giroContatore.pdr.{PdrDettaglioGiroContatoreSbg, UddPdrDettaglioGiroContatoreSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.GiroContatoreTraitSbg

object UddGiroContatoreSbg extends GiroContatoreTraitSbg {
  override val baseNumber: String = "1"
  override val pdrDettaglioGiroContatore: PdrDettaglioGiroContatoreSbg = UddPdrDettaglioGiroContatoreSbg
  override val elencoFlussiDettaglioGiroContatore: ElencoFlussiDettaglioGiroContatoreSbg = UddElencoFlussiDettaglioGiroContatoreSbg
  override val keyPiva1: String = GiroContatoreDettaglioSchema.piva_udd
  override val keyPiva2: String = GiroContatoreDettaglioSchema.piva_distr
  override val mainPiva: String = keyPiva1
}
