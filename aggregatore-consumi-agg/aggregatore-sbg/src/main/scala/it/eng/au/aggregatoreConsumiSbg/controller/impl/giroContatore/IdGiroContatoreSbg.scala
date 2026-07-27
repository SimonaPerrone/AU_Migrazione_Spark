package it.eng.au.aggregatoreConsumiSbg.controller.impl.giroContatore

import it.eng.au.aggregatoreConsumiCommon.schema.GiroContatoreDettaglioSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.giroContatore.elencoFlussi.{ElencoFlussiDettaglioGiroContatoreSbg, IdElencoFlussiDettaglioGiroContatoreSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.giroContatore.pdr.{IdPdrDettaglioGiroContatoreSbg, PdrDettaglioGiroContatoreSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.GiroContatoreTraitSbg

object IdGiroContatoreSbg extends GiroContatoreTraitSbg {
  override val baseNumber: String = "4"
  override val pdrDettaglioGiroContatore: PdrDettaglioGiroContatoreSbg = IdPdrDettaglioGiroContatoreSbg
  override val elencoFlussiDettaglioGiroContatore: ElencoFlussiDettaglioGiroContatoreSbg = IdElencoFlussiDettaglioGiroContatoreSbg
  override val keyPiva1: String = GiroContatoreDettaglioSchema.piva_distr
  override val keyPiva2: String = GiroContatoreDettaglioSchema.piva_udd
  override val mainPiva: String = keyPiva1
}
