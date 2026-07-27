package it.eng.au.aggregatoreConsumiCommon.controller.impl.giroContatore

import it.eng.au.aggregatoreConsumiCommon.controller.impl.giroContatore.elencoFlussi.{ElencoFlussiDettaglioGiroContatore, UddElencoFlussiDettaglioGiroContatore}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.giroContatore.pdr.{PdrDettaglioGiroContatore, UddPdrDettaglioGiroContatore}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.GiroContatoreTrait
import it.eng.au.aggregatoreConsumiCommon.schema.GiroContatoreDettaglioSchema

object UddGiroContatore extends GiroContatoreTrait {
  override val baseNumber: String = "1"
  override val pdrDettaglioGiroContatore: PdrDettaglioGiroContatore = UddPdrDettaglioGiroContatore
  override val elencoFlussiDettaglioGiroContatore: ElencoFlussiDettaglioGiroContatore = UddElencoFlussiDettaglioGiroContatore
  override val keyPiva1: String = GiroContatoreDettaglioSchema.piva_udd
  override val keyPiva2: String = GiroContatoreDettaglioSchema.piva_distr
  override val mainPiva: String = keyPiva1
}
