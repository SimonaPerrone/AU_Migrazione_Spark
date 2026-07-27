package it.eng.au.aggregatoreConsumiCommon.controller.impl.giroContatore

import it.eng.au.aggregatoreConsumiCommon.controller.impl.giroContatore.elencoFlussi.{ElencoFlussiDettaglioGiroContatore, UdbElencoFlussiDettaglioGiroContatore}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.giroContatore.pdr.{PdrDettaglioGiroContatore, UdbPdrDettaglioGiroContatore}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.GiroContatoreTrait
import it.eng.au.aggregatoreConsumiCommon.schema.GiroContatoreDettaglioSchema

object UdbGiroContatore extends GiroContatoreTrait {
  override val baseNumber: String = "5"
  override val pdrDettaglioGiroContatore: PdrDettaglioGiroContatore = UdbPdrDettaglioGiroContatore
  override val elencoFlussiDettaglioGiroContatore: ElencoFlussiDettaglioGiroContatore = UdbElencoFlussiDettaglioGiroContatore
  override val keyPiva1: String = GiroContatoreDettaglioSchema.piva_udb
  override val keyPiva2: String = GiroContatoreDettaglioSchema.piva_udd
  override val mainPiva: String = keyPiva1
}
