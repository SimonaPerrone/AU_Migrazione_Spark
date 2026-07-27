package it.eng.au.aggregatoreConsumiCommon.controller.impl.giroContatore

import it.eng.au.aggregatoreConsumiCommon.controller.impl.giroContatore.elencoFlussi.{ElencoFlussiDettaglioGiroContatore, IdElencoFlussiDettaglioGiroContatore}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.giroContatore.pdr.{IdPdrDettaglioGiroContatore, PdrDettaglioGiroContatore}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.GiroContatoreTrait
import it.eng.au.aggregatoreConsumiCommon.schema.GiroContatoreDettaglioSchema

object IdGiroContatore extends GiroContatoreTrait {
  override val baseNumber: String = "4"
  override val pdrDettaglioGiroContatore: PdrDettaglioGiroContatore = IdPdrDettaglioGiroContatore
  override val elencoFlussiDettaglioGiroContatore: ElencoFlussiDettaglioGiroContatore = IdElencoFlussiDettaglioGiroContatore
  override val keyPiva1: String = GiroContatoreDettaglioSchema.piva_distr
  override val keyPiva2: String = GiroContatoreDettaglioSchema.piva_udd
  override val mainPiva: String = keyPiva1
}
