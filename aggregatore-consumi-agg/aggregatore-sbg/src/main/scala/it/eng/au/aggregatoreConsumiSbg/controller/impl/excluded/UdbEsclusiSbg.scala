package it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded

import it.eng.au.aggregatoreConsumiCommon.schema.EsclusiOutputSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded.elencoFlussi.{ElencoFlussiDettaglioEsclusiSbg, UdbElencoFlussiEsclusiDettaglioSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded.pdr.{PdrDettaglioEsclusiSbg, UdbPdrEsclusiDettaglioSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.EsclusiTraitSbg

object UdbEsclusiSbg extends EsclusiTraitSbg {
  override val baseNumber: String = "5"
  override val pdrDettaglioEsclusi: PdrDettaglioEsclusiSbg = UdbPdrEsclusiDettaglioSbg
  override val elencoFlussiDettaglioEsclusi: ElencoFlussiDettaglioEsclusiSbg = UdbElencoFlussiEsclusiDettaglioSbg
  override val keyPiva1: String = EsclusiOutputSchema.piva_udb
  override val keyPiva2: String = EsclusiOutputSchema.piva_udd
  override val mainPiva: String = keyPiva1
}