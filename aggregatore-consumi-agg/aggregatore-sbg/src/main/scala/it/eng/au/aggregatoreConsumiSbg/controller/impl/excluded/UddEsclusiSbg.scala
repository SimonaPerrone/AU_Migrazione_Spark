package it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded

import it.eng.au.aggregatoreConsumiCommon.schema.EsclusiOutputSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded.elencoFlussi.{ElencoFlussiDettaglioEsclusiSbg, UddElencoFlussiEsclusiDettaglioSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded.pdr.{PdrDettaglioEsclusiSbg, UddPdrEsclusiDettaglioSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.EsclusiTraitSbg

object UddEsclusiSbg extends EsclusiTraitSbg {
  override val baseNumber: String = "1"
  override val pdrDettaglioEsclusi: PdrDettaglioEsclusiSbg = UddPdrEsclusiDettaglioSbg
  override val elencoFlussiDettaglioEsclusi: ElencoFlussiDettaglioEsclusiSbg = UddElencoFlussiEsclusiDettaglioSbg
  override val keyPiva1: String = EsclusiOutputSchema.piva_udd
  override val keyPiva2: String = EsclusiOutputSchema.piva_distr
  override val mainPiva: String = keyPiva1
}
