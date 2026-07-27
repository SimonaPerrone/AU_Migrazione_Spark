package it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded

import it.eng.au.aggregatoreConsumiCommon.schema.EsclusiOutputSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded.elencoFlussi.{ElencoFlussiDettaglioEsclusiSbg, RdbElencoFlussiEsclusiDettaglioSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded.pdr.{PdrDettaglioEsclusiSbg, RdbPdrEsclusiDettaglioSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.EsclusiTraitSbg

object RdbEsclusiSbg extends EsclusiTraitSbg {
  override val baseNumber: String = "2"
  override val pdrDettaglioEsclusi: PdrDettaglioEsclusiSbg = RdbPdrEsclusiDettaglioSbg
  override val elencoFlussiDettaglioEsclusi: ElencoFlussiDettaglioEsclusiSbg = RdbElencoFlussiEsclusiDettaglioSbg
  override val keyPiva1: String = EsclusiOutputSchema.piva_rdb
  override val keyPiva2: String = EsclusiOutputSchema.piva_rdb
  override val mainPiva: String = keyPiva1
}
