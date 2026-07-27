package it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded

import it.eng.au.aggregatoreConsumiCommon.schema.EsclusiOutputSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded.elencoFlussi.{ElencoFlussiDettaglioEsclusiSbg, IdElencoFlussiEsclusiDettaglioSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded.pdr.{IdPdrEsclusiDettaglioSbg, PdrDettaglioEsclusiSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.EsclusiTraitSbg

object IdEsclusiSbg extends EsclusiTraitSbg {
  override val baseNumber: String = "4"
  override val pdrDettaglioEsclusi: PdrDettaglioEsclusiSbg = IdPdrEsclusiDettaglioSbg
  override val elencoFlussiDettaglioEsclusi: ElencoFlussiDettaglioEsclusiSbg = IdElencoFlussiEsclusiDettaglioSbg
  override val keyPiva1: String = EsclusiOutputSchema.piva_distr
  override val keyPiva2: String = EsclusiOutputSchema.piva_udd
  override val mainPiva: String = keyPiva1
}