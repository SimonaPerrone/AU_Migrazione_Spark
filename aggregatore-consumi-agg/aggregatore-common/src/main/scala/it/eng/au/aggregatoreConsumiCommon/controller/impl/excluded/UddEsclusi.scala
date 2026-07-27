package it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded

import it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded.elencoFlussi.{ElencoFlussiDettaglioEsclusi, UddElencoFlussiEsclusiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded.pdr.{PdrDettaglioEsclusi, UddPdrEsclusiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.EsclusiTrait
import it.eng.au.aggregatoreConsumiCommon.schema.EsclusiOutputSchema

object UddEsclusi extends EsclusiTrait {
  override val baseNumber: String = "1"
  override val pdrDettaglioEsclusi: PdrDettaglioEsclusi = UddPdrEsclusiDettaglio
  override val elencoFlussiDettaglioEsclusi: ElencoFlussiDettaglioEsclusi = UddElencoFlussiEsclusiDettaglio
  override val keyPiva1: String = EsclusiOutputSchema.piva_udd
  override val keyPiva2: String = EsclusiOutputSchema.piva_distr
  override val mainPiva: String = keyPiva1
}
