package it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded

import it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded.elencoFlussi.{ElencoFlussiDettaglioEsclusi, UdbElencoFlussiEsclusiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded.pdr.{PdrDettaglioEsclusi, UdbPdrEsclusiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.EsclusiTrait
import it.eng.au.aggregatoreConsumiCommon.schema.EsclusiOutputSchema

object UdbEsclusi extends EsclusiTrait {
  override val baseNumber: String = "5"
  override val pdrDettaglioEsclusi: PdrDettaglioEsclusi = UdbPdrEsclusiDettaglio
  override val elencoFlussiDettaglioEsclusi: ElencoFlussiDettaglioEsclusi = UdbElencoFlussiEsclusiDettaglio
  override val keyPiva1: String = EsclusiOutputSchema.piva_udb
  override val keyPiva2: String = EsclusiOutputSchema.piva_udd
  override val mainPiva: String = keyPiva1
}
