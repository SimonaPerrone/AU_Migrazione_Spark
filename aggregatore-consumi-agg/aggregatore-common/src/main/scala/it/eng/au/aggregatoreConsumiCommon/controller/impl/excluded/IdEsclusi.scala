package it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded

import it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded.elencoFlussi.{ElencoFlussiDettaglioEsclusi, IdElencoFlussiEsclusiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded.pdr.{IdPdrEsclusiDettaglio, PdrDettaglioEsclusi}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.EsclusiTrait
import it.eng.au.aggregatoreConsumiCommon.schema.EsclusiOutputSchema

object IdEsclusi extends EsclusiTrait {
  override val baseNumber: String = "4"
  override val pdrDettaglioEsclusi: PdrDettaglioEsclusi = IdPdrEsclusiDettaglio
  override val elencoFlussiDettaglioEsclusi: ElencoFlussiDettaglioEsclusi = IdElencoFlussiEsclusiDettaglio
  override val keyPiva1: String = EsclusiOutputSchema.piva_distr
  override val keyPiva2: String = EsclusiOutputSchema.piva_udd
  override val mainPiva: String = keyPiva1
}
