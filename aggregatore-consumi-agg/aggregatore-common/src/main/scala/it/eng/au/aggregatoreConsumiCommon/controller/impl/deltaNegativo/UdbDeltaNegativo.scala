package it.eng.au.aggregatoreConsumiCommon.controller.impl.deltaNegativo

import it.eng.au.aggregatoreConsumiCommon.controller.impl.deltaNegativo.elencoFlussi.{ElencoFlussiDettaglioDeltaNegativo, UdbElencoFlussiDettaglioDeltaNegativo}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.deltaNegativo.pdr.{PdrDettaglioDeltaNegativo, UdbPdrDettaglioDeltaNegativo}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.DeltaNegativoTrait
import it.eng.au.aggregatoreConsumiCommon.schema.DeltaNegativoDettaglioSchema

object UdbDeltaNegativo extends DeltaNegativoTrait {
  override val baseNumber: String = "5"
  override val pdrDettaglioDeltaNegativo: PdrDettaglioDeltaNegativo = UdbPdrDettaglioDeltaNegativo
  override val elencoFlussiDettaglioDeltaNegativo: ElencoFlussiDettaglioDeltaNegativo = UdbElencoFlussiDettaglioDeltaNegativo
  override val keyPiva1: String = DeltaNegativoDettaglioSchema.piva_udb
  override val keyPiva2: String = DeltaNegativoDettaglioSchema.piva_udd
  override val mainPiva: String = keyPiva1
}
