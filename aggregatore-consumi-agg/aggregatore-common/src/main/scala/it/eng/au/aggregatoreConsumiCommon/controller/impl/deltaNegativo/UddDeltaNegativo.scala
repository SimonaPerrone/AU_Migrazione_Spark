package it.eng.au.aggregatoreConsumiCommon.controller.impl.deltaNegativo

import it.eng.au.aggregatoreConsumiCommon.controller.impl.deltaNegativo.elencoFlussi.{ElencoFlussiDettaglioDeltaNegativo, UddElencoFlussiDettaglioDeltaNegativo}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.deltaNegativo.pdr.{PdrDettaglioDeltaNegativo, UddPdrDettaglioDeltaNegativo}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.DeltaNegativoTrait
import it.eng.au.aggregatoreConsumiCommon.schema.DeltaNegativoDettaglioSchema

object UddDeltaNegativo extends DeltaNegativoTrait {
  override val baseNumber: String = "1"
  override val pdrDettaglioDeltaNegativo: PdrDettaglioDeltaNegativo = UddPdrDettaglioDeltaNegativo
  override val elencoFlussiDettaglioDeltaNegativo: ElencoFlussiDettaglioDeltaNegativo = UddElencoFlussiDettaglioDeltaNegativo
  override val keyPiva1: String = DeltaNegativoDettaglioSchema.piva_udd
  override val keyPiva2: String = DeltaNegativoDettaglioSchema.piva_distr
  override val mainPiva: String = keyPiva1
}
