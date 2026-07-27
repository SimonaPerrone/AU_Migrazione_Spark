package it.eng.au.aggregatoreConsumiCommon.controller.impl.deltaNegativo

import it.eng.au.aggregatoreConsumiCommon.controller.impl.deltaNegativo.elencoFlussi.{ElencoFlussiDettaglioDeltaNegativo, IdElencoFlussiDettaglioDeltaNegativo}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.deltaNegativo.pdr.{IdPdrDettaglioDeltaNegativo, PdrDettaglioDeltaNegativo}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.DeltaNegativoTrait
import it.eng.au.aggregatoreConsumiCommon.schema.DeltaNegativoDettaglioSchema

object IdDeltaNegativo extends DeltaNegativoTrait {
  override val baseNumber: String = "4"
  override val pdrDettaglioDeltaNegativo: PdrDettaglioDeltaNegativo = IdPdrDettaglioDeltaNegativo
  override val elencoFlussiDettaglioDeltaNegativo: ElencoFlussiDettaglioDeltaNegativo = IdElencoFlussiDettaglioDeltaNegativo
  override val keyPiva1: String = DeltaNegativoDettaglioSchema.piva_distr
  override val keyPiva2: String = DeltaNegativoDettaglioSchema.piva_udd
  override val mainPiva: String = keyPiva1
}
