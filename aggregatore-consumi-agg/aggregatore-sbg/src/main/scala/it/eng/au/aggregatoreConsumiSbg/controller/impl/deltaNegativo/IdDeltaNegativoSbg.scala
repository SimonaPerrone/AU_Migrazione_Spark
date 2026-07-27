package it.eng.au.aggregatoreConsumiSbg.controller.impl.deltaNegativo

import it.eng.au.aggregatoreConsumiCommon.schema.DeltaNegativoDettaglioSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.deltaNegativo.elencoFlussi.{ElencoFlussiDettaglioDeltaNegativoSbg, IdElencoFlussiDettaglioDeltaNegativoSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.deltaNegativo.pdr.{IdPdrDettaglioDeltaNegativoSbg, PdrDettaglioDeltaNegativoSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.DeltaNegativoTraitSbg

object IdDeltaNegativoSbg extends DeltaNegativoTraitSbg {
  override val baseNumber: String = "4"
  override val pdrDettaglioDeltaNegativo: PdrDettaglioDeltaNegativoSbg = IdPdrDettaglioDeltaNegativoSbg
  override val elencoFlussiDettaglioDeltaNegativo: ElencoFlussiDettaglioDeltaNegativoSbg = IdElencoFlussiDettaglioDeltaNegativoSbg
  override val keyPiva1: String = DeltaNegativoDettaglioSchema.piva_distr
  override val keyPiva2: String = DeltaNegativoDettaglioSchema.piva_udd
  override val mainPiva: String = keyPiva1
}
