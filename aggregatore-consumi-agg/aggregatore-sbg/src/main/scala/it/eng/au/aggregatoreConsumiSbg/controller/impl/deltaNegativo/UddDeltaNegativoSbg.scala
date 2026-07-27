package it.eng.au.aggregatoreConsumiSbg.controller.impl.deltaNegativo

import it.eng.au.aggregatoreConsumiCommon.schema.DeltaNegativoDettaglioSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.deltaNegativo.elencoFlussi.{ElencoFlussiDettaglioDeltaNegativoSbg, UddElencoFlussiDettaglioDeltaNegativoSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.deltaNegativo.pdr.{PdrDettaglioDeltaNegativoSbg, UddPdrDettaglioDeltaNegativoSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.DeltaNegativoTraitSbg

object UddDeltaNegativoSbg extends DeltaNegativoTraitSbg {
  override val baseNumber: String = "1"
  override val pdrDettaglioDeltaNegativo: PdrDettaglioDeltaNegativoSbg = UddPdrDettaglioDeltaNegativoSbg
  override val elencoFlussiDettaglioDeltaNegativo: ElencoFlussiDettaglioDeltaNegativoSbg = UddElencoFlussiDettaglioDeltaNegativoSbg
  override val keyPiva1: String = DeltaNegativoDettaglioSchema.piva_udd
  override val keyPiva2: String = DeltaNegativoDettaglioSchema.piva_distr
  override val mainPiva: String = keyPiva1
}
