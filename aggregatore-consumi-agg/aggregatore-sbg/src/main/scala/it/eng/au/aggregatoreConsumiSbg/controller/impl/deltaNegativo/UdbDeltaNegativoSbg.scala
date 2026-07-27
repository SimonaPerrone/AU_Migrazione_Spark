package it.eng.au.aggregatoreConsumiSbg.controller.impl.deltaNegativo

import it.eng.au.aggregatoreConsumiCommon.schema.DeltaNegativoDettaglioSchema
import it.eng.au.aggregatoreConsumiSbg.controller.impl.deltaNegativo.elencoFlussi.{ElencoFlussiDettaglioDeltaNegativoSbg, UdbElencoFlussiDettaglioDeltaNegativoSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.deltaNegativo.pdr.{PdrDettaglioDeltaNegativoSbg, UdbPdrDettaglioDeltaNegativoSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.DeltaNegativoTraitSbg

object UdbDeltaNegativoSbg extends DeltaNegativoTraitSbg {
  override val baseNumber: String = "5"
  override val pdrDettaglioDeltaNegativo: PdrDettaglioDeltaNegativoSbg = UdbPdrDettaglioDeltaNegativoSbg
  override val elencoFlussiDettaglioDeltaNegativo: ElencoFlussiDettaglioDeltaNegativoSbg = UdbElencoFlussiDettaglioDeltaNegativoSbg
  override val keyPiva1: String = DeltaNegativoDettaglioSchema.piva_udb
  override val keyPiva2: String = DeltaNegativoDettaglioSchema.piva_udd
  override val mainPiva: String = keyPiva1
}
