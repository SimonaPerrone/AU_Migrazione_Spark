package it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico

import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.elencoFlussi.{ElencoFlussiDettaglioUnico, IdElencoFlussiDettaglioUnico}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.pdr.{IdPdrDettaglioUnico, PdrDettaglioUnico}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.DettaglioUnicoTrait
import it.eng.au.aggregatoreConsumiCommon.schema.DettaglioUnicoSchema

object IdDettaglioUnico extends DettaglioUnicoTrait {
  override val baseNumber: String = "4"
  override val pdrDettaglioUnico: PdrDettaglioUnico = IdPdrDettaglioUnico
  override val elencoFlussiDettaglioUnico: ElencoFlussiDettaglioUnico = IdElencoFlussiDettaglioUnico
  override val keyPiva1: String = DettaglioUnicoSchema.piva_distr
  override val keyPiva2: String = DettaglioUnicoSchema.piva_udd
  override val mainPiva: String = keyPiva1

//  override val csvFields: List[String] = List() //throw new Exception("not supported")
//  override val header: String = "" //throw new Exception("not supported")
}
