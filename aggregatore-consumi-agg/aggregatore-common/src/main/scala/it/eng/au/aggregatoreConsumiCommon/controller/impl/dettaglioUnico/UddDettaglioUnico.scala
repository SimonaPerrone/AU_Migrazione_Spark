package it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico

import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.elencoFlussi.{ElencoFlussiDettaglioUnico, UddElencoFlussiDettaglioUnico}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.pdr.{PdrDettaglioUnico, UddPdrDettaglioUnico}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.DettaglioUnicoTrait
import it.eng.au.aggregatoreConsumiCommon.schema.DettaglioUnicoSchema

object UddDettaglioUnico extends DettaglioUnicoTrait {
  override val baseNumber: String = "1"
  override val pdrDettaglioUnico: PdrDettaglioUnico = UddPdrDettaglioUnico
  override val elencoFlussiDettaglioUnico: ElencoFlussiDettaglioUnico = UddElencoFlussiDettaglioUnico
  override val keyPiva1: String = DettaglioUnicoSchema.piva_udd
  override val keyPiva2: String = DettaglioUnicoSchema.piva_distr
  override val mainPiva: String = keyPiva1

//  override val csvFields: List[String] = List() //throw new Exception("not supported")
//  override val header: String = "" //throw new Exception("not supported")

}
