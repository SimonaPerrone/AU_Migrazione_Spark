package it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico

import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.elencoFlussi.{ElencoFlussiDettaglioUnico, UdbElencoFlussiDettaglioUnico}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.pdr.{PdrDettaglioUnico, UdbPdrDettaglioUnico}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.DettaglioUnicoTrait
import it.eng.au.aggregatoreConsumiCommon.schema.DettaglioUnicoSchema

object UdbDettaglioUnico extends DettaglioUnicoTrait {
  override val baseNumber: String = "5"
  override val pdrDettaglioUnico: PdrDettaglioUnico = UdbPdrDettaglioUnico
  override val elencoFlussiDettaglioUnico: ElencoFlussiDettaglioUnico = UdbElencoFlussiDettaglioUnico
  override val keyPiva1: String = DettaglioUnicoSchema.Piva_Udb
  override val keyPiva2: String = DettaglioUnicoSchema.piva_udd
  override val mainPiva: String = keyPiva1

//  override val csvFields: List[String] = List() //throw new Exception("not supported")
//  override val header: String = "" //throw new Exception("not supported")
}
