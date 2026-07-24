package it.eng.au.pubblicazioneIndennizzi.controller.impl.aggregator

import it.eng.au.pubblicazioneIndennizzi.controller.traits.DettaglioPDRTrait
import it.eng.au.pubblicazioneIndennizzi.schema.DETTAGLIO_PDR_IZGOutputSchema

import scala.collection.immutable.ListMap

object DettaglioPDRIZG2 extends DettaglioPDRTrait {
  override val keyFields: ListMap[String, String] = ListMap(
    pivaFirst -> DETTAGLIO_PDR_IZGOutputSchema.PIVA_UDD.toString,
    pivaSecond -> DETTAGLIO_PDR_IZGOutputSchema.PIVA_ID.toString,
    annoMese -> DETTAGLIO_PDR_IZGOutputSchema.AAAAMM.toString
  )

  override val flowName: String = "DETTAGLIO_PDR_IZG2"
  override val flowBaseName: String = "CIG2"
  override val baseName: String = "CIG2"
}