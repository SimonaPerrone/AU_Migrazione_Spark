package it.eng.au.pubblicazioneIndennizzi.controller.impl.aggregator

import it.eng.au.pubblicazioneIndennizzi.controller.traits.IZGTrait
import it.eng.au.pubblicazioneIndennizzi.schema.IZGOutputSchema

import scala.collection.immutable.ListMap

object IZG2Aggregator extends IZGTrait {
  override val keyFields: ListMap[String, String] = ListMap(
    pivaFirst -> IZGOutputSchema.PIVA_UDD.toString,
    pivaSecond -> IZGOutputSchema.PIVA_ID.toString,
    annoMese -> IZGOutputSchema.AAAAMM.toString
  )

  override val flowName: String = "IZG2"
  override val flowBaseName: String = "CIG2"
  override val baseName: String = "CIG2"
}
