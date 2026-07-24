package it.eng.au.pubblicazioneIndennizzi.factory

import scala.language.implicitConversions

object AggregatorLaunchModeEnum extends Enumeration {

  val ID: AggregatorLaunchModeEnum.Value = Value("CIG1")
  val UDD: AggregatorLaunchModeEnum.Value = Value("CIG2")

  val IZG: AggregatorLaunchModeEnum.Value = Value("IZG")
  val DETTAGLIO: AggregatorLaunchModeEnum.Value = Value("DETTAGLIO")

  private val m = this.values.map(v => (v.toString, v)).toMap

  def toValue(s: String): Value = this.m.get(s).orNull

}
