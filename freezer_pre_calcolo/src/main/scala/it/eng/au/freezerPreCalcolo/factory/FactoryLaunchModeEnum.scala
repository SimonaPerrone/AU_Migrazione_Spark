package it.eng.au.freezerPreCalcolo.factory

import scala.language.implicitConversions

object FactoryLaunchModeEnum extends Enumeration {
  val RCUGASMASSIVOTECH: FactoryLaunchModeEnum.Value = Value("RCUGASMASSIVOTECH")

  private val m = this.values.map(v => (v.toString, v)).toMap

  def toValue(s: String): Value = this.m.get(s).orNull

}
