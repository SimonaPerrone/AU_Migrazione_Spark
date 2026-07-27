package it.eng.au.aggregatoreConsumiSbg.factory

import scala.language.implicitConversions

object AggregatorLaunchModeEnum extends Enumeration {
  val UDD: AggregatorLaunchModeEnum.Value = Value("SBG1")
  val RDB: AggregatorLaunchModeEnum.Value = Value("SBG2")
  val IT: AggregatorLaunchModeEnum.Value = Value("SBG3")
  val ID: AggregatorLaunchModeEnum.Value = Value("SBG4")
  val UDB: AggregatorLaunchModeEnum.Value = Value("SBG5")

  val AGGREGATO: AggregatorLaunchModeEnum.Value = Value("AGGREGATO")
  val DETTAGLIOUNICO: AggregatorLaunchModeEnum.Value = Value("DETTAGLIOUNICO")
  val ESCLUSI: AggregatorLaunchModeEnum.Value = Value("ESCLUSI")
  val DETTAGLIO: AggregatorLaunchModeEnum.Value = Value("DETTAGLIO")
  val DTG: AggregatorLaunchModeEnum.Value = Value("DTG")
  val INCOERENTI: AggregatorLaunchModeEnum.Value = Value("INCOERENTI")
  val INCOERENTIDETTAGLIO: AggregatorLaunchModeEnum.Value = Value("INCOERENTIDETTAGLIO")
  val DELTANEGATIVO: AggregatorLaunchModeEnum.Value = Value("DN")
  val GIROCONTATORE: AggregatorLaunchModeEnum.Value = Value("GIRO")

  private val m = this.values.map(v => (v.toString, v)).toMap

  def toValue(s: String): Value = this.m.get(s).orNull

}
