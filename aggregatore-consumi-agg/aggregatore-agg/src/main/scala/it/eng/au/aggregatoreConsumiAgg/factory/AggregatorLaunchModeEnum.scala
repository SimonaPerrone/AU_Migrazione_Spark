package it.eng.au.aggregatoreConsumiAgg.factory

import scala.language.implicitConversions

object AggregatorLaunchModeEnum extends Enumeration {
  val UDD: AggregatorLaunchModeEnum.Value = Value("AGG1")
  val RDB: AggregatorLaunchModeEnum.Value = Value("AGG2")
  val IT: AggregatorLaunchModeEnum.Value = Value("AGG3")
  val ID: AggregatorLaunchModeEnum.Value = Value("AGG4")
  val UDB: AggregatorLaunchModeEnum.Value = Value("AGG5")

  val AGGREGATO: AggregatorLaunchModeEnum.Value = Value("AGGREGATO")
  val DETTAGLIOUNICO: AggregatorLaunchModeEnum.Value = Value("DETTAGLIOUNICO")
  val ESCLUSI: AggregatorLaunchModeEnum.Value = Value("ESCLUSI")
  val DETTAGLIO: AggregatorLaunchModeEnum.Value = Value("DETTAGLIO")
  val DTG: AggregatorLaunchModeEnum.Value = Value("DTG")
  val INCOERENTI: AggregatorLaunchModeEnum.Value = Value("INCOERENTI")
  val INCOERENTIDETTAGLIO: AggregatorLaunchModeEnum.Value = Value("INCOERENTIDETTAGLIO")
  val DELTANEGATIVO: AggregatorLaunchModeEnum.Value = Value("DN")
  val GIROCONTATORE: AggregatorLaunchModeEnum.Value = Value("GIRO")
  val AGGREGATO_TRIPLA: AggregatorLaunchModeEnum.Value = Value("DISTRITREMI")

  private val m = this.values.map(v => (v.toString, v)).toMap

  def toValue(s: String): Value = this.m.get(s).orNull

}
