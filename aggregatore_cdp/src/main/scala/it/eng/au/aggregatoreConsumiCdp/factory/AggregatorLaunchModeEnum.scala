package it.eng.au.aggregatoreConsumiCdp.factory

import scala.language.implicitConversions

object AggregatorLaunchModeEnum extends Enumeration {
  val UDD: AggregatorLaunchModeEnum.Value = Value("CDP1")
  val DISTR: AggregatorLaunchModeEnum.Value = Value("CDP2")
  val UDB: AggregatorLaunchModeEnum.Value = Value("CDP3")

  val PRE: AggregatorLaunchModeEnum.Value = Value("PRE")
  val FIN: AggregatorLaunchModeEnum.Value = Value("FIN")
  val AGGRIC: AggregatorLaunchModeEnum.Value = Value("AGGRIC")
  val AGGDS: AggregatorLaunchModeEnum.Value = Value("AGGDS")
  val DEDOTTI: AggregatorLaunchModeEnum.Value = Value("DEDOTTI")

  val DETTAGLIOFLUSSIPRE: AggregatorLaunchModeEnum.Value = Value("DETTAGLIOFLUSSIPRE")
  val DETTAGLIOFLUSSIFIN: AggregatorLaunchModeEnum.Value = Value("DETTAGLIOFLUSSIFIN")
  val DETTAGLIOFLUSSIRIC: AggregatorLaunchModeEnum.Value = Value("DETTAGLIOFLUSSIRIC")

  private val m = this.values.map(v => (v.toString, v)).toMap

  def toValue(s: String): Value = this.m.get(s).orNull

}
