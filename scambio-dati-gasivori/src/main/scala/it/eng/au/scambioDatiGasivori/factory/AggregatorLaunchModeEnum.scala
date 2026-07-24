package it.eng.au.scambioDatiGasivori.factory

import scala.language.implicitConversions

object AggregatorLaunchModeEnum extends Enumeration {
  val CC: AggregatorLaunchModeEnum.Value = Value("CC")
  val UDD: AggregatorLaunchModeEnum.Value = Value("UDD")
  val UDB: AggregatorLaunchModeEnum.Value = Value("UDB")
  val ID: AggregatorLaunchModeEnum.Value = Value("ID")
  val CSEA: AggregatorLaunchModeEnum.Value = Value("CSEA")
  val AMM: AggregatorLaunchModeEnum.Value = Value("AMM")

  private val m = this.values.map(v => (v.toString, v)).toMap

  def toValue(s: String): Value = this.m.get(s).orNull
}