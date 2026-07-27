package it.sferanet.au.model

object ProfStdMode extends Enumeration {
  val Calculated: Value = Value(0)
  val LastRcu: Value = Value(1)
  val Tds: Value = Value(2)
  val Wip: Value = Value(99)
}
