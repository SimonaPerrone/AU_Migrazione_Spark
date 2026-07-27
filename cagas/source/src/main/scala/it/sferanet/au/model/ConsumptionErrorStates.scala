package it.sferanet.au.model

object ConsumptionErrorStates extends Enumeration {
  val None: Value = Value(0)
  val NotConfigured: Value = Value(1)
  val SerialNumberMismatch: Value = Value(2)
  val MissingValues: Value = Value(3)
  val MissingRcu: Value = Value(4)
  val Exception: Value = Value(5)
}
