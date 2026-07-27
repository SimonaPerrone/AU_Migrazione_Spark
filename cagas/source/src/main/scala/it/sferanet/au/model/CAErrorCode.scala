package it.sferanet.au.model

object CAErrorCode extends Enumeration {
  val None: Value = Value(0)
  val MissingMeasure: Value = Value(1)
  val MissingWeight: Value = Value(2)
  val MissingMethods: Value = Value(3)
  val ContractDiscontinuity: Value = Value(4)
  val MissingMZInterval: Value = Value(5)
  val MissingRcu: Value = Value(6)
  val MissingCe: Value = Value(7)
  val NegativeConsumption: Value = Value(8)
  val SerialNumberMismatch: Value = Value(9)
}
