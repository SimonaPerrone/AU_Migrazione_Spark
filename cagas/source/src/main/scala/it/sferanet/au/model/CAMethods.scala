package it.sferanet.au.model

object CAMethods extends Enumeration {
  val Monthly: Value = Value(1) // formula 1
  val Daily: Value = Value(2) // formula 2
  val DailyHeat: Value = Value(3) // formula 3

  val MissingMZInterval: Value = Value(70)
  val MissingMeasure: Value = Value(71)
  val MissingRcu: Value = Value(72)
  val NoSuchTypeConsume: Value = Value(73)

  val SerialNumberMismatch: Value = Value(90)
}
