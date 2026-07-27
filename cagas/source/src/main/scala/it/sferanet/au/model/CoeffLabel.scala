package it.sferanet.au.model

object CoeffLabel extends Enumeration {
  val CM, RCU = Value

  override def toString: String = if (this.Value == CM) "CM" else "RCU"
}
