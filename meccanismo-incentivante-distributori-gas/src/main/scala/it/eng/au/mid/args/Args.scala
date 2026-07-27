package it.eng.au.mid.args

case class Args(
                 flow: String = null,
                 pathToProperties: String = null
               )

object Args {
  // CALCOLO
  val SBG_FLOW = "SBG"
  val AGG_FLOW = "AGG"
  val AGG_BIT_FLOW = "AGG_BIT"
  // PREPARAZIONE PUBBLICAZIONE
  val MID1_PREP = "MID1_PREP"
  val MID2_PREP = "MID2_PREP"
  // PUBBLICAZIONE
  val MID1_PUBB = "MID1_PUBB"
  val MID2_PUBB = "MID2_PUBB"
}
