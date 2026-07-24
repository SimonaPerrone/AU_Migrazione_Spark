package it.eng.au.cceCalcolo.args

case class CalcoloArgs(
                        annoCalc: Option[Int] = None,
                        meseCalc: Option[Int] = None,
                        tipoCalc: Option[String] = None,
                        massivoFlag: Option[Boolean] = None
                      ) extends Args