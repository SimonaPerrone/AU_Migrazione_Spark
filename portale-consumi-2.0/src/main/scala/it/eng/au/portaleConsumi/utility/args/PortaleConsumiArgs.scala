package it.eng.au.portaleConsumi.utility.args

import java.time.LocalDate

case class PortaleConsumiArgs(
                               pathToProperties: String = null,
                               flow: String = null,
                               interval: String = null,
                               runDay: LocalDate = null,
                               dayInterval: Int = 0,
                               skipMisure: Boolean = false
                             ) {
  override def toString: String = {
    s"PortaleConsumiArgs: \n" +
      s"pathToProperties: $pathToProperties, \n" +
      s"flow: $flow, \n" +
      s"interval: $interval, \n" +
      s"runDay: $runDay, \n" +
      s"dayInterval: $dayInterval (se 0 allora non impostato), \n" +
      s"skipMisure: $skipMisure (true solo per test)"
  }
}

object PortaleConsumiArgs {
  val flowFornitureGas: String = "FORNITURE_GAS"
  val flowMisureGas: String = "MISURE_GAS"
  val flowVerificaFornitureGas: String = "VERIFICA_FORNITURE_GAS"
  val flowVerificaMisureGas: String = "VERIFICA_MISURE_GAS"
  val flowVerificaMisureGasStorico: String = "VERIFICA_MISURE_GAS_STORICO"

  val intervalFull: String = "FULL"
  val intervalShort: String = "3M"
  val intervalLong: String = "33M"

  val flowsOptions: Seq[String] = Seq(flowFornitureGas, flowMisureGas, flowVerificaFornitureGas, flowVerificaMisureGas)
  val intervalOptions: Seq[String] = Seq("3M", "33M", "FULL")

}