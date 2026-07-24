package it.au.misure.ingestionMisureGasUnico.args

case class FlowArgsConfig(
                       flowName: String = ""
                     ) {
  override def toString: String = s"FlowArgsConfig=[flowName=$flowName]"
}
