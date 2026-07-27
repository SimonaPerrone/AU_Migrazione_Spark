package it.eng.au.calcoloSettlementGas.args

case class Args(
                 propertiesPath: String = null,
                 annoMese: Option[String] = None,
                 isRecoveryMode: Boolean = false
               )