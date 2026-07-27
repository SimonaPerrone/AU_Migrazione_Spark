package it.eng.au.ammissibilitaSettlementGas.args

case class Args(
                  propertiesPath: String = null,
                  annoMese: Option[String] = None,
                  isRecoveryMode: Boolean = false
               )
