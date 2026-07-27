package it.eng.au.ammissibilitaSettlementGas.utility

import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test", "LOG:", "src/test/resources/params.properties", isLocal = true)
}
