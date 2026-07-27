package it.eng.au.CalcoloSettlementGas.utility

import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Calcolo Test", "LOG:", "src/test/resources/params.properties", isLocal = true)
}
