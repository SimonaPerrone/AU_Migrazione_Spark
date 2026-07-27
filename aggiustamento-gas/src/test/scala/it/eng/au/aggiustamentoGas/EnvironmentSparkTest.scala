package it.eng.au.aggiustamentoGas

import it.eng.au.aggiustamentoGas.utility.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test", "Local test LOG:", "src/test/resources/params.properties", true)
}
