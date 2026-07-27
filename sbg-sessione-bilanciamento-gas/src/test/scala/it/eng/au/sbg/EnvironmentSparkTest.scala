package it.eng.au.sbg

import it.eng.au.aggiustamentoGas.utility.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test", "LOCAL TEST LOG", "src/test/resources/params.properties", true)
}
