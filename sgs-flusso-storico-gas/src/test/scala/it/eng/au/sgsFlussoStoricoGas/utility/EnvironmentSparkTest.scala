package it.eng.au.sgsFlussoStoricoGas.utility

import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test", "[SGS LOG]","src/test/resources/params.properties", isLocal = true)
}

