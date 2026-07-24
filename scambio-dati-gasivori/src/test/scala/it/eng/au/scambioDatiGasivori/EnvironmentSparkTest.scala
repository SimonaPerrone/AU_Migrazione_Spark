package it.eng.au.scambioDatiGasivori

import it.eng.au.scambioDatiGasivori.utility.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test", "LOG:", "src/test/resources/params.properties", isLocal = true)
}
