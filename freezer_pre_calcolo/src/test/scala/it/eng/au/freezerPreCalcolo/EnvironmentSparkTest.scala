package it.eng.au.freezerPreCalcolo

import it.eng.au.freezerPreCalcolo.utility.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test Freezer","src/test/resources/params.properties", true)
}
