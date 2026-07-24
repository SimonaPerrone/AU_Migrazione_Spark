package it.eng.au.cceCalcolo.utility

import it.eng.au.cceCalcolo.utility.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test", "[CCE LOG]", isLocal = true)
}
