package it.eng.au.gse.common

import it.eng.au.gse.common.utility.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test", "LOG:", "src/test/resources/params.properties", isLocal = true)
}
