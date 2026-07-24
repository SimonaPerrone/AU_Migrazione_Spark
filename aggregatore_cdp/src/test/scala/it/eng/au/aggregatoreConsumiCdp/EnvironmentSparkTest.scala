package it.eng.au.aggregatoreConsumiCdp

import it.eng.au.aggregatoreConsumiCdp.utility.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test", "src/test/resources/deploy/params.properties", isLocal = true)
}
