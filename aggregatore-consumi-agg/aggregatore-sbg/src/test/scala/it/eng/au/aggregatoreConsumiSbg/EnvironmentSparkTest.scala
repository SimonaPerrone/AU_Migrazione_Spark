package it.eng.au.aggregatoreConsumiSbg

import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test", "src/test/resources/params.properties", isLocal = true)
}
