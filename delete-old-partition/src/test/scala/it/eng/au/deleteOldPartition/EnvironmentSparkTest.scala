package it.eng.au.deleteOldPartition

import it.eng.au.deleteOldPartition.utility.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test", "src/test/resources/params.properties", true)
}
