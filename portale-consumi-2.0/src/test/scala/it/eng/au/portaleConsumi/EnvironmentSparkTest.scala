package it.eng.au.portaleConsumi

import it.eng.au.portaleConsumi.utility.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate(appName = "Test", path = "src/test/resources/params.properties", isLocal = true)
}
