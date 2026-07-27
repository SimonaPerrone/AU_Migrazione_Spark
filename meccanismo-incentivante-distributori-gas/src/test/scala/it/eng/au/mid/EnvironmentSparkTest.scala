package it.eng.au.mid

import it.eng.au.mid.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate(appName = "Test", path = "src/test/resources/params.properties", isLocal = true)
}
