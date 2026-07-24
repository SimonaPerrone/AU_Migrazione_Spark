package it.eng.au.pubblicazione_cce

import it.eng.au.pubblicazione_cce.utility.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate(appName = "Test", path = "src/test/resources/params.properties", isLocal = true)
}
