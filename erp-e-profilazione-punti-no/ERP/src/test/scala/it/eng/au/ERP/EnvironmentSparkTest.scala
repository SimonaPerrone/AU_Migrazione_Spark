package it.eng.au.ERP

import it.eng.au.ERP.utility.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase{
  Environment.getOrCreate(appName = "Test", path = "src/test/resources/params.properties", isLocal = true)

}
