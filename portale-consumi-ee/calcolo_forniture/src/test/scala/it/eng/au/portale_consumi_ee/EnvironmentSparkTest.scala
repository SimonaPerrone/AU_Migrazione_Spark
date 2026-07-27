package it.eng.au.portale_consumi_ee

import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase  {
  Environment.getOrCreate(appName = "Test", path = "src/test/resources/params.properties", isLocal = true)

}
