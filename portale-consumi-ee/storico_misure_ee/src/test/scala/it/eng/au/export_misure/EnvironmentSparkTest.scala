package it.eng.au.export_misure

import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase  {
  EnvironmentMisure.getOrCreate(appName = "Test", path = "src/test/resources/params.properties", isLocal = true,storic = false)

}
