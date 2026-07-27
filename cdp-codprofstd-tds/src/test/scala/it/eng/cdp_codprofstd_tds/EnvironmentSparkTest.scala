package it.eng.cdp_codprofstd_tds

import it.eng.cdp_codprofstd_tds.utility.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase{
  Environment.getOrCreate("Test","src/test/resources/params.properties", true)
}
