package it.au.misure.ingestionMisureGasUnico.unzip

import it.au.misure.ingestionMisureGasUnico.driver.UnzipDriver
import it.au.misure.ingestionMisureGasUnico.utility.EnvironmentSparkTest
//import it.au.misure.ingestionMisureGasUnico.utility.SparkLocal
import junit.framework.TestCase
import org.junit.Ignore

@Ignore
class TestUnzipDriver extends /*TestCase with SparkLocal*/ EnvironmentSparkTest {
  def testRun(): Unit = {
    val args = Array(
      "-d", "2020-10-29"
    )
    UnzipDriver.main(args)
  }
}
