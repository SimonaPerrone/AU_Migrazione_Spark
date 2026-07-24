package it.au.misure.ingestionMisureGasUnico.flow

//import it.au.misure.ingestionMisureGasUnico.utility.SparkLocal
import it.au.misure.ingestionMisureGasUnico.utility.EnvironmentSparkTest
import junit.framework.TestCase

class TestExampleFlow extends /*TestCase with SparkLocal*/ EnvironmentSparkTest {
  def testRun(): Unit = {
    ExampleFlow().run()
  }
}
