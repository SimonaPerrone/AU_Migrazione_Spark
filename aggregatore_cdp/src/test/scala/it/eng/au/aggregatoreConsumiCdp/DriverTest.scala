package it.eng.au.aggregatoreConsumiCdp

import org.junit.Ignore

@Ignore
class DriverTest extends EnvironmentSparkTest {
  def testGetAggregato(): Unit = {
    val arguments: Array[String] = Array(
      "src/test/resources/deploy/params.properties"
    )

    Driver.main(arguments)
  }
}
