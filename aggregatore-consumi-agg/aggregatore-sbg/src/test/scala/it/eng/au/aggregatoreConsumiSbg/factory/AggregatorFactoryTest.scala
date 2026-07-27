package it.eng.au.aggregatoreConsumiSbg.factory

import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.EnvironmentSparkTest
import org.junit.Assert

import scala.util.Try

class AggregatorFactoryTest extends EnvironmentSparkTest {
  def testGetAggregators(): Unit = {
    Environment.setProperty("output.file.couples", " aggregato -> SBG1, esclusi -> SBG1  ")

    val aggSet = AggregatorFactory.getAggregators(None)

    Assert.assertEquals(2, aggSet.size)
    Assert.assertTrue(aggSet.exists(_.operationName.equals("AGGREGATO")))
    Assert.assertTrue(aggSet.exists(_.operationName.equals("INCOERENTI_EXC")))

    Environment.setProperty("output.file.couples", "aggregato, aggregato -> AGG1, esclusi -> AGG2  ")
    val aggSetFailure = Try(AggregatorFactory.getAggregators(None))
    Assert.assertTrue(aggSetFailure.isFailure)
  }

  def testGetAggregatorsParams(): Unit = {
    Environment.setProperty("output.file.couples", " aggregato -> SBG1, esclusi -> SBG1  ")

    val aggSet = AggregatorFactory.getAggregators(Some(" aggregato -> SBG1, esclusi -> SBG1  "))

    Assert.assertEquals(2, aggSet.size)
    Assert.assertTrue(aggSet.exists(_.operationName.equals("AGGREGATO")))
    Assert.assertTrue(aggSet.exists(_.operationName.equals("INCOERENTI_EXC")))

    Environment.setProperty("output.file.couples", "aggregato, aggregato -> AGG1, esclusi -> AGG2  ")
    val aggSetFailure = Try(AggregatorFactory.getAggregators(Some("aggregato, aggregato -> AGG1, esclusi -> AGG2  ")))
    Assert.assertTrue(aggSetFailure.isFailure)
  }
}
