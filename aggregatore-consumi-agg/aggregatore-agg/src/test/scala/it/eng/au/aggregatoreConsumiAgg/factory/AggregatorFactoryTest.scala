package it.eng.au.aggregatoreConsumiAgg.factory

import it.eng.au.aggregatoreConsumiAgg.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.junit.Assert

import scala.util.Try

class AggregatorFactoryTest extends EnvironmentSparkTest {
  def testGetAggregators(): Unit = {
    Environment.setProperty("output.file.couples", " aggregato -> AGG1, esclusi -> AGG1  ")

    val aggSet = AggregatorFactory.getAggregators

    Assert.assertEquals(2, aggSet.size)
    Assert.assertTrue(aggSet.exists(_.operationName.equals("AGGREGATO")))
    Assert.assertTrue(aggSet.exists(_.operationName.equals("INCOERENTI_EXC")))

    Environment.setProperty("output.file.couples", "aggregato, aggregato -> SBG1, esclusi -> SBG3  ")
    val aggSetFailure = Try(AggregatorFactory.getAggregators)
    Assert.assertTrue(aggSetFailure.isFailure)
  }
}
