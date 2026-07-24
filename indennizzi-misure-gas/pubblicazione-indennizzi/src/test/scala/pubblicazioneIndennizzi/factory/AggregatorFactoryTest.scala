package pubblicazioneIndennizzi.factory

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import it.eng.au.pubblicazioneIndennizzi.factory.AggregatorFactory
import pubblicazioneIndennizzi.EnvironmentSparkTest

class AggregatorFactoryTest extends EnvironmentSparkTest {
  def testGetAggregators(): Unit = {
    Environment.setProperty("output.file.couples", " IZG->CIG1,IZG->CIG2,DETTAGLIO->CIG1,DETTAGLIO->CIG2 ")

    val aggSet = AggregatorFactory.getAggregators

    var output:String = ""
    aggSet.foreach(output += _.flowName + "\n")
    println(output)

    //aggSet.toString()
  }

    /*Assert.assertEquals(2, aggSet.size)
    Assert.assertTrue(aggSet.exists(_.operationName.equals("AGGREGATO")))
    Assert.assertTrue(aggSet.exists(_.operationName.equals("ESCLUSI")))

    Environment.setProperty("output.file.couples", "aggregato, aggregato -> AGG10, esclusi -> AGG  ")
    val aggSetFailure = Try(AggregatorFactory.getAggregators)
    Assert.assertTrue(aggSetFailure.isFailure)*/

}

/*
class AggregatorFactoryTest extends EnvironmentSparkTest {
  def testGetAggregators(): Unit = {
    Environment.setProperty("output.file.couples", " aggregato -> AGG1, esclusi -> AGG1  ")

    val aggSet = AggregatorFactory.getAggregators

    Assert.assertEquals(2, aggSet.size)
    Assert.assertTrue(aggSet.exists(_.operationName.equals("AGGREGATO")))
    Assert.assertTrue(aggSet.exists(_.operationName.equals("ESCLUSI")))

    Environment.setProperty("output.file.couples", "aggregato, aggregato -> AGG10, esclusi -> AGG  ")
    val aggSetFailure = Try(AggregatorFactory.getAggregators)
    Assert.assertTrue(aggSetFailure.isFailure)
  }
}
 */
