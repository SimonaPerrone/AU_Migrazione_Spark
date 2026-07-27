package it.eng.au.sbg.utility.properties

import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.sbg.EnvironmentSparkTest
import org.junit.Assert

class SbgPropertyTest extends EnvironmentSparkTest {

  def testProperties(): Unit = {
    println(Environment.executionId)
//    println(Environment.getPartitionDate)
  }

  def testTrim(): Unit = {

    val dailyConsumption = Environment.getProperty("dailyConsumption.table")
    println(dailyConsumption)

    Assert.assertEquals("eng_test.daily_consumption_sbg", dailyConsumption)
    Assert.assertNotEquals("    eng_test.daily_consumption_sbg     ", dailyConsumption)
  }
}
