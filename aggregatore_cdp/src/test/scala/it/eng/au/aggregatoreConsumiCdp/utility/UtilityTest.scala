package it.eng.au.aggregatoreConsumiCdp.utility

import it.eng.au.aggregatoreConsumiCdp.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCdp.utility.DateTimeUtility._
import org.junit.Assert

class UtilityTest extends EnvironmentSparkTest {
  def testGetCurrentThermalYear(): Unit = {
    val currentDate = "2021-09-16 10:11:08"
    val CurrentThermalYear = getCurrentThermalYear(currentDate)

    val currentDate2 = "2021-11-16 10:11:08"
    val CurrentThermalYear2 = getCurrentThermalYear(currentDate2)

    val currentDate3 = "2020-11-16 10:11:08"
    val CurrentThermalYear3 = getCurrentThermalYear(currentDate3)

    val currentDate4 = "2020-8-16 10:11:08"
    val CurrentThermalYear4 = getCurrentThermalYear(currentDate4)

    Assert.assertTrue(CurrentThermalYear.equals("2020-10-01 00:00:00"))
    Assert.assertTrue(CurrentThermalYear2.equals("2021-10-01 00:00:00"))
    Assert.assertTrue(CurrentThermalYear3.equals("2020-10-01 00:00:00"))
    Assert.assertTrue(CurrentThermalYear4.equals("2019-10-01 00:00:00"))

  }
}
