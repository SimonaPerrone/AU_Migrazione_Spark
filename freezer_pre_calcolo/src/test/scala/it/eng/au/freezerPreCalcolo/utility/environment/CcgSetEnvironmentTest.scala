package it.eng.au.freezerPreCalcolo.utility.environment

import it.eng.au.freezerPreCalcolo.EnvironmentSparkTest
import org.joda.time.DateTime
import org.junit.Assert

class CcgSetEnvironmentTest extends EnvironmentSparkTest {
  def testFinEnvironmentSet(): Unit = {
    val sysDate = DateTime.parse("2022-05-19")

    CcgFinEnvironment.set(sysDate)
    Assert.assertEquals(Environment.getFreezeDate, "2022-06-01")
    Assert.assertEquals(Environment.getSession, "CCG")
  }

  def testFinEnvironmentSet2(): Unit = {
    val sysDate = DateTime.parse("2022-11-21")

    CcgFinEnvironment.set(sysDate)
    Assert.assertEquals(Environment.getFreezeDate, "2023-06-01")
    Assert.assertEquals(Environment.getSession, "CCG")
  }

  def testRicEnvironmentSet(): Unit = {
    val sysDate = DateTime.parse("2022-05-19")

    CcgRicEnvironment.set(sysDate)
    Assert.assertEquals(Environment.getFreezeDate, "2021-06-01")
    Assert.assertEquals(Environment.getSession, "CCG")

  }

  def testRicEnvironmentSet2(): Unit = {
    val sysDate = DateTime.parse("2022-11-21")

    CcgRicEnvironment.set(sysDate)
    Assert.assertEquals(Environment.getFreezeDate, "2022-06-01")
    Assert.assertEquals(Environment.getSession, "CCG")
  }
}
