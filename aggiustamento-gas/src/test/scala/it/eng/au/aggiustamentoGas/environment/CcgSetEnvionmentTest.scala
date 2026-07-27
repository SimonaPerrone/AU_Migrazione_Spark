package it.eng.au.aggiustamentoGas.environment

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.utility.environment.{CcgSetEnvironment, Environment}
import org.joda.time.DateTime
import org.junit.Assert

class CcgSetEnvionmentTest extends EnvironmentSparkTest{
  def testEnvironmentSet(): Unit = {
    val sysDate = DateTime.parse("2022-04-21")

    CcgSetEnvironment.set(sysDate)

    Assert.assertEquals(Environment.getProperty("period.read.startDate"), "202001")
    Assert.assertEquals(Environment.getProperty("period.read.endDate"), "202203")
    Assert.assertEquals(Environment.getProperty("flow.read.startDate"), "201901")
    Assert.assertEquals(Environment.getProperty("flow.read.endDate"), "202204")
    Assert.assertEquals(Environment.getProperty("flow.read.ghigliottina"), "20220419")
    Assert.assertEquals(Environment.getProperty("rcugas.sqoop.date"), "20220419")
  }

  def testEnvironmentSet2(): Unit = {
    val sysDate = DateTime.parse("2028-04-21")

    CcgSetEnvironment.set(sysDate)

    Assert.assertEquals(Environment.getProperty("period.read.startDate"), "202301")
    Assert.assertEquals(Environment.getProperty("period.read.endDate"), "202803")
    Assert.assertEquals(Environment.getProperty("flow.read.startDate"), "202201")
    Assert.assertEquals(Environment.getProperty("flow.read.endDate"), "202804")
    Assert.assertEquals(Environment.getProperty("flow.read.ghigliottina"), "20280419")
    Assert.assertEquals(Environment.getProperty("rcugas.sqoop.date"), "20280419")
  }

  def testEnvironmentSet3(): Unit = {
    val sysDate = DateTime.parse("2029-04-21")

    CcgSetEnvironment.set(sysDate)

    Assert.assertEquals(Environment.getProperty("period.read.startDate"), "202401")
    Assert.assertEquals(Environment.getProperty("period.read.endDate"), "202903")
    Assert.assertEquals(Environment.getProperty("flow.read.startDate"), "202301")
    Assert.assertEquals(Environment.getProperty("flow.read.endDate"), "202904")
    Assert.assertEquals(Environment.getProperty("flow.read.ghigliottina"), "20290419")
    Assert.assertEquals(Environment.getProperty("rcugas.sqoop.date"), "20290419")
  }
}
