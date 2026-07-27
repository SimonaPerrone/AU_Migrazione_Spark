package it.eng.au.sbg.utility.environment

import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.sbg.EnvironmentSparkTest
import org.joda.time.DateTime
import org.junit.Assert

class CcgSetEnvironmentTest extends EnvironmentSparkTest{
  def testEnvironmentSet(): Unit = {
    val sysDate = DateTime.parse("2022-04-21")

    CcgSetEnvironment.set(sysDate)

    Assert.assertEquals(Environment.getProperty("period.read.startDate"), "202203")
    Assert.assertEquals(Environment.getProperty("period.read.endDate"), "202203")
    Assert.assertEquals(Environment.getProperty("flow.read.startDate"), "202110")
    Assert.assertEquals(Environment.getProperty("flow.read.endDate"), "202204")
    Assert.assertEquals(Environment.getProperty("flow.read.ghigliottina"), "20220419")
    Assert.assertEquals(Environment.getProperty("rcugas.sqoop.date"), "20220419")
    Assert.assertEquals(Environment.getProperty("period.competence"), "202203")
  }

  def testEnvironmentSet2(): Unit = {
    val sysDate = DateTime.parse("2028-04-21")

    CcgSetEnvironment.set(sysDate)

    Assert.assertEquals(Environment.getProperty("period.read.startDate"), "202803")
    Assert.assertEquals(Environment.getProperty("period.read.endDate"), "202803")
    Assert.assertEquals(Environment.getProperty("flow.read.startDate"), "202710")
    Assert.assertEquals(Environment.getProperty("flow.read.endDate"), "202804")
    Assert.assertEquals(Environment.getProperty("flow.read.ghigliottina"), "20280419")
    Assert.assertEquals(Environment.getProperty("rcugas.sqoop.date"), "20280419")
    Assert.assertEquals(Environment.getProperty("period.competence"), "202803")
  }

  def testEnvironmentSet3(): Unit = {
    val sysDate = DateTime.parse("2029-04-21")

    CcgSetEnvironment.set(sysDate)

    Assert.assertEquals(Environment.getProperty("period.read.startDate"), "202903")
    Assert.assertEquals(Environment.getProperty("period.read.endDate"), "202903")
    Assert.assertEquals(Environment.getProperty("flow.read.startDate"), "202810")
    Assert.assertEquals(Environment.getProperty("flow.read.endDate"), "202904")
    Assert.assertEquals(Environment.getProperty("flow.read.ghigliottina"), "20290419")
    Assert.assertEquals(Environment.getProperty("rcugas.sqoop.date"), "20290419")
    Assert.assertEquals(Environment.getProperty("period.competence"), "202903")
  }
}
