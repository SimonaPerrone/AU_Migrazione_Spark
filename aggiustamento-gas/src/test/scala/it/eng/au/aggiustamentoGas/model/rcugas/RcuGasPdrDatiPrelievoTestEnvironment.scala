package it.eng.au.aggiustamentoGas.model.rcugas

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import org.junit.Assert

class RcuGasPdrDatiPrelievoTestEnvironment extends EnvironmentSparkTest {
  def testThermYear(): Unit = {
    val rcuGasDatiPrelievo = RcuGasPdrDatiPrelievo(nIdPdr = "a", nPrelievoAnnuo = Some(1.0), tCodProfilo = None, tAnno = "2021")

    Assert.assertEquals("01-10-2020", rcuGasDatiPrelievo.startThermalYear.toString("dd-MM-yyyy"))
    Assert.assertEquals("30-09-2021", rcuGasDatiPrelievo.endThermalYear.toString("dd-MM-yyyy"))
  }
}
