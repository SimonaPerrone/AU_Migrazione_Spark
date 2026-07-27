package it.eng.au.aggiustamentoGas.model.agg

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.utility.constants.Treatment
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasMassivoP
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class ExternalDailyInfoTestEnvironment extends EnvironmentSparkTest {

  def testFindMaxRcuGasMassivoP(): Unit = {
    val date1 = DateTime.parse("15/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy"))
    val date2 = DateTime.parse("16/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy"))
    val date3 = DateTime.parse("16/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy"))
    val date4 = DateTime.parse("17/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy"))
    val date5 = DateTime.parse("17/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy"))

    val it: Iterable[RcuGasMassivoP] = Iterable(
      RcuGasMassivoP(startDate = date1, endDate = date1, tCodicePdr = "", nIdPdr = "", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = date1, endDate = date2, tCodicePdr = "", nIdPdr = "", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = date1, endDate = date3, tCodicePdr = "", nIdPdr = "", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = date1, endDate = date4, tCodicePdr = "", nIdPdr = "", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = date1, endDate = date5, tCodicePdr = "", nIdPdr = "", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
    )
    val ei = ExternalDailyInfo(
      rcuGasMassivoPList = Some(it)
    )

    Assert.assertTrue(ei.getRcuGasMassivoPWithMaxDataFineForn.isDefined)
    Assert.assertEquals(date5, ei.getRcuGasMassivoPWithMaxDataFineForn.get.endDate)
    Assert.assertTrue(ExternalDailyInfo().getRcuGasMassivoPWithMaxDataFineForn.isEmpty)
    Assert.assertTrue(ExternalDailyInfo(rcuGasMassivoPList = Some(Iterable())).getRcuGasMassivoPWithMaxDataFineForn.isEmpty)
  }
}
