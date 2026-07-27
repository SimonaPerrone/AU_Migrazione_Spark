package it.sferanet.au.controller.validation

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.model.Flow
import it.sferanet.au.model.periodico.{Tgl, Tml}
import it.sferanet.au.model.prestazionale._
import it.sferanet.au.model.rettifica._
import it.sferanet.au.utilities.Environment
import org.junit.Assert

import java.text.SimpleDateFormat

class CambioMisuratoreControllerTest extends EnvironmentSparkTest {

  /**
   * Questo test verifica la corretta esecuzione delle rettifiche ai flussi di cambio misuratore IM1 e IGMG
   * secondo tutte le combinazioni definite nelle due tabelle al capitolo 6.4.1 del documento Procedura di Calcolo CA e COD_PROF_PREL_STD_23082022.
   * Vengono testate tutte le combinazioni dei valori cau_int_mis e cau_int_cor in una serie di possibili scenari partendo da una lista di misure
   * per un dato pdr "1" in una determinata data di misura "2020-01-01". Ogni lista contiene la sequenza temporale di misure caricate
   * dal distributore e presenti in Hive sulle quali il motore di rettifica andarà a lavorare in maniera "ordinata" (ossia dalla più vecchia alla
   * più recente)
   */
  def testRectifyMeasures(): Unit = {
    val format = new SimpleDateFormat("yyyy-mm-dd")

    val combinationsRettificheTable1 = List(Some(1), Some(2), Some(3), Some(4)).flatMap(cau_int_mis => List[Option[Int]](None).map(cau_int_cor => (cau_int_mis, cau_int_cor)))
    for ((cau_int_mis, cau_int_cor) <- combinationsRettificheTable1) {

      // IGMG

      // tutte matricole coincidenti
      val measures1 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output1 = CambioMisuratoreController.rectifyMeasures(measures1)
      Assert.assertEquals(2, output1.length)
      Assert.assertEquals(Some(0), output1.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output1.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output1.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output1.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output1.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output1.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // solo matricola misuratore coincidente
      val measures2 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output2 = CambioMisuratoreController.rectifyMeasures(measures2)
      Assert.assertEquals(2, output2.length)
      Assert.assertEquals(Some(0), output2.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output2.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output2.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output2.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output2.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output2.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // solo matricola convertitore coincidente
      val measures3 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output3 = CambioMisuratoreController.rectifyMeasures(measures3)
      Assert.assertEquals(2, output3.length)
      Assert.assertEquals(Some(500), output3.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(1500), output3.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output3.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output3.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output3.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output3.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // nessuna matricola coincidente
      val measures4 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output4 = CambioMisuratoreController.rectifyMeasures(measures4)
      Assert.assertEquals(2, output4.length)
      Assert.assertEquals(Some(500), output4.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(1500), output4.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output4.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output4.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output4.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output4.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // matricola coincidente solo in PRE
      val measures5 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output5 = CambioMisuratoreController.rectifyMeasures(measures5)
      Assert.assertEquals(2, output5.length)
      Assert.assertEquals(Some(0), output5.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(1500), output5.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output5.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output5.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output5.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output5.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // matricola coincidente solo in POST
      val measures6 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output6 = CambioMisuratoreController.rectifyMeasures(measures6)
      Assert.assertEquals(2, output6.length)
      Assert.assertEquals(Some(500), output6.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output6.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output6.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output6.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output6.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output6.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // matricola coincidente solo in POST + presenza di altri flussi
      val measures13 = List(
        Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = None, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = None, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output13 = CambioMisuratoreController.rectifyMeasures(measures13)
      Assert.assertEquals(2, output13.length)
      Assert.assertEquals(Some(500), output13.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output13.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output13.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output13.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output13.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output13.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // matricola coincidente solo in POST + presenza di altri flussi + matricola coincidente solo in PRE
      val measures14 = List(
        Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IGMG0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IGMG0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X1"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0204/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_IGMG0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_IGMG0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X2"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output14 = CambioMisuratoreController.rectifyMeasures(measures14).sorted(Flow.priorityOrderingFlows)
      Assert.assertEquals(2, output14.length)
      Assert.assertEquals(Some(0), output14.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(1500), output14.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output14.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output14.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X2"), output14.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X2"), output14.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // matricole NULL
      val measuresMatricoleNull1 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val outputMatricoleNull1 = CambioMisuratoreController.rectifyMeasures(measuresMatricoleNull1)
      Assert.assertEquals(2, outputMatricoleNull1.length)
      Assert.assertEquals(Some(500), outputMatricoleNull1.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(1500), outputMatricoleNull1.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), outputMatricoleNull1.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), outputMatricoleNull1.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), outputMatricoleNull1.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), outputMatricoleNull1.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // Rettifica trasmessa prima di flusso di cambio misuratore
      val measuresReffificaBeforeCM1 = List(
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(10),
          converted = Some(11), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val outputMeasuresReffificaBeforeCM1 = CambioMisuratoreController.rectifyMeasures(measuresReffificaBeforeCM1)
      Assert.assertEquals(2, outputMeasuresReffificaBeforeCM1.length)
      Assert.assertEquals(Some(500), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(1500), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPOST").head.fileRettifica)


      // IM1

      // tutte matricole coincidenti
      val measures7 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output7 = CambioMisuratoreController.rectifyMeasures(measures7)
      Assert.assertEquals(2, output7.length)
      Assert.assertEquals(Some(0), output7.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(0), output7.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), output7.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(1), output7.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output7.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output7.filter(f => f.service == "IM1POST").head.fileRettifica)

      // solo matricola misuratore coincidente
      val measures8 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output8 = CambioMisuratoreController.rectifyMeasures(measures8)
      Assert.assertEquals(2, output8.length)
      Assert.assertEquals(Some(0), output8.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(0), output8.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), output8.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(1), output8.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output8.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output8.filter(f => f.service == "IM1POST").head.fileRettifica)

      // solo matricola convertitore coincidente
      val measures9 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output9 = CambioMisuratoreController.rectifyMeasures(measures9)
      Assert.assertEquals(2, output9.length)
      Assert.assertEquals(Some(500), output9.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(1500), output9.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), output9.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(1), output9.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output9.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output9.filter(f => f.service == "IM1POST").head.fileRettifica)

      // nessuna matricola coincidente
      val measures10 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output10 = CambioMisuratoreController.rectifyMeasures(measures10)
      Assert.assertEquals(2, output10.length)
      Assert.assertEquals(Some(500), output10.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(1500), output10.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), output10.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(1), output10.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output10.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output10.filter(f => f.service == "IM1POST").head.fileRettifica)

      // matricola coincidente solo in PRE
      val measures11 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output11 = CambioMisuratoreController.rectifyMeasures(measures11)
      Assert.assertEquals(2, output11.length)
      Assert.assertEquals(Some(0), output11.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(1500), output11.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), output11.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(1), output11.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output11.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output11.filter(f => f.service == "IM1POST").head.fileRettifica)

      // matricola coincidente solo in POST
      val measures12 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output12 = CambioMisuratoreController.rectifyMeasures(measures12)
      Assert.assertEquals(2, output12.length)
      Assert.assertEquals(Some(500), output12.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(0), output12.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), output12.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(1), output12.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output12.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output12.filter(f => f.service == "IM1POST").head.fileRettifica)

      // matricola coincidente solo in POST + presenza di altri flussi + matricola coincidente solo in PRE
      val measures15 = List(
        Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X1"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0204/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X2"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output15 = CambioMisuratoreController.rectifyMeasures(measures15).sorted(Flow.priorityOrderingFlows)
      Assert.assertEquals(2, output15.length)
      Assert.assertEquals(Some(0), output15.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(1500), output15.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), output15.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(1), output15.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X2"), output15.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X2"), output15.filter(f => f.service == "IM1POST").head.fileRettifica)

      // matricole NULL
      val measuresMatricoleNull2 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val outputMatricoleNull2 = CambioMisuratoreController.rectifyMeasures(measuresMatricoleNull2)
      Assert.assertEquals(2, outputMatricoleNull2.length)
      Assert.assertEquals(Some(500), outputMatricoleNull2.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(1500), outputMatricoleNull2.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), outputMatricoleNull2.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(1), outputMatricoleNull2.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), outputMatricoleNull2.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), outputMatricoleNull2.filter(f => f.service == "IM1POST").head.fileRettifica)

      // Rettifica trasmessa prima di flusso di cambio misuratore
      val measuresReffificaBeforeCM2 = List(
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(10),
          converted = Some(11), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val outputMeasuresReffificaBeforeCM12 = CambioMisuratoreController.rectifyMeasures(measuresReffificaBeforeCM2)
      Assert.assertEquals(2, outputMeasuresReffificaBeforeCM12.length)
      Assert.assertEquals(Some(500), outputMeasuresReffificaBeforeCM12.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(1500), outputMeasuresReffificaBeforeCM12.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), outputMeasuresReffificaBeforeCM12.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(1), outputMeasuresReffificaBeforeCM12.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), outputMeasuresReffificaBeforeCM12.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), outputMeasuresReffificaBeforeCM12.filter(f => f.service == "IM1POST").head.fileRettifica)

    }

    val combinationsRettificheTable2 = List(Some(1), Some(2), Some(3), Some(4)).flatMap(cau_int_mis => List(Some(1), Some(2), Some(3), Some(4)).map(cau_int_cor => (cau_int_mis, cau_int_cor)))
    for ((cau_int_mis, cau_int_cor) <- combinationsRettificheTable2) {

      // IGMG

      // tutte matricole coincidenti
      val measures1 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output1 = CambioMisuratoreController.rectifyMeasures(measures1)
      Assert.assertEquals(2, output1.length)
      Assert.assertEquals(Some(0), output1.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output1.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output1.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output1.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output1.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output1.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // solo matricola misuratore coincidente
      val measures2 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output2 = CambioMisuratoreController.rectifyMeasures(measures2)
      Assert.assertEquals(2, output2.length)
      Assert.assertEquals(Some(0), output2.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output2.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1000), output2.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(2000), output2.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output2.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output2.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // solo matricola convertitore coincidente
      val measures3 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output3 = CambioMisuratoreController.rectifyMeasures(measures3)
      Assert.assertEquals(2, output3.length)
      Assert.assertEquals(Some(500), output3.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(1500), output3.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output3.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output3.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output3.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output3.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // nessuna matricola coincidente
      val measures4 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output4 = CambioMisuratoreController.rectifyMeasures(measures4)
      Assert.assertEquals(2, output4.length)
      Assert.assertEquals(Some(500), output4.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(1500), output4.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1000), output4.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(2000), output4.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(None, output4.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(None, output4.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // matricola coincidente solo in PRE
      val measures5 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output5 = CambioMisuratoreController.rectifyMeasures(measures5)
      Assert.assertEquals(2, output5.length)
      Assert.assertEquals(Some(0), output5.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(1500), output5.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output5.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(2000), output5.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output5.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(None, output5.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // matricola coincidente solo in POST
      val measures6 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output6 = CambioMisuratoreController.rectifyMeasures(measures6)
      Assert.assertEquals(2, output6.length)
      Assert.assertEquals(Some(500), output6.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output6.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1000), output6.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output6.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(None, output6.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output6.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // matricola coincidente solo in POST + presenza di altri flussi
      val measures13 = List(
        Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = None, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = None, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output13 = CambioMisuratoreController.rectifyMeasures(measures13)
      Assert.assertEquals(2, output13.length)
      Assert.assertEquals(Some(500), output13.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output13.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1000), output13.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output13.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(None, output13.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output13.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // matricola coincidente solo in POST + presenza di altri flussi + matricola coincidente solo in PRE
      val measures14 = List(
        Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IGMG0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IGMG0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X1"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0204/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_IGMG0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_IGMG0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X2"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output14 = CambioMisuratoreController.rectifyMeasures(measures14).sorted(Flow.priorityOrderingFlows)
      Assert.assertEquals(2, output14.length)
      Assert.assertEquals(Some(0), output14.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(1500), output14.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output14.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(2000), output14.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X2"), output14.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(None, output14.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // matricole NULL
      val measuresMatricoleNull1 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val outputMatricoleNull1 = CambioMisuratoreController.rectifyMeasures(measuresMatricoleNull1)
      Assert.assertEquals(2, outputMatricoleNull1.length)
      Assert.assertEquals(Some(500), outputMatricoleNull1.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(1500), outputMatricoleNull1.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1000), outputMatricoleNull1.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(2000), outputMatricoleNull1.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(None, outputMatricoleNull1.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(None, outputMatricoleNull1.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // Rettifica trasmessa prima di flusso di cambio misuratore
      val measuresReffificaBeforeCM1 = List(
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(10),
          converted = Some(11), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val outputMeasuresReffificaBeforeCM1 = CambioMisuratoreController.rectifyMeasures(measuresReffificaBeforeCM1)
      Assert.assertEquals(2, outputMeasuresReffificaBeforeCM1.length)
      Assert.assertEquals(Some(500), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(1500), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1000), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(2000), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(None, outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(None, outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPOST").head.fileRettifica)


      // IM1

      // tutte matricole coincidenti
      val measures7 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output7 = CambioMisuratoreController.rectifyMeasures(measures7)
      Assert.assertEquals(2, output7.length)
      Assert.assertEquals(Some(0), output7.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(0), output7.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), output7.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(1), output7.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output7.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output7.filter(f => f.service == "IM1POST").head.fileRettifica)

      // solo matricola misuratore coincidente
      val measures8 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output8 = CambioMisuratoreController.rectifyMeasures(measures8)
      Assert.assertEquals(2, output8.length)
      Assert.assertEquals(Some(0), output8.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(0), output8.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1000), output8.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(2000), output8.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output8.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output8.filter(f => f.service == "IM1POST").head.fileRettifica)

      // solo matricola convertitore coincidente
      val measures9 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output9 = CambioMisuratoreController.rectifyMeasures(measures9)
      Assert.assertEquals(2, output9.length)
      Assert.assertEquals(Some(500), output9.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(1500), output9.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), output9.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(1), output9.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output9.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output9.filter(f => f.service == "IM1POST").head.fileRettifica)

      // nessuna matricola coincidente
      val measures10 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output10 = CambioMisuratoreController.rectifyMeasures(measures10)
      Assert.assertEquals(2, output10.length)
      Assert.assertEquals(Some(500), output10.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(1500), output10.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1000), output10.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(2000), output10.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(None, output10.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(None, output10.filter(f => f.service == "IM1POST").head.fileRettifica)

      // matricola coincidente solo in PRE
      val measures11 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output11 = CambioMisuratoreController.rectifyMeasures(measures11)
      Assert.assertEquals(2, output11.length)
      Assert.assertEquals(Some(0), output11.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(1500), output11.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), output11.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(2000), output11.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output11.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(None, output11.filter(f => f.service == "IM1POST").head.fileRettifica)

      // matricola coincidente solo in POST
      val measures12 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output12 = CambioMisuratoreController.rectifyMeasures(measures12)
      Assert.assertEquals(2, output12.length)
      Assert.assertEquals(Some(500), output12.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(0), output12.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1000), output12.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(1), output12.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(None, output12.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output12.filter(f => f.service == "IM1POST").head.fileRettifica)

      // matricola coincidente solo in POST + presenza di altri flussi + matricola coincidente solo in PRE
      val measures15 = List(
        Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X1"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0204/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X2"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output15 = CambioMisuratoreController.rectifyMeasures(measures15).sorted(Flow.priorityOrderingFlows)
      Assert.assertEquals(2, output15.length)
      Assert.assertEquals(Some(0), output15.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(1500), output15.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), output15.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(2000), output15.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X2"), output15.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(None, output15.filter(f => f.service == "IM1POST").head.fileRettifica)

      // matricole NULL
      val measuresMatricoleNull2 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val outputMatricoleNull2 = CambioMisuratoreController.rectifyMeasures(measuresMatricoleNull2)
      Assert.assertEquals(2, outputMatricoleNull2.length)
      Assert.assertEquals(Some(500), outputMatricoleNull2.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(1500), outputMatricoleNull2.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1000), outputMatricoleNull2.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(2000), outputMatricoleNull2.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(None, outputMatricoleNull2.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(None, outputMatricoleNull2.filter(f => f.service == "IM1POST").head.fileRettifica)

      // Rettifica trasmessa prima di flusso di cambio misuratore
      val measuresReffificaBeforeCM2 = List(
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(10),
          converted = Some(11), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val outputMeasuresReffificaBeforeCM12 = CambioMisuratoreController.rectifyMeasures(measuresReffificaBeforeCM2)
      Assert.assertEquals(2, outputMeasuresReffificaBeforeCM12.length)
      Assert.assertEquals(Some(500), outputMeasuresReffificaBeforeCM12.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(1500), outputMeasuresReffificaBeforeCM12.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1000), outputMeasuresReffificaBeforeCM12.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(2000), outputMeasuresReffificaBeforeCM12.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(None, outputMeasuresReffificaBeforeCM12.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(None, outputMeasuresReffificaBeforeCM12.filter(f => f.service == "IM1POST").head.fileRettifica)

    }


    val combinationsRettificheTable3 = List[Option[Int]](None).flatMap(cau_int_mis => List(Some(1), Some(2), Some(3), Some(4)).map(cau_int_cor => (cau_int_mis, cau_int_cor)))
    for ((cau_int_mis, cau_int_cor) <- combinationsRettificheTable3) {

      // IGMG

      // tutte matricole coincidenti
      val measures1 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output1 = CambioMisuratoreController.rectifyMeasures(measures1)
      Assert.assertEquals(2, output1.length)
      Assert.assertEquals(Some(0), output1.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output1.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output1.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output1.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output1.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output1.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // solo matricola misuratore coincidente
      val measures2 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output2 = CambioMisuratoreController.rectifyMeasures(measures2)
      Assert.assertEquals(2, output2.length)
      Assert.assertEquals(Some(0), output2.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output2.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1000), output2.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(2000), output2.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output2.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output2.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // solo matricola convertitore coincidente
      val measures3 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output3 = CambioMisuratoreController.rectifyMeasures(measures3)
      Assert.assertEquals(2, output3.length)
      Assert.assertEquals(Some(0), output3.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output3.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output3.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output3.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output3.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output3.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // nessuna matricola coincidente (misura prel rettificata a prescindere dalla coincidenza delle matricole)
      val measures4 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output4 = CambioMisuratoreController.rectifyMeasures(measures4)
      Assert.assertEquals(2, output4.length)
      Assert.assertEquals(Some(0), output4.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output4.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1000), output4.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(2000), output4.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output4.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output4.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // matricola coincidente solo in PRE (misura prel rettificata a prescindere dalla coincidenza delle matricole)
      val measures5 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output5 = CambioMisuratoreController.rectifyMeasures(measures5)
      Assert.assertEquals(2, output5.length)
      Assert.assertEquals(Some(0), output5.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output5.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output5.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(2000), output5.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output5.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output5.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // matricola coincidente solo in POST (misura prel rettificata a prescindere dalla coincidenza delle matricole)
      val measures6 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output6 = CambioMisuratoreController.rectifyMeasures(measures6)
      Assert.assertEquals(2, output6.length)
      Assert.assertEquals(Some(0), output6.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output6.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1000), output6.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output6.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output6.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output6.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // matricola coincidente solo in POST + presenza di altri flussi (misura prel rettificata a prescindere dalla coincidenza delle matricole)
      val measures13 = List(
        Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = None, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = None, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output13 = CambioMisuratoreController.rectifyMeasures(measures13)
      Assert.assertEquals(2, output13.length)
      Assert.assertEquals(Some(0), output13.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output13.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1000), output13.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(1), output13.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), output13.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output13.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // matricola coincidente solo in POST + presenza di altri flussi + matricola coincidente solo in PRE (misura prel rettificata a prescindere dalla coincidenza delle matricole)
      val measures14 = List(
        Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X1"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0204/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0204/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X2"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output14 = CambioMisuratoreController.rectifyMeasures(measures14).sorted(Flow.priorityOrderingFlows)
      Assert.assertEquals(2, output14.length)
      Assert.assertEquals(Some(0), output14.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), output14.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1), output14.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(2000), output14.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X2"), output14.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X2"), output14.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // matricole NULL
      val measuresMatricoleNull1 = List(
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val outputMatricoleNull1 = CambioMisuratoreController.rectifyMeasures(measuresMatricoleNull1)
      Assert.assertEquals(2, outputMatricoleNull1.length)
      Assert.assertEquals(Some(0), outputMatricoleNull1.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), outputMatricoleNull1.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1000), outputMatricoleNull1.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(2000), outputMatricoleNull1.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), outputMatricoleNull1.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), outputMatricoleNull1.filter(f => f.service == "IGMGPOST").head.fileRettifica)

      // Rettifica trasmessa prima di flusso di cambio misuratore
      val measuresReffificaBeforeCM1 = List(
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(10),
          converted = Some(11), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val outputMeasuresReffificaBeforeCM1 = CambioMisuratoreController.rectifyMeasures(measuresReffificaBeforeCM1)
      Assert.assertEquals(2, outputMeasuresReffificaBeforeCM1.length)
      Assert.assertEquals(Some(0), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPRE").head.measure)
      Assert.assertEquals(Some(0), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPOST").head.measure)
      Assert.assertEquals(Some(1000), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPRE").head.converted)
      Assert.assertEquals(Some(2000), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPOST").head.converted)
      Assert.assertEquals(Some("X"), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), outputMeasuresReffificaBeforeCM1.filter(f => f.service == "IGMGPOST").head.fileRettifica)


      // IM1

      // tutte matricole coincidenti
      val measures7 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output7 = CambioMisuratoreController.rectifyMeasures(measures7)
      Assert.assertEquals(2, output7.length)
      Assert.assertEquals(Some(0), output7.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(0), output7.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), output7.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(1), output7.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output7.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output7.filter(f => f.service == "IM1POST").head.fileRettifica)

      // solo matricola misuratore coincidente
      val measures8 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output8 = CambioMisuratoreController.rectifyMeasures(measures8)
      Assert.assertEquals(2, output8.length)
      Assert.assertEquals(Some(0), output8.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(0), output8.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1000), output8.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(2000), output8.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output8.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output8.filter(f => f.service == "IM1POST").head.fileRettifica)

      // solo matricola convertitore coincidente
      val measures9 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output9 = CambioMisuratoreController.rectifyMeasures(measures9)
      Assert.assertEquals(2, output9.length)
      Assert.assertEquals(Some(0), output9.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(0), output9.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), output9.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(1), output9.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output9.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output9.filter(f => f.service == "IM1POST").head.fileRettifica)

      // nessuna matricola coincidente (misura prel rettificata a prescindere dalla coincidenza delle matricole)
      val measures10 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output10 = CambioMisuratoreController.rectifyMeasures(measures10)
      Assert.assertEquals(2, output10.length)
      Assert.assertEquals(Some(0), output10.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(0), output10.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1000), output10.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(2000), output10.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output10.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output10.filter(f => f.service == "IM1POST").head.fileRettifica)

      // matricola coincidente solo in PRE (misura prel rettificata a prescindere dalla coincidenza delle matricole)
      val measures11 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output11 = CambioMisuratoreController.rectifyMeasures(measures11)
      Assert.assertEquals(2, output11.length)
      Assert.assertEquals(Some(0), output11.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(0), output11.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), output11.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(2000), output11.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output11.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output11.filter(f => f.service == "IM1POST").head.fileRettifica)

      // matricola coincidente solo in POST (misura prel rettificata a prescindere dalla coincidenza delle matricole)
      val measures12 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output12 = CambioMisuratoreController.rectifyMeasures(measures12)
      Assert.assertEquals(2, output12.length)
      Assert.assertEquals(Some(0), output12.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(0), output12.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1000), output12.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(1), output12.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), output12.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), output12.filter(f => f.service == "IM1POST").head.fileRettifica)

      // matricola coincidente solo in POST + presenza di altri flussi + matricola coincidente solo in PRE (misura prel rettificata a prescindere dalla coincidenza delle matricole)
      val measures15 = List(
        Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X1"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0204/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0204/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
          local_file = Some("X2"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val output15 = CambioMisuratoreController.rectifyMeasures(measures15).sorted(Flow.priorityOrderingFlows)
      Assert.assertEquals(2, output15.length)
      Assert.assertEquals(Some(0), output15.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(0), output15.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1), output15.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(2000), output15.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X2"), output15.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X2"), output15.filter(f => f.service == "IM1POST").head.fileRettifica)

      // matricole NULL
      val measuresMatricoleNull2 = List(
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val outputMatricoleNull2 = CambioMisuratoreController.rectifyMeasures(measuresMatricoleNull2)
      Assert.assertEquals(2, outputMatricoleNull2.length)
      Assert.assertEquals(Some(0), outputMatricoleNull2.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(0), outputMatricoleNull2.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1000), outputMatricoleNull2.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(2000), outputMatricoleNull2.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), outputMatricoleNull2.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), outputMatricoleNull2.filter(f => f.service == "IM1POST").head.fileRettifica)

      // Rettifica trasmessa prima di flusso di cambio misuratore
      val measuresReffificaBeforeCM2 = List(
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(10),
          converted = Some(11), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
          converted = Some(1), serialNumberMis = None, serialNumberConv = None,
          local_file = Some("X"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
          converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
          converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
          local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None)
      ).map(_.asInstanceOf[Flow])

      val outputMeasuresReffificaBeforeCM2 = CambioMisuratoreController.rectifyMeasures(measuresReffificaBeforeCM2)
      Assert.assertEquals(2, outputMeasuresReffificaBeforeCM2.length)
      Assert.assertEquals(Some(0), outputMeasuresReffificaBeforeCM2.filter(f => f.service == "IM1PRE").head.measure)
      Assert.assertEquals(Some(0), outputMeasuresReffificaBeforeCM2.filter(f => f.service == "IM1POST").head.measure)
      Assert.assertEquals(Some(1000), outputMeasuresReffificaBeforeCM2.filter(f => f.service == "IM1PRE").head.converted)
      Assert.assertEquals(Some(2000), outputMeasuresReffificaBeforeCM2.filter(f => f.service == "IM1POST").head.converted)
      Assert.assertEquals(Some("X"), outputMeasuresReffificaBeforeCM2.filter(f => f.service == "IM1PRE").head.fileRettifica)
      Assert.assertEquals(Some("X"), outputMeasuresReffificaBeforeCM2.filter(f => f.service == "IM1POST").head.fileRettifica)

    }

  }

  /**
   * Questo test verifica la corretta esecuzione delle rettifiche ai flussi di cambio misuratore IM1 e IGMG
   * Vengono testati una serie di possibili scenari partendo da un set di misure di input considerando uno o più pdr
   * e una o più date delle misure
   */
  def testRettificaCambioMisuratore(): Unit = {
    val format = new SimpleDateFormat("yyyy-mm-dd")
    val cau_int_mis = Some(1)
    val cau_int_cor = Some(1)

    // singolo PDR e singola data, lista di flussi caricati in giorni successivi
    val measures1 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
        converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
        converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0204/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
        converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
        converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0206/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val output1 = CambioMisuratoreController.rettificaCambioMisuratore(measures1).cache()
    Assert.assertEquals(2, output1.count())
    Assert.assertEquals(1, output1.filter(f => f.service == "IM1PRE").count())
    Assert.assertEquals(1, output1.filter(f => f.service == "IM1POST").count())
    Assert.assertEquals(Some(0), output1.filter(f => f.service == "IM1PRE" && f.dateLoadFromLocalFile == 20200205).collect().head.measure)
    Assert.assertEquals(Some(1500), output1.filter(f => f.service == "IM1POST" && f.dateLoadFromLocalFile == 20200205).collect().head.measure)
    Assert.assertEquals(Some(1), output1.filter(f => f.service == "IM1PRE" && f.dateLoadFromLocalFile == 20200205).collect().head.converted)
    Assert.assertEquals(Some(2000), output1.filter(f => f.service == "IM1POST" && f.dateLoadFromLocalFile == 20200205).collect().head.converted)
    Assert.assertEquals(Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0206/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), output1.filter(f => f.service == "IM1PRE" && f.dateLoadFromLocalFile == 20200205).collect().head.fileRettifica)
    Assert.assertEquals(None, output1.filter(f => f.service == "IM1POST" && f.dateLoadFromLocalFile == 20200205).collect().head.fileRettifica)


    // due diversi PDR e singola data, flussi caricati in giorni successivi
    val measures2 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
        converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
        converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "2", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(service = "IM1PRE", pdr = "2", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
        converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "2", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
        converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "2", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val output2 = CambioMisuratoreController.rettificaCambioMisuratore(measures2).cache()
    Assert.assertEquals(4, output2.count())
    Assert.assertEquals(2, output2.filter(f => f.service == "IM1PRE").count())
    Assert.assertEquals(2, output2.filter(f => f.service == "IM1POST").count())
    Assert.assertEquals(Some(500), output2.filter(f => f.service == "IM1PRE" && f.dateLoadFromLocalFile == 20200202 && f.pdr == "1").collect().head.measure)
    Assert.assertEquals(Some(0), output2.filter(f => f.service == "IM1POST" && f.dateLoadFromLocalFile == 20200202 && f.pdr == "1").collect().head.measure)
    Assert.assertEquals(Some(1000), output2.filter(f => f.service == "IM1PRE" && f.dateLoadFromLocalFile == 20200202 && f.pdr == "1").collect().head.converted)
    Assert.assertEquals(Some(1), output2.filter(f => f.service == "IM1POST" && f.dateLoadFromLocalFile == 20200202 && f.pdr == "1").collect().head.converted)
    Assert.assertEquals(None, output2.filter(f => f.service == "IM1PRE" && f.dateLoadFromLocalFile == 20200202 && f.pdr == "1").collect().head.fileRettifica)
    Assert.assertEquals(Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), output2.filter(f => f.service == "IM1POST" && f.dateLoadFromLocalFile == 20200202 && f.pdr == "1").collect().head.fileRettifica)
    Assert.assertEquals(Some(0), output2.filter(f => f.service == "IM1PRE" && f.dateLoadFromLocalFile == 20200202 && f.pdr == "2").collect().head.measure)
    Assert.assertEquals(Some(1500), output2.filter(f => f.service == "IM1POST" && f.dateLoadFromLocalFile == 20200202 && f.pdr == "2").collect().head.measure)
    Assert.assertEquals(Some(1), output2.filter(f => f.service == "IM1PRE" && f.dateLoadFromLocalFile == 20200202 && f.pdr == "2").collect().head.converted)
    Assert.assertEquals(Some(2000), output2.filter(f => f.service == "IM1POST" && f.dateLoadFromLocalFile == 20200202 && f.pdr == "2").collect().head.converted)
    Assert.assertEquals(Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), output2.filter(f => f.service == "IM1PRE" && f.dateLoadFromLocalFile == 20200202 && f.pdr == "2").collect().head.fileRettifica)
    Assert.assertEquals(None, output2.filter(f => f.service == "IM1POST" && f.dateLoadFromLocalFile == 20200202 && f.pdr == "2").collect().head.fileRettifica)


    // singolo PDR e due diverse date, lista di flussi caricati negli stessi giorni
    val measures3 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
        converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
        converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-02")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-02")), readType = None, measure = Some(500),
        converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-02")), readType = None, measure = Some(1500),
        converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-02")), measure = Some(0),
        converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val output3 = CambioMisuratoreController.rettificaCambioMisuratore(measures3).cache()
    Assert.assertEquals(4, output3.count())
    Assert.assertEquals(2, output3.filter(f => f.date.get == format.parse("2020-01-01")).count())
    Assert.assertEquals(2, output3.filter(f => f.date.get == format.parse("2020-01-02")).count())
    Assert.assertEquals(2, output3.filter(f => f.service == "IM1PRE").count())
    Assert.assertEquals(2, output3.filter(f => f.service == "IM1POST").count())
    Assert.assertEquals(Some(500), output3.filter(f => f.service == "IM1PRE" && f.date.get == format.parse("2020-01-01")).collect().head.measure)
    Assert.assertEquals(Some(0), output3.filter(f => f.service == "IM1POST" && f.date.get == format.parse("2020-01-01")).collect().head.measure)
    Assert.assertEquals(Some(1000), output3.filter(f => f.service == "IM1PRE" && f.date.get == format.parse("2020-01-01")).collect().head.converted)
    Assert.assertEquals(Some(1), output3.filter(f => f.service == "IM1POST" && f.date.get == format.parse("2020-01-01")).collect().head.converted)
    Assert.assertEquals(None, output3.filter(f => f.service == "IM1PRE" && f.date.get == format.parse("2020-01-01")).collect().head.fileRettifica)
    Assert.assertEquals(Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), output3.filter(f => f.service == "IM1POST" && f.date.get == format.parse("2020-01-01")).collect().head.fileRettifica)
    Assert.assertEquals(Some(0), output3.filter(f => f.service == "IM1PRE" && f.date.get == format.parse("2020-01-02")).collect().head.measure)
    Assert.assertEquals(Some(1500), output3.filter(f => f.service == "IM1POST" && f.date.get == format.parse("2020-01-02")).collect().head.measure)
    Assert.assertEquals(Some(1), output3.filter(f => f.service == "IM1PRE" && f.date.get == format.parse("2020-01-02")).collect().head.converted)
    Assert.assertEquals(Some(2000), output3.filter(f => f.service == "IM1POST" && f.date.get == format.parse("2020-01-02")).collect().head.converted)
    Assert.assertEquals(Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), output3.filter(f => f.service == "IM1PRE" && f.date.get == format.parse("2020-01-02")).collect().head.fileRettifica)
    Assert.assertEquals(None, output3.filter(f => f.service == "IM1POST" && f.date.get == format.parse("2020-01-02")).collect().head.fileRettifica)


    // due diversi PDR e due diverse date, flussi caricati in giorni successivi
    val measures4 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
        converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
        converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "2", service = "TML", date = Some(format.parse("2020-01-02")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(service = "IM1PRE", pdr = "2", date = Some(format.parse("2020-01-02")), readType = None, measure = Some(500),
        converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "2", date = Some(format.parse("2020-01-02")), readType = None, measure = Some(1500),
        converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "2", service = "RML", date = Some(format.parse("2020-01-02")), measure = Some(0),
        converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val output4 = CambioMisuratoreController.rettificaCambioMisuratore(measures4).cache()
    Assert.assertEquals(4, output4.count())
    Assert.assertEquals(2, output4.filter(f => f.date.get == format.parse("2020-01-01")).count())
    Assert.assertEquals(2, output4.filter(f => f.date.get == format.parse("2020-01-02")).count())
    Assert.assertEquals(2, output4.filter(f => f.service == "IM1PRE").count())
    Assert.assertEquals(2, output4.filter(f => f.service == "IM1POST").count())
    Assert.assertEquals(Some(500), output4.filter(f => f.service == "IM1PRE" && f.date.get == format.parse("2020-01-01") && f.pdr == "1").collect().head.measure)
    Assert.assertEquals(Some(0), output4.filter(f => f.service == "IM1POST" && f.date.get == format.parse("2020-01-01") && f.pdr == "1").collect().head.measure)
    Assert.assertEquals(Some(1000), output4.filter(f => f.service == "IM1PRE" && f.date.get == format.parse("2020-01-01") && f.pdr == "1").collect().head.converted)
    Assert.assertEquals(Some(1), output4.filter(f => f.service == "IM1POST" && f.date.get == format.parse("2020-01-01") && f.pdr == "1").collect().head.converted)
    Assert.assertEquals(None, output4.filter(f => f.service == "IM1PRE" && f.date.get == format.parse("2020-01-01") && f.pdr == "1").collect().head.fileRettifica)
    Assert.assertEquals(Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), output4.filter(f => f.service == "IM1POST" && f.date.get == format.parse("2020-01-01") && f.pdr == "1").collect().head.fileRettifica)
    Assert.assertEquals(Some(0), output4.filter(f => f.service == "IM1PRE" && f.date.get == format.parse("2020-01-02") && f.pdr == "2").collect().head.measure)
    Assert.assertEquals(Some(1500), output4.filter(f => f.service == "IM1POST" && f.date.get == format.parse("2020-01-02") && f.pdr == "2").collect().head.measure)
    Assert.assertEquals(Some(1), output4.filter(f => f.service == "IM1PRE" && f.date.get == format.parse("2020-01-02") && f.pdr == "2").collect().head.converted)
    Assert.assertEquals(Some(2000), output4.filter(f => f.service == "IM1POST" && f.date.get == format.parse("2020-01-02") && f.pdr == "2").collect().head.converted)
    Assert.assertEquals(Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), output4.filter(f => f.service == "IM1PRE" && f.date.get == format.parse("2020-01-02") && f.pdr == "2").collect().head.fileRettifica)
    Assert.assertEquals(None, output4.filter(f => f.service == "IM1POST" && f.date.get == format.parse("2020-01-02") && f.pdr == "2").collect().head.fileRettifica)


    // flussi di cambio misuratore trasmessi dopo la rettifica => rettifica applicata comunque
    val measures5 = Environment.getSparkContext.parallelize(List(
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
        converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
        converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val output5 = CambioMisuratoreController.rettificaCambioMisuratore(measures5).cache()
    Assert.assertEquals(2, output5.count())
    Assert.assertEquals(Some(0), output5.filter(f => f.service == "IM1PRE").collect().head.measure)
    Assert.assertEquals(Some(1500), output5.filter(f => f.service == "IM1POST").collect().head.measure)
    Assert.assertEquals(Some(1), output5.filter(f => f.service == "IM1PRE").collect().head.converted)
    Assert.assertEquals(Some(2000), output5.filter(f => f.service == "IM1POST").collect().head.converted)
    Assert.assertEquals(Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), output5.filter(f => f.service == "IM1PRE").collect().head.fileRettifica)
    Assert.assertEquals(None, output5.filter(f => f.service == "IM1POST").collect().head.fileRettifica)


    // presenza flusso tra rettifica e flussi cambio misuratore => la rettifica agisce sul cambio misuratore e non sul flusso in mezzo
    val measures6 = Environment.getSparkContext.parallelize(List(
      Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
        converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
        converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val output6 = CambioMisuratoreController.rettificaCambioMisuratore(measures6).cache()
    Assert.assertEquals(2, output6.count())
    Assert.assertEquals(Some(0), output6.filter(f => f.service == "IM1PRE").collect().head.measure)
    Assert.assertEquals(Some(1500), output6.filter(f => f.service == "IM1POST").collect().head.measure)
    Assert.assertEquals(Some(1), output6.filter(f => f.service == "IM1PRE").collect().head.converted)
    Assert.assertEquals(Some(2000), output6.filter(f => f.service == "IM1POST").collect().head.converted)
    Assert.assertEquals(Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), output6.filter(f => f.service == "IM1PRE").collect().head.fileRettifica)
    Assert.assertEquals(None, output6.filter(f => f.service == "IM1POST").collect().head.fileRettifica)


    // assenza di rettifica
    val measures7 = Environment.getSparkContext.parallelize(List(
      Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
        converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
        converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val output7 = CambioMisuratoreController.rettificaCambioMisuratore(measures7).cache()
    Assert.assertEquals(3, output7.count())
    Assert.assertEquals(Some(500), output7.filter(f => f.service == "IM1PRE").collect().head.measure)
    Assert.assertEquals(Some(1500), output7.filter(f => f.service == "IM1POST").collect().head.measure)
    Assert.assertEquals(Some(1000), output7.filter(f => f.service == "IM1PRE").collect().head.converted)
    Assert.assertEquals(Some(2000), output7.filter(f => f.service == "IM1POST").collect().head.converted)
    Assert.assertEquals(None, output7.filter(f => f.service == "IM1PRE").collect().head.fileRettifica)
    Assert.assertEquals(None, output7.filter(f => f.service == "IM1POST").collect().head.fileRettifica)


    // presenza di due rettifiche dopo cambio misuratore => viene applicata solo l'ultima, poi vengono tutte scartate alla fine del processo (quindi prima di arrivare al motore delle priorità)
    val measures8 = Environment.getSparkContext.parallelize(List(
      Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
        converted = Some(1000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
        converted = Some(2000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_IM10050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(0), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(1), serialNumberMis = Some("100"), serialNumberConv = Some("200"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val output8 = CambioMisuratoreController.rettificaCambioMisuratore(measures8).cache()
    Assert.assertEquals(2, output8.count())
    Assert.assertEquals(Some(1), output8.filter(f => f.service == "IM1PRE").collect().head.measure)
    Assert.assertEquals(Some(1500), output8.filter(f => f.service == "IM1POST").collect().head.measure)
    Assert.assertEquals(Some(1), output8.filter(f => f.service == "IM1PRE").collect().head.converted)
    Assert.assertEquals(Some(2000), output8.filter(f => f.service == "IM1POST").collect().head.converted)
    Assert.assertEquals(Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), output8.filter(f => f.service == "IM1PRE").collect().head.fileRettifica)
    Assert.assertEquals(None, output8.filter(f => f.service == "IM1POST").collect().head.fileRettifica)
  }
}
