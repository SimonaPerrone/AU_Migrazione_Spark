package it.sferanet.au.controller.validation

import it.sferanet.au.model.Flow
import it.sferanet.au.model.autolettura._
import it.sferanet.au.model.periodico._
import it.sferanet.au.model.prestazionale._
import it.sferanet.au.model.rettifica._
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.{EnvironmentSparkTest, SparkTest}
import org.junit.Assert

import java.text.SimpleDateFormat

/**
 * Questa classe la corretta esecuzione del controller dell'intero processo di validazione:
 * - testGetLastLoadingMeasuresVersion: ottenimento dell'ultima versione dei dati caricati in Hive (una sorta di funzione sql "lead")
 * - testGetMeasuresGroup e testApplyRectificationLogic: applicazione motore degli annullamenti e delle rettiche di cambio misuratore implementati in CancelController.scala e CambioMisuratoreController.scala (con le relative classi di test)
 * - testGetPriorityMeasures: esecuzione motore di validazione e priorità delle misure
 *
 * testGetMeasures: esecuzione sequenziale di tutti gli step precedenti necessari
 */
class ValidationControllerTest extends EnvironmentSparkTest {

  def testGetLastLoadingMeasuresVersion(): Unit = {
    val format1 = new SimpleDateFormat("yyyy-mm-dd")
    val format2 = Constants.FORMAT_DATE_LOAD
    val vc = new ValidationController

    // simulazione caricamento in Hive dello stesso file più volte (cambia solo d_caricamento)
    val measures1 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = Some(format2.parse("2020-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = Some(format2.parse("2021-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    vc.getLastLoadingMeasuresVersion(measures1).foreach(println)
    val output1 = ValidationController.getLastLoadingMeasuresVersion(measures1).cache()
    Assert.assertEquals(1, output1.count())
    Assert.assertEquals(format2.parse("2021-04-04T22:34:53.905225"), output1.take(1).head.d_caricamento.get)

    // simulazione caricamento nel cloud dello stesso file più volte da parte del distributore (cambia il path del local_file e d_caricamento)
    val measures2 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = Some(format2.parse("2020-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = Some(format2.parse("2021-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = Some(format2.parse("2022-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    vc.getLastLoadingMeasuresVersion(measures2).foreach(println)
    val output2 = ValidationController.getLastLoadingMeasuresVersion(measures2).cache()
    Assert.assertEquals(1, output2.count())
    Assert.assertEquals(format2.parse("2022-04-04T22:34:53.905225"), output2.take(1).head.d_caricamento.get)

    // caricamento multiplo in Hive di due diversi files
    val measures3 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), d_caricamento = Some(format2.parse("2020-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), d_caricamento = Some(format2.parse("2021-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = Some(format2.parse("2022-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = Some(format2.parse("2023-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    vc.getLastLoadingMeasuresVersion(measures3).foreach(println)
    val output3 = ValidationController.getLastLoadingMeasuresVersion(measures3).cache()
    Assert.assertEquals(2, output3.count())
    Assert.assertEquals(1, output3.take(2).count(f => (f.service, f.d_caricamento.get) == ("TGL", format2.parse("2021-04-04T22:34:53.905225"))))
    Assert.assertEquals(1, output3.take(2).count(f => (f.service, f.d_caricamento.get) == ("TML", format2.parse("2023-04-04T22:34:53.905225"))))

    // caricamento più volte da parte del distributore di due diversi files in giorni diversi
    val measures4 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), d_caricamento = Some(format2.parse("2020-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), d_caricamento = Some(format2.parse("2021-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = Some(format2.parse("2022-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = Some(format2.parse("2023-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    vc.getLastLoadingMeasuresVersion(measures4).foreach(println)
    val output4 = ValidationController.getLastLoadingMeasuresVersion(measures4).cache()
    Assert.assertEquals(2, output4.count())
    Assert.assertEquals(1, output4.take(2).count(f => (f.service, f.d_caricamento.get) == ("TGL", format2.parse("2021-04-04T22:34:53.905225"))))
    Assert.assertEquals(1, output4.take(2).count(f => (f.service, f.d_caricamento.get) == ("TML", format2.parse("2023-04-04T22:34:53.905225"))))


    // simulazione caricamento in Hive dello stesso file più volte con case estensione differente (cambia solo d_caricamento)
    val measures5 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = Some(format2.parse("2020-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.XmL"), d_caricamento = Some(format2.parse("2021-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    vc.getLastLoadingMeasuresVersion(measures5).foreach(println)
    val output5 = ValidationController.getLastLoadingMeasuresVersion(measures5).cache()
    Assert.assertEquals(1, output5.count())
    Assert.assertEquals(format2.parse("2021-04-04T22:34:53.905225"), output5.take(1).head.d_caricamento.get)


    // simulazione caricamento in Hive di file con timestamp mal formattato
    val measures6 = Environment.getSparkContext.parallelize(List(
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format1.parse("2020-01-01")), readType = Some('E'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_202001_IM10050_0030_1.xml"), d_caricamento = Some(format2.parse("2021-04-04T22:34:53.905225")), isNewRoute = false,
        coefCorr = None, cau_int_cor = None, cau_int_mis = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format1.parse("2020-01-01")), readType = Some('E'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_202001_IM10050_0030_1.xml"), d_caricamento = Some(format2.parse("2020-01-01T22:34:53.905225")), isNewRoute = false,
        coefCorr = None, cau_int_cor = None, cau_int_mis = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format1.parse("2020-12-31")), readType = Some('E'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_202001_IM10050_0030_1.xml"), d_caricamento = Some(format2.parse("2020-01-01T22:34:53.905225")), isNewRoute = false,
        coefCorr = None, cau_int_cor = None, cau_int_mis = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format1.parse("2020-12-31")), readType = Some('E'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_202001_IM10050_0030_1.xml"), d_caricamento = Some(format2.parse("2021-04-04T22:34:53.905225")), isNewRoute = false,
        coefCorr = None, cau_int_cor = None, cau_int_mis = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "2", service = "TGL", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_202001_TGL0050_0030_1.xml"), d_caricamento = Some(format2.parse("2020-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "2", service = "TGL", date = Some(format1.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_202001_TGL0050_0030_1.xml"), d_caricamento = Some(format2.parse("2021-04-04T22:34:53.905225")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    vc.getLastLoadingMeasuresVersion(measures5).foreach(println)
    val output6 = ValidationController.getLastLoadingMeasuresVersion(measures6).cache()
    Assert.assertEquals(3, output6.count())
    Assert.assertEquals(format2.parse("2021-04-04T22:34:53.905225"), output6.take(1).head.d_caricamento.get)

  }

  def testGetMeasuresGroup(): Unit = {
    val format = new SimpleDateFormat("yyyy-mm-dd")
    val vc = new ValidationController

    val measures1 = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_2.xml"), motivation = Some(2), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_2.xml"), motivation = Some(2), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_3.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_3.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_4.xml"), motivation = Some(4), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_4.xml"), motivation = Some(4), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), motivation = Some(5), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), motivation = Some(5), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),

      IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = None,
        converted = None, serialNumberMis = None, serialNumberConv = None, cau_int_mis = None, cau_int_cor = None,
        local_file = None, d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = None,
        converted = None, serialNumberMis = None, serialNumberConv = None, cau_int_mis = None, cau_int_cor = None,
        local_file = None, d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = None,
        converted = None, serialNumberMis = None, serialNumberConv = None, cau_int_mis = None, cau_int_cor = None,
        local_file = None, d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = None,
        converted = None, serialNumberMis = None, serialNumberConv = None, cau_int_mis = None, cau_int_cor = None,
        local_file = None, d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),

      Tal(pdr = "1", service = "TAL", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tas(pdr = "1", service = "TAS", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tav(pdr = "1", service = "TAS", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_6.xml"), motivation = Some(6), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_6.xml"), motivation = Some(6), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),


      Tmv(pdr = "1", service = "TMV", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Sw1(pdr = "1", service = "SW1", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Swg1(pdr = "1", service = "SWG1", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      FUI(pdr = "1", service = "FUI", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      FDD(pdr = "1", service = "FDD", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0105/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A01(pdr = "1", service = "A01", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0106/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), outcome = None, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A40(pdr = "1", service = "A40", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0107/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), outcome = None, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Sm1(pdr = "1", service = "SM1", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0108/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), outcome = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      AD2(pdr = "1", service = "AD2", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0109/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      AD3(pdr = "1", service = "AD3", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A02(pdr = "1", service = "A02", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0111/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      S02(pdr = "1", service = "S02", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0112/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      S40(pdr = "1", service = "S40", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0113/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      R01(pdr = "1", service = "R01", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0114/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, outcome = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      R40(pdr = "1", service = "R40", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0115/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      M01(pdr = "1", service = "M01", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0116/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V01(pdr = "1", service = "V01", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0117/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V02(pdr = "1", service = "V02", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0118/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rmv(pdr = "1", service = "RMV", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0119/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rsl(pdr = "1", service = "RSL", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0120/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rsl(pdr = "1", service = "RSL", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0121/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rsl(pdr = "1", service = "RSL", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0122/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rsl(pdr = "1", service = "RSL", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0123/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A01R(pdr = "1", service = "A01R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0124/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A40R(pdr = "1", service = "A40R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0125/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      SM1R(pdr = "1", service = "SM1R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0126/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      AD2R(pdr = "1", service = "AD2R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0127/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      AD3R(pdr = "1", service = "AD3R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0128/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A02R(pdr = "1", service = "A02R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0129/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      S02R(pdr = "1", service = "S02R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0130/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      S40R(pdr = "1", service = "S40R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0131/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      R01r(pdr = "1", service = "R01R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      R40r(pdr = "1", service = "R40R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      M01r(pdr = "1", service = "M01R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V01R(pdr = "1", service = "V01R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0204/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V02R(pdr = "1", service = "V02R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)

    )).map(_.asInstanceOf[Flow])

    Assert.assertEquals(12, vc.getMeasuresGroup(measures1, 1).count())
    Assert.assertEquals(4, vc.getMeasuresGroup(measures1, 2).count())
    Assert.assertEquals(5, vc.getMeasuresGroup(measures1, 3).count())
    Assert.assertEquals(36, vc.getMeasuresGroup(measures1, 4).count())
  }

  def testApplyRectificationLogic(): Unit = {
    val format = new SimpleDateFormat("yyyy-mm-dd")
    val vc = new ValidationController
    val numberPartitions = 2
    //Environment.resetProperty()
    Environment.setProperty("dataset.numberPartition", numberPartitions.toString)

    val measures1 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),


      IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = None,
        converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0105/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = None,
        converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0105/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = None,
        converted = None, serialNumberMis = Some("300"), serialNumberConv = Some("400"), cau_int_mis = Some(1), cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0106/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = None,
        converted = None, serialNumberMis = Some("300"), serialNumberConv = Some("400"), cau_int_mis = Some(1), cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0106/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),

      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(15),
        converted = Some(10), serialNumberConv = Some("200"), serialNumberMis = Some("100"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0107/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), motivation = Some(2), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(15),
        converted = Some(10), serialNumberConv = Some("400"), serialNumberMis = Some("300"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0108/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), motivation = Some(2), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),


      Tal(pdr = "1", service = "TAL", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tas(pdr = "1", service = "TAS", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tav(pdr = "1", service = "TAS", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_6.xml"), motivation = Some(6), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_6.xml"), motivation = Some(6), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),


      Tmv(pdr = "1", service = "TMV", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Sw1(pdr = "1", service = "SW1", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Swg1(pdr = "1", service = "SWG1", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      FUI(pdr = "1", service = "FUI", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      FDD(pdr = "1", service = "FDD", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0105/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A01(pdr = "1", service = "A01", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0106/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), outcome = None, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A40(pdr = "1", service = "A40", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0107/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), outcome = None, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Sm1(pdr = "1", service = "SM1", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0108/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), outcome = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      AD2(pdr = "1", service = "AD2", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0109/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      AD3(pdr = "1", service = "AD3", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A02(pdr = "1", service = "A02", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0111/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      S02(pdr = "1", service = "S02", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0112/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      S40(pdr = "1", service = "S40", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0113/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      R01(pdr = "1", service = "R01", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0114/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, outcome = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      R40(pdr = "1", service = "R40", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0115/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      M01(pdr = "1", service = "M01", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0116/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V01(pdr = "1", service = "V01", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0117/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V02(pdr = "1", service = "V02", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0118/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rmv(pdr = "1", service = "RMV", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0119/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rsl(pdr = "1", service = "RSL", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0120/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rsl(pdr = "1", service = "RSL", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0121/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rsl(pdr = "1", service = "RSL", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0122/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rsl(pdr = "1", service = "RSL", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0123/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A01R(pdr = "1", service = "A01R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0124/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A40R(pdr = "1", service = "A40R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0125/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      SM1R(pdr = "1", service = "SM1R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0126/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      AD2R(pdr = "1", service = "AD2R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0127/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      AD3R(pdr = "1", service = "AD3R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0128/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A02R(pdr = "1", service = "A02R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0129/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      S02R(pdr = "1", service = "S02R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0130/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      S40R(pdr = "1", service = "S40R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0131/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      R01r(pdr = "1", service = "R01R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      R40r(pdr = "1", service = "R40R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0202/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      M01r(pdr = "1", service = "M01R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0203/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V01R(pdr = "1", service = "V01R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0204/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V02R(pdr = "1", service = "V02R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)

    )).map(_.asInstanceOf[Flow])

    val output1 = vc.applyRectificationLogic(measures1).cache()
    output1.collect.foreach(println)
    Assert.assertEquals(3, output1.count())
    Assert.assertEquals(1, output1.filter(f => f.service == "TAL").count())
    Assert.assertEquals(1, output1.filter(f => f.service == "IM1PRE").count())
    Assert.assertEquals(1, output1.filter(f => f.service == "IM1POST").count())
    Assert.assertEquals(Some(15), output1.filter(f => f.service == "IM1PRE").collect().head.measure)
    Assert.assertEquals(Some(10), output1.filter(f => f.service == "IM1PRE").collect().head.converted)
    Assert.assertEquals(Some(15), output1.filter(f => f.service == "IM1POST").collect().head.measure)
    Assert.assertEquals(Some(10), output1.filter(f => f.service == "IM1POST").collect().head.converted)
  }

  def testGetPriorityMeasures(): Unit = {
    val format = new SimpleDateFormat("yyyy-mm-dd")
    val vc = new ValidationController

    for (isNewRoute <- List(true, false)) {
      // singolo PDR, singola data delle misure e singola data di caricamento nel cloud 2020-01-01
      val measuresList = List(
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = isNewRoute, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_RGL0050_20200109151501_5.xml"), motivation = Some(4), d_caricamento = None, isNewRoute = isNewRoute, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = None,
          converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = None,
          converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('S'), measure = None,
          converted = None, serialNumberMis = Some("300"), serialNumberConv = Some("400"), cau_int_mis = Some(1), cau_int_cor = Some(1),
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IM10050_20200109151501_5.xml"), d_caricamento = None, isNewRoute = false, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('S'), measure = None,
          converted = None, serialNumberMis = Some("300"), serialNumberConv = Some("400"), cau_int_mis = Some(1), cau_int_cor = Some(1),
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IM10050_20200109151501_5.xml"), d_caricamento = None, isNewRoute = false, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_RGL0050_20200109151501_5.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = isNewRoute, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rmv(pdr = "1", service = "RMV", date = Some(format.parse("2020-01-01")), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0119/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(2), d_caricamento = None, isNewRoute = isNewRoute, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tmv(pdr = "1", service = "TMV", date = Some(format.parse("2020-01-01")), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tal(pdr = "1", service = "TAL", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tav(pdr = "1", service = "TAS", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tas(pdr = "1", service = "TAS", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rsl(pdr = "1", service = "RSL", date = Some(format.parse("2020-01-01")), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0120/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(2), d_caricamento = None, isNewRoute = isNewRoute, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Swg1(pdr = "1", service = "SWG1", date = Some(format.parse("2020-01-01")), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
        FUI(pdr = "1", service = "FUI", date = Some(format.parse("2020-01-01")), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        FDD(pdr = "1", service = "FDD", date = Some(format.parse("2020-01-01")), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0105/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Sw1(pdr = "1", service = "SW1", date = Some(format.parse("2020-01-01")), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None)

        // altri flussi successivi non aggiunti in quanto questo test valuta solo la correttezza dell'algoritmo

      ).map(_.asInstanceOf[Flow])

      for (i <- measuresList.indices) {
        if (!measuresList(i).service.endsWith("POST")) {
          val measuresRdd = Environment.getSparkContext.parallelize(scala.util.Random.shuffle(measuresList.takeRight(measuresList.length - i)))
          val priorityResult = vc.getPriorityMeasures(measuresRdd).cache()
          val expectedFlowWin = if (measuresList(i).service.startsWith("IM1")) (2, "IM1")
          else if (measuresList(i).service.startsWith("IGMG")) (2, "IGMG")
          else (1, measuresList(i).service)

          Assert.assertEquals(expectedFlowWin._1, priorityResult.count())
          Assert.assertTrue(priorityResult.collect().forall(f => f.service.startsWith(expectedFlowWin._2)))
        }
      }
    }


    // data di caricamento nel cloud più alta
    val measures1 = Environment.getSparkContext.parallelize(List(
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val priorityResult1 = vc.getPriorityMeasures(measures1).cache()
    Assert.assertEquals(1, priorityResult1.count())
    Assert.assertEquals(20200102, priorityResult1.collect().head.dateLoadFromLocalFile)

    // timestamp creazione file più alto
    val measures2 = Environment.getSparkContext.parallelize(List(
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_RML0050_20210109151501_5.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val priorityResult2 = vc.getPriorityMeasures(measures2).cache()
    Assert.assertEquals(1, priorityResult2.count())
    Assert.assertEquals(Constants.FORMAT_DATE_CLOUD_FILENAME.parse("20210109151501"), priorityResult2.collect().head.timestampLocalFile)

    // progressivo più alto
    val measures3 = Environment.getSparkContext.parallelize(List(
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_RML0050_20210109151501_6.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val priorityResult3 = vc.getPriorityMeasures(measures3).cache()
    Assert.assertEquals(1, priorityResult3.count())
    Assert.assertEquals(6, priorityResult3.collect().head.progressiveLocalFile)

    // tracciato Standard vince contro vecchio tracciato
    val measures4 = Environment.getSparkContext.parallelize(List(
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_RML0050_20200109151501_5_R.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = true, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val priorityResult4 = vc.getPriorityMeasures(measures4).cache()
    Assert.assertEquals(1, priorityResult4.count())
    Assert.assertEquals(true, priorityResult4.collect().head.isNewRoute)


    // misura da non considerare:  motivatione = 100
    val measures5 = Environment.getSparkContext.parallelize(List(
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_RML0050_20200109151501_5_R.xml"), motivation = Some(100), d_caricamento = None, isNewRoute = true, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val priorityResult5 = vc.getPriorityMeasures(measures5).cache()
    Assert.assertEquals(0, priorityResult5.count())


    // test presenza flussi di cambio misuratore con entries duplicate a meno di cau_int_mis e cau_int_cor
    val measures6 = Environment.getSparkContext.parallelize(List(
      IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = None,
        converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = None,
        converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = None,
        converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = None, cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = None,
        converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = None, cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val priorityResult6 = vc.getPriorityMeasures(measures6).cache()
    Assert.assertEquals(0, priorityResult6.count())

    // test presenza flussi di cambio misuratore con entries duplicate
    val measures7 = Environment.getSparkContext.parallelize(List(
      IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = None,
        converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = None,
        converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = None,
        converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = None,
        converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val priorityResult7 = vc.getPriorityMeasures(measures7).cache()
    Assert.assertEquals(2, priorityResult7.count())

    // test presenza flussi di cambio misuratore con entries duplicate a meno di prelevata
    val measures8 = Environment.getSparkContext.parallelize(List(
      IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = None,
        converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = None,
        converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = Some(10),
        converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = Some(10),
        converted = None, serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val priorityResult8 = vc.getPriorityMeasures(measures8).cache()
    Assert.assertEquals(0, priorityResult8.count())


    // ATTENZIONE: NON VIENE EFFETTUATO NESSUN TEST SUL CAMPO d_caricamento IN QUANTO IN PRECEDENZA E' PRESENTE UN'OPERAZIONE DI FILTRAGGIO
    // CHE OTTIENE SOLTANTO L'ULTIMA VERSIONE DEI DATI CARICATI IN HIVE (UNA SORTA DI LEAD). NON E' QUINDI POSSIBILE AVERE A QUESTO PUNTO
    // DEL PROGRAMMA LA STESSA MISURA CARICATA PIU' VOLTE IN HIVE
  }

  def testGetMeasures(): Unit = {
    val format = new SimpleDateFormat("yyyy-mm-dd")
    val vc = new ValidationController
    val numberPartitions = 2
    //Environment.resetProperty()
    Environment.setProperty("dataset.numberPartition", numberPartitions.toString)

    // senza cancellazioni e rettifiche cambio misuratore
    for (isNewRoute <- List(true, false)) {

      // measureList contiene la lista di misure a cui in precedenza è già stata applicata la funzione di "lead", quindi rappresentano la
      // versione più aggiornata di tali entries caricate in Hive
      // singolo PDR, singola data delle misure e singola data di caricamento
      val measuresList = List(
        Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_RML0050_20200109151501_5.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = isNewRoute, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_RGL0050_20200109151501_5.xml"), motivation = Some(4), d_caricamento = None, isNewRoute = isNewRoute, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = Some(0),
          converted = Some(0), serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('E'), measure = Some(0),
          converted = Some(0), serialNumberMis = Some("100"), serialNumberConv = Some("200"), cau_int_mis = Some(1), cau_int_cor = Some(1),
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_IGMG_20200109151501_5.xml"), d_caricamento = None, isNewRoute = true, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Pre(service = "IM1PRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('S'), measure = Some(0),
          converted = Some(0), serialNumberMis = Some("300"), serialNumberConv = Some("400"), cau_int_mis = Some(1), cau_int_cor = Some(1),
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_IM10050_20200109151501_5.xml"), d_caricamento = None, isNewRoute = false, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Im1Post(service = "IM1POST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = Some('S'), measure = Some(0),
          converted = Some(0), serialNumberMis = Some("300"), serialNumberConv = Some("400"), cau_int_mis = Some(1), cau_int_cor = Some(1),
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_IM10050_20200109151501_5.xml"), d_caricamento = None, isNewRoute = false, coefCorr = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0105/00489490011_12420101003_201912_RGL0050_20200109151501_5.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = isNewRoute, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0106/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0107/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rmv(pdr = "1", service = "RMV", date = Some(format.parse("2020-01-01")), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0118/00489490011_12420101003_201912_RMV0050_20200109151501_1.xml"), motivation = Some(2), d_caricamento = None, isNewRoute = isNewRoute, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tmv(pdr = "1", service = "TMV", date = Some(format.parse("2020-01-01")), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0109/00489490011_12420101003_201912_TMV0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tal(pdr = "1", service = "TAL", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TAL0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tav(pdr = "1", service = "TAS", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0111/00489490011_12420101003_201912_TAV0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Tas(pdr = "1", service = "TAS", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0112/00489490011_12420101003_201912_TAS0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Rsl(pdr = "1", service = "RSL", date = Some(format.parse("2020-01-01")), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0113/00489490011_12420101003_201912_RSL0050_20200109151501_1.xml"), motivation = Some(2), d_caricamento = None, isNewRoute = isNewRoute, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Swg1(pdr = "1", service = "SWG1", date = Some(format.parse("2020-01-01")), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0114/00489490011_12420101003_201912_SWG1_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
        FUI(pdr = "1", service = "FUI", date = Some(format.parse("2020-01-01")), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0115/00489490011_12420101003_201912_FUI_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        FDD(pdr = "1", service = "FDD", date = Some(format.parse("2020-01-01")), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0116/00489490011_12420101003_201912_FDD_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
        Sw1(pdr = "1", service = "SW1", date = Some(format.parse("2020-01-01")), measure = Some(1),
          converted = Some(10), serialNumberConv = None, serialNumberMis = None,
          local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0117/00489490011_12420101003_201912_SW1_20200109151501_1.xml"), d_caricamento = None, isNewRoute = isNewRoute, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None)

        // altri flussi successivi non aggiunti in quanto questo test valuta solo la correttezza dell'algoritmo

      ).map(_.asInstanceOf[Flow])

      for (i <- measuresList.indices) {
        if (!measuresList(i).service.endsWith("POST")) {
          val measuresRdd = Environment.getSparkContext.parallelize(scala.util.Random.shuffle(measuresList.takeRight(measuresList.length - i)))
          val priorityResult = vc.getMeasures(measuresRdd).cache()
          val expectedFlowWin = if (measuresList(i).service.startsWith("IM1")) (2, "IM1")
          else if (measuresList(i).service.startsWith("IGMG")) (2, "IGMG")
          else (1, measuresList(i).service)

          // nel caso di presenza flussi di cambio misuratore le rettifiche RML e RGL vengono applicate (se possibile) e dopo filtrate
          if (!((priorityResult.collect().head.service.startsWith("IM1") || priorityResult.collect().head.service.startsWith("IGMG"))
            && Set("RML", "RGL").contains(expectedFlowWin._2))) {
            Assert.assertEquals(expectedFlowWin._1, priorityResult.count())
            Assert.assertTrue(priorityResult.collect().forall(f => f.service.startsWith(expectedFlowWin._2)))
          } else {
            Assert.assertEquals(2, priorityResult.count())
            Assert.assertTrue(priorityResult.collect().forall(f => f.service.startsWith("IM1") || f.service.startsWith("IGMG")))
          }
        }
      }
    }

  }

}
