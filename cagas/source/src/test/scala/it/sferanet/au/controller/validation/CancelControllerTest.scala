package it.sferanet.au.controller.validation

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.model.Flow
import it.sferanet.au.model.autolettura.{Tal, Tas, Tav}
import it.sferanet.au.model.periodico.{Tgl, Tml}
import it.sferanet.au.model.prestazionale._
import it.sferanet.au.model.rettifica._
import it.sferanet.au.utilities.Environment
import org.junit.Assert

import java.text.SimpleDateFormat

/**
 * Questa classe la corretta esecuzione del controller degli annullamenti sulle misure.
 * Vengono testati una serie di possibili scenari discussi in fase di sviluppo. Il motore degli annullamenti
 * lavora creando 3 diversi gruppi di misure e applicando le opportune operazioni ad essi. Questa classe
 * andrà a testare tutti e 3 i gruppi di misure con l'aggiunta di altre combinazioni utili possibili.
 */
class CancelControllerTest extends EnvironmentSparkTest {

  /**
   * casistiche gruppo 1 (RGL,RML,TML,TGL, rettifiche presenti con motivazione 1,2,3,4,5 ma vengono applicate solo gli annullamenti le motivazioni 3):
   *
   * - COMPORTAMENTO OTTIMALE:
   *
   * ·       RGL(3) --> RML,RGL,TGL,TML
   *
   * ·       RML(3) --> RML,TML
   *
   * ·       Gli annullamenti devono partire dal più recente
   *
   *
   * - esempi da chiarire:
   *
   * 1.       TGL, TML, RML(3) --> TGL OK
   *
   * 2.       TGL, TML, RGL(3) --> TGL OK
   *
   * 3.       TML, TGL, RML(3) --> TGL (deve annullare il TML) OK
   *
   * 4.       TML, TGL, RGL(3) --> TML OK
   *
   * 5.       TML, TGL, RGL(1), RML(3) --> RGL (deve annullare il TML) OK
   *
   * 6.       TML, TGL, RGL(3), RML(3) --> NON PASSA NULLA OK
   *
   * 7.       TML, TGL, RML(1), RGL(3) --> TGL OK
   *
   * 8.       TGL, TML, RGL(1), RML(3) --> RGL OK
   *
   * 9.       TGL, TML, RGL(3), RML(3) à NON PASSA NULLA (scenario analogo al 6)
   *
   * 10.      TGL, TML, RML(1), RGL(3) --> TML,TGL (ovvero passano al modulo delle priorità che farà emergere la TGL)
   *
   * 11.      TGL, RGL(3), TML --> TML
   *
   * 12.      TML, RML(3), TGL --> TGL
   *
   */

  /**
   * CR Gabrini Federico, 22/08/2022 15:55
   * c.	rettifiche di annullamento: attualmente nella procedura le rettifiche di tipo 3 (annullamento) agiscono solamente sul flusso immediatamente precedente, a richiesta è quella di annullare tutte le misure precedenti dello stesso tipo secondo quanto segue
   * i.     flussi RGL con mot_rett_lett=3 annullano tutti i *GL precedentemente trasmessi
   * ii.     flussi RML con mot_rett_lett=3 annullano tutti i flussi *ML precedentemente trasmessi tranne *GL
   */

  def testCancelGroup1(): Unit = {
    val format = new SimpleDateFormat("yyyy-mm-dd")

    ///////// CASISTICHE GRUPPO 1 /////////

    val measures1 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures1, 1).foreach(println)
    val output1 = CancelController.cancelMeasures(measures1, 1).cache()
    Assert.assertEquals(1, output1.count())
    Assert.assertEquals("TGL", output1.take(1).head.service)

    val measures2 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures2, 1).foreach(println)
    val output2 = CancelController.cancelMeasures(measures2, 1).cache()
    Assert.assertEquals(1, output2.count())
    Assert.assertEquals("TML", output2.take(1).head.service)

    val measures3 = Environment.getSparkContext.parallelize(List(
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures3, 1).foreach(println)
    val output3 = CancelController.cancelMeasures(measures3, 1).cache()
    Assert.assertEquals(1, output3.count())
    Assert.assertEquals("TGL", output3.take(1).head.service)

    val measures4 = Environment.getSparkContext.parallelize(List(
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures4, 1).foreach(println)
    val output4 = CancelController.cancelMeasures(measures4, 1).cache()
    Assert.assertEquals(1, output4.count())
    Assert.assertEquals("TML", output4.take(1).head.service)

    val measures5 = Environment.getSparkContext.parallelize(List(
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures5, 1).foreach(println)
    val output5 = CancelController.cancelMeasures(measures5, 1).cache()
    Assert.assertEquals(2, output5.count())
    Assert.assertEquals(1, output5.take(2).count(f => f.service == "RGL"))
    Assert.assertEquals(1, output5.take(2).count(f => f.service == "TGL"))

    val measures6 = Environment.getSparkContext.parallelize(List(
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures6, 1).foreach(println)
    val output6 = CancelController.cancelMeasures(measures6, 1).cache()
    Assert.assertEquals(0, output6.count())

    val measures7 = Environment.getSparkContext.parallelize(List(
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures7, 1).foreach(println)
    val output7 = CancelController.cancelMeasures(measures7, 1).cache()

    Assert.assertEquals(2, output7.count())
    Assert.assertEquals(1, output7.take(2).count(f => f.service == "TML"))
    Assert.assertEquals(1, output7.take(2).count(f => f.service == "RML"))

    val measures8 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures8, 1).foreach(println)
    val output8 = CancelController.cancelMeasures(measures8, 1).cache()
    Assert.assertEquals(2, output8.count())
    Assert.assertEquals(1, output8.take(2).count(f => f.service == "RGL"))
    Assert.assertEquals(1, output8.take(2).count(f => f.service == "TGL"))

    val measures9 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures9, 1).foreach(println)
    val output9 = CancelController.cancelMeasures(measures9, 1).cache()
    Assert.assertEquals(0, output9.count())

    val measures10 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures10, 1).foreach(println)
    val output10 = CancelController.cancelMeasures(measures10, 1).cache()
    Assert.assertEquals(2, output10.count())
    Assert.assertEquals(1, output10.take(2).count(f => f.service == "TML"))
    Assert.assertEquals(1, output10.take(2).count(f => f.service == "RML"))

    val measures11 = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures11, 1).foreach(println)
    val output11 = CancelController.cancelMeasures(measures11, 1).cache()
    Assert.assertEquals(1, output11.count())
    Assert.assertEquals(1, output11.take(1).count(f => f.service == "TML"))

    val measures12 = Environment.getSparkContext.parallelize(List(
      Tml(pdr = "1", service = "TML", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('E'), isValid = Some("SI"), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures12, 1).foreach(println)
    val output12 = CancelController.cancelMeasures(measures12, 1).cache()
    Assert.assertEquals(1, output12.count())
    Assert.assertEquals(1, output12.take(1).count(f => f.service == "TGL"))

  }


  /**
   * casistiche gruppo 3 (TAL/TAS/TAV/RGL/RML, rettifiche presenti solo con motivazione 6 ossia annullamenti autoletture):
   *
   * 13.      TAL, TAS, RML(6) --> TAL OK
   *
   * 14.      TAL, TAV, RGL(6) --> TAL OK
   *
   * 15.      TAL, TAS, RML(1), RML(6) --> RML (deve annullare il TAS) OK
   *
   * 16.      TAL, TAS, RML(1), RGL(6) --> RML (deve annullare il TAS) OK
   *
   * 17.      TAL, TAS, RGL(1), RML(6) --> RGL (deve annullare il TAS) OK
   *
   * 18.      TAL, TAS, RGL(1), RGL(6) --> RGL (deve annullare il TAS) OK
   *
   */

  def testCancelGroup3(): Unit = {
    val format = new SimpleDateFormat("yyyy-mm-dd")

    ///////// CASISTICHE GRUPPO 3 /////////

    val measures13 = Environment.getSparkContext.parallelize(List(
      Tal(pdr = "1", service = "TAL", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tas(pdr = "1", service = "TAS", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(6), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures13, 3).foreach(println)
    val output13 = CancelController.cancelMeasures(measures13, 3).cache()
    Assert.assertEquals(1, output13.count())
    Assert.assertEquals(1, output13.take(1).count(f => f.service == "TAL"))

    val measures14 = Environment.getSparkContext.parallelize(List(
      Tal(pdr = "1", service = "TAL", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tav(pdr = "1", service = "TAV", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(6), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures14, 3).foreach(println)
    val output14 = CancelController.cancelMeasures(measures14, 3).cache()
    Assert.assertEquals(1, output14.count())
    Assert.assertEquals(1, output14.take(1).count(f => f.service == "TAL"))

    val measures15 = Environment.getSparkContext.parallelize(List(
      Tal(pdr = "1", service = "TAL", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tas(pdr = "1", service = "TAS", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(6), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures15, 3).foreach(println)
    val output15 = CancelController.cancelMeasures(measures15, 3).cache()
    Assert.assertEquals(2, output15.count())
    Assert.assertEquals(1, output15.take(2).count(f => f.service == "TAL"))
    Assert.assertEquals(1, output15.take(2).count(f => f.service == "RML"))

    val measures16 = Environment.getSparkContext.parallelize(List(
      Tal(pdr = "1", service = "TAL", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tas(pdr = "1", service = "TAS", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(6), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures16, 3).foreach(println)
    val output16 = CancelController.cancelMeasures(measures16, 3).cache()
    Assert.assertEquals(2, output16.count())
    Assert.assertEquals(1, output16.take(2).count(f => f.service == "TAL"))
    Assert.assertEquals(1, output16.take(2).count(f => f.service == "RML"))

    val measures17 = Environment.getSparkContext.parallelize(List(
      Tal(pdr = "1", service = "TAL", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tas(pdr = "1", service = "TAS", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(6), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures17, 3).foreach(println)
    val output17 = CancelController.cancelMeasures(measures17, 3).cache()
    Assert.assertEquals(2, output17.count())
    Assert.assertEquals(1, output17.take(2).count(f => f.service == "TAL"))
    Assert.assertEquals(1, output17.take(2).count(f => f.service == "RGL"))

    val measures18 = Environment.getSparkContext.parallelize(List(
      Tal(pdr = "1", service = "TAL", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tas(pdr = "1", service = "TAS", date = Some(format.parse("2020-01-01")), outcome = Some('V'), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(1), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(1.5),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_RML0050_20200109151501_1.xml"), motivation = Some(6), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    //    CancelController.cancelMeasures(measures18, 3).foreach(println)
    val output18 = CancelController.cancelMeasures(measures18, 3).cache()
    Assert.assertEquals(2, output18.count())
    Assert.assertEquals(1, output18.take(2).count(f => f.service == "TAL"))
    Assert.assertEquals(1, output18.take(2).count(f => f.service == "RGL"))
  }

  /**
   * CR Gabrini Federico, 22/08/2022 15:55
   * c.	rettifiche di annullamento: attualmente nella procedura le rettifiche di tipo 3 (annullamento) agiscono solamente sul flusso immediatamente precedente, a richiesta è quella di annullare tutte le misure precedenti dello stesso tipo secondo quanto segue
   * iii.     flussi RMV con mot_rett_lett=3 annullano tutti i flussi *MV precedentemente trasmessi
   * iv.     flussi RSL con mot_rett_lett=3 annullano tutti i flussi SW1 e RSL precedentemente trasmessi
   * v.     lo stesso vale per D01R, D02R, R01R, A40R, S40R, R40R, A01R, A02R, S02R, V01R, M01R, V02R, SM1R, SM2R, AD2R, AD3R, AD4R, AD5R
   */

  def testCancelGroup4(): Unit = {
    val format = new SimpleDateFormat("yyyy-mm-dd")

    ///////// CASISTICHE GRUPPO 4 /////////
    //    (classOf[x], List(classOf[y]) => classe x può rettificare SOLO misure della classe y
    //    (classOf[Rmv], List(classOf[Tmv])),
    //    (classOf[Rsl], List(classOf[Sw1], classOf[Swg1], classOf[FUI], classOf[FDD])),
    //    (classOf[A01R], List(classOf[A01])),
    //    (classOf[A40R], List(classOf[A40])),
    //    (classOf[SM1R], List(classOf[Sm1])),
    //    (classOf[AD2R], List(classOf[AD2])),
    //    (classOf[AD3R], List(classOf[AD3])),
    //    (classOf[A02R], List(classOf[A02])),
    //    (classOf[S02R], List(classOf[S02])),
    //    (classOf[S40R], List(classOf[S40])),
    //    (classOf[R01r], List(classOf[R01])),
    //    (classOf[R40r], List(classOf[R40])),
    //    (classOf[M01r], List(classOf[M01])),
    //    (classOf[V01R], List(classOf[V01])),
    //    (classOf[V02R], List(classOf[V02])),
    //    (classOf[AD4R], List(classOf[AD4])),
    //    (classOf[AD5R], List(classOf[AD5]))


    // una rettifica per ogni flusso di misura
    val measures1 = Environment.getSparkContext.parallelize(List(
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
      AD4(pdr = "1", service = "AD4", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      AD5(pdr = "1", service = "AD5", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),


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
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      AD4R(pdr = "1", service = "AD4R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0128/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      AD5R(pdr = "1", service = "AD5R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0128/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)

    )).map(_.asInstanceOf[Flow])

    val output1 = CancelController.cancelMeasures(measures1, 4).cache()
    Assert.assertEquals(0, output1.count())

    // numero di flussi di misura > numero di flussi di rettifica
    val measures2 = Environment.getSparkContext.parallelize(List(
      Tmv(pdr = "1", service = "TMV", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Sw1(pdr = "1", service = "SW1", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Swg1(pdr = "1", service = "SWG1", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V01(pdr = "1", service = "V01", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0117/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V02(pdr = "1", service = "V02", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0118/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      AD5(pdr = "1", service = "AD5", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),

      Rsl(pdr = "1", service = "RSL", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0122/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V01R(pdr = "1", service = "V01R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0204/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V02R(pdr = "1", service = "V02R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)

    )).map(_.asInstanceOf[Flow])

    val output2 = CancelController.cancelMeasures(measures2, 4).cache()
    Assert.assertEquals(2, output2.count())
    Assert.assertEquals(1, output2.take(2).count(f => f.service == "TMV"))
    Assert.assertEquals(1, output2.take(2).count(f => f.service == "AD5"))

    // numero di flussi di rettifica > numero di flussi di misura
    val measures3 = Environment.getSparkContext.parallelize(List(
      Swg1(pdr = "1", service = "SWG1", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V01(pdr = "1", service = "V01", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0117/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V02(pdr = "1", service = "V02", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0118/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      AD4(pdr = "1", service = "AD5", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),

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
      V01R(pdr = "1", service = "V01R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0204/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V02R(pdr = "1", service = "V02R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      AD4R(pdr = "1", service = "AD4R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0128/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val output3 = CancelController.cancelMeasures(measures3, 4).cache()
    Assert.assertEquals(0, output3.count())

  }

  def testVariousCombinations(): Unit = {
    val format = new SimpleDateFormat("yyyy-mm-dd")

    // solo rettifiche
    val measures1 = Environment.getSparkContext.parallelize(List(
      Rmv(pdr = "1", service = "RMV", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0119/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rsl(pdr = "1", service = "RSL", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0120/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val output1 = CancelController.cancelMeasures(measures1, 4).cache()
    Assert.assertEquals(0, output1.count())

    // rettifica trasmessa prima del flusso di misura
    val measures2 = Environment.getSparkContext.parallelize(List(
      Rmv(pdr = "1", service = "RMV", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tmv(pdr = "1", service = "TMV", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0102/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val output2 = CancelController.cancelMeasures(measures2, 4).cache()
    Assert.assertEquals(1, output2.count())
    Assert.assertEquals(1, output2.take(1).count(f => f.service == "TMV"))

  }

  def testMot3Cancel(): Unit = {
    val format = new SimpleDateFormat("yyyy-mm-dd")

    val measures = Environment.getSparkContext.parallelize(List(
      Swg1(pdr = "1", service = "SWG1", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Sw1(pdr = "1", service = "SW1", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0103/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V01(pdr = "1", service = "V01", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0117/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V02(pdr = "1", service = "V02", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0118/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V02R(pdr = "1", service = "V02R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A01(pdr = "1", service = "A01", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0117/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, outcome = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A01(pdr = "1", service = "A01", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0117/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, outcome = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A01R(pdr = "1", service = "A01R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0117/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A02(pdr = "1", service = "A02", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0117/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), d_caricamento = None, isNewRoute = false, readType = Some('E'), collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
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
      V01R(pdr = "1", service = "V01R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0204/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      V02R(pdr = "1", service = "V02R", date = Some(format.parse("2020-01-01")), measure = Some(1),
        converted = Some(10), serialNumberConv = None, serialNumberMis = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0205/00489490011_12420101003_201912_TML0050_20200109151501_1.xml"), motivation = Some(3), d_caricamento = None, isNewRoute = false, raccolta = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)

    )).map(_.asInstanceOf[Flow])

    val output = CancelController.cancelMeasures(measures, 4).cache()

    Assert.assertEquals(1, output.count())
    Assert.assertEquals(1, output.take(1).count(f => f.service == "A02"))
  }
}
