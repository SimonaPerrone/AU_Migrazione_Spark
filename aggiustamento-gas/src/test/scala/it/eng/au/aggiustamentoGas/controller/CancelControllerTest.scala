package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.model.measure._
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class CancelControllerTest extends EnvironmentSparkTest {
  def testCancelTreatmentMeasures(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val measures = Environment.getSpark.sparkContext.parallelize(List(
      Tgl(service = "TGL", pdr = "1", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        isValid = None, dataCaricamento = None),
      Rgl(service = "RGL", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"),
        motivation = Some(3), dataCaricamento = None),
      Rgl(service = "RGL", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_3.xml"),
        motivation = Some(4), dataCaricamento = None),

      Tml(service = "TML", pdr = "1", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_4.xml"),
        isValid = None, dataCaricamento = None, coefCorr = None, freqLet = None),
      Rml(service = "RML", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_5.xml"),
        motivation = Some(3), dataCaricamento = None, freqLet = None, readType = None, tipoRettifica = None),
      Rml(service = "RML", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_6.xml"),
        motivation = Some(4), dataCaricamento = None, freqLet = None, readType = None, tipoRettifica = None),


      Tgl(service = "TGL", pdr = "2", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        isValid = None, dataCaricamento = Some(formatter.parseDateTime("24-02-2021"))),
      Rgl(service = "RGL", pdr = "2", date = None,
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"),
        motivation = Some(1), dataCaricamento = Some(formatter.parseDateTime("25-02-2021"))),
      Rgl(service = "RGL", pdr = "2", date = None,
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_3.xml"),
        motivation = Some(4), dataCaricamento = None),


      Tgl(service = "TGL", pdr = "3", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        isValid = None, dataCaricamento = None),
      Rgl(service = "RGL", pdr = "3", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"),
        motivation = Some(1), dataCaricamento = None),
      Rgl(service = "RGL", pdr = "3", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_3.xml"),
        motivation = Some(3), dataCaricamento = None),

      Rgl(service = "RGL", pdr = "4", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(3), dataCaricamento = None),
      Rgl(service = "RGL", pdr = "4", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"),
        motivation = Some(4), dataCaricamento = None),


      Tml(service = "TML", pdr = "5", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        isValid = None, dataCaricamento = None, coefCorr = None, freqLet = None),
      Tgl(service = "TGL", pdr = "5", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"),
        isValid = None, dataCaricamento = None),
      Rgl(service = "RGL", pdr = "5", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_3.xml"),
        motivation = Some(3), dataCaricamento = None),
      Rml(service = "RML", pdr = "5", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_4.xml"),
        motivation = Some(3), dataCaricamento = None, freqLet = None, readType = None, tipoRettifica = None),

      Tgl(service = "TGL", pdr = "6", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        isValid = None, dataCaricamento = None),
      Tml(service = "TML", pdr = "6", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"),
        isValid = None, dataCaricamento = None, coefCorr = None, freqLet = None),
      Rgl(service = "RGL", pdr = "6", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_3.xml"),
        motivation = Some(3), dataCaricamento = None)
    )).asInstanceOf[RDD[Flow]]

    val result = new CancelController().cancelTreatmentMeasures(measures).cache()

    result.collect().foreach(println)

    //    Assert.assertEquals(8, result.count())

    Assert.assertEquals(2, result.filter(f => f.pdr == "1").count)
    Assert.assertEquals(Some(3), result.filter(f => f.pdr == "1" && f.isInstanceOf[Rgl]).first().measure)
    Assert.assertEquals(Some(3), result.filter(f => f.pdr == "1" && f.isInstanceOf[Rml]).first().measure)

    Assert.assertEquals(3, result.filter(f => f.pdr == "2").count)

    Assert.assertEquals(0, result.filter(f => f.pdr == "3").count)
    //    Assert.assertEquals(Some(1), result.filter(f => f.pdr == "3"  && f.isInstanceOf[Tgl]).first().measure)

    Assert.assertEquals(1, result.filter(f => f.pdr == "4").count)

    Assert.assertEquals(0, result.filter(f => f.pdr == "5").count)

    Assert.assertEquals(1, result.filter(f => f.pdr == "6").count)
    Assert.assertEquals(Some(2), result.filter(f => f.pdr == "6" && f.isInstanceOf[Tml]).first().measure)
  }

  def testCancelMot6(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val measures = Environment.getSpark.sparkContext.parallelize(List(
      Tal(service = "TAL", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), outcome = None),
      Tav(service = "TAV", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200110151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), outcome = None),
      Rml(service = "RML", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = None, tipoRettifica = None),
      Tal(service = "TAL", pdr = "2", date = Some(formatter.parseDateTime("21-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("piva_2"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), outcome = None),
      Rgl(service = "RGL", pdr = "2", date = Some(formatter.parseDateTime("21-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("piva_2"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")))

    )).asInstanceOf[RDD[Flow]]

    Assert.assertEquals(2, measures.filter(_.pdr.equalsIgnoreCase("2")).count())

    val result = new CancelController().cancelMot6(measures).cache
    result.collect.foreach(println)

    Assert.assertEquals(0, result.count())
  }

  def testCancelOtherMeasures(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val measures = Environment.getSpark.sparkContext.parallelize(List(
      Tmv(service = "TMV", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), readType = None),
      Tmv(service = "TMV", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200110151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), readType = None),
      Rmv(service = "RMV", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), motivation = Some(3)),

      Fdd(service = "FDD", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), readType = None),
      Fui(service = "FUI", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200110151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), readType = None),
      Rsl(service = "RSL", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), motivation = Some(3)),

      Tmv(service = "TMV", pdr = "2", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), readType = None),
      Rmv(service = "RMV", pdr = "2", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), motivation = Some(1)),
      Rmv(service = "RMV", pdr = "2", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_3.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), motivation = Some(3)),

      Tmv(service = "TMV", pdr = "3", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), readType = None),
      Rmv(service = "RMV", pdr = "3", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), motivation = Some(1)),
      Rmv(service = "RMV", pdr = "3", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_3.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), motivation = Some(3)),
      Tmv(service = "TMV", pdr = "3", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(4), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_4.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), readType = None),


      Tmv(service = "TMV", pdr = "4", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), readType = None),
      Rmv(service = "RMV", pdr = "4", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), motivation = Some(3)),
      Rmv(service = "RMV", pdr = "4", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_3.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), motivation = Some(3)),
      Tmv(service = "TMV", pdr = "4", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(4), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_4.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), readType = None),

      Ad5(service = "Ad5", pdr = "7", readType = Some(0), date = Some(formatter.parseDateTime("27-10-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_01396650218/DISTRIBUTORE/TMG_01396650218_01745520211/2021/1103/01396650218_01745520211_202110_AD5_20211103175214_1_M.zip"),
        dataCaricamento = Some(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS").parseDateTime("2021-11-04T02:04:43.716000"))),
      Ad5r(service = "Ad5r", pdr = "7", date = Some(formatter.parseDateTime("27-10-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_01396650218/DISTRIBUTORE/TMG_01396650218_01745520211/2021/1119/01396650218_01745520211_202110_AD5R_20211119093415_1_R.zip"),
        motivation = Some(3), dataCaricamento = Some(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS").parseDateTime("2021-11-20T03:18:27.131000")))
    )).asInstanceOf[RDD[Flow]]

    val result = new CancelController().cancelOtherMeasures(measures).cache
    result.collect.foreach(println)

//    Assert.assertEquals(3, result.count())
    Assert.assertEquals(0, result.filter(f => f.pdr == "1" && f.isInstanceOf[Rmv]).count)
    Assert.assertEquals(0, result.filter(f => f.pdr == "1" && f.isInstanceOf[Rsl]).count)

    Assert.assertEquals(0, result.filter(f => f.pdr == "2" && f.isInstanceOf[Rmv]).count)

    Assert.assertEquals(0, result.filter(f => f.pdr == "3" && f.isInstanceOf[Rmv]).count)
    Assert.assertEquals(Some(4), result.filter(f => f.pdr == "3" && f.isInstanceOf[Tmv]).first().measure)

    Assert.assertEquals(0, result.filter(f => f.pdr == "4" && f.isInstanceOf[Rmv]).count)
    Assert.assertEquals(Some(4), result.filter(f => f.pdr == "4" && f.isInstanceOf[Tmv]).first().measure)

    Assert.assertEquals(0, result.filter(f => f.pdr == "7").count)
  }
}
