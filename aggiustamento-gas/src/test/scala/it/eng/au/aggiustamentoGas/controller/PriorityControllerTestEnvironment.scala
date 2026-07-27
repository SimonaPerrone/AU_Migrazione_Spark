package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.model.measure._
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Igmg, Im1, Im1Igmg}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class PriorityControllerTestEnvironment extends EnvironmentSparkTest {
  def testGetPriorityMeasures(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val measures = Environment.getSpark.sparkContext.parallelize(List(
      Rml(service = "RML", pdr = "1", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(6), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rgl(service = "RGL", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(7), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rgl(service = "RGL", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(4), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),

      Rml(service = "RML", pdr = "2", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2021/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(4), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rml(service = "RML", pdr = "2", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(4), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),

      Rml(service = "RML", pdr = "3", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(1), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rml(service = "RML", pdr = "3", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200209151501_1.xml"),
        motivation = Some(1), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),

      Rml(service = "RML", pdr = "4", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"),
        motivation = Some(1), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rml(service = "RML", pdr = "4", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(1), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),

      Rml(service = "RML", pdr = "5", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"),
        motivation = Some(3), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rml(service = "RML", pdr = "5", readType = Some(0), date = Some(formatter.parseDateTime("27-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"),
        motivation = Some(3), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rgl(service = "RGL", pdr = "5", date = Some(formatter.parseDateTime("28-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(3), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),

      Rmv(service = "RMV", pdr = "6", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_RMV0050_20200109151501_1.xml"),
        motivation = Some(1), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rmv(service = "RMV", pdr = "6", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_RMV0050_20200109151501_2.xml"),
        motivation = Some(4), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rmv(service = "RMV", pdr = "6", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_RMV0050_20200109151501_3.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Tmv(service = "TMV", pdr = "6", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TMV0050_20200109151501_4.xml"),
        dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),

      Tgl(service = "TGL", pdr = "7", readType = Some('S'), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TMV0050_20200109151501_4.xml"),
        dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      D01(service = "D01", pdr = "7", readType = Some('S'), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TMV0050_20200109151501_4.xml"),
        dataCaricamento = Some(formatter.parseDateTime("26-02-2021")))

    )).asInstanceOf[RDD[Flow]]

    val result = new PriorityController().getPriorityMeasures(measures).cache()

    result.collect().foreach(println)

    Assert.assertEquals(6, result.count())

    Assert.assertEquals(1, result.filter(_.pdr == "1").count)
    Assert.assertEquals(Some(3), result.filter(_.pdr == "1").first().measure)

    Assert.assertEquals(1, result.filter(_.pdr == "2").count)
    Assert.assertEquals(Some(1), result.filter(_.pdr == "2").first().measure)

    Assert.assertEquals(1, result.filter(_.pdr == "3").count)
    Assert.assertEquals(Some(2), result.filter(_.pdr == "3").first().measure)

    Assert.assertEquals(1, result.filter(_.pdr == "4").count)
    Assert.assertEquals(Some(1), result.filter(_.pdr == "4").first().measure)

    Assert.assertEquals(0, result.filter(_.pdr == "5").count)

    Assert.assertEquals(1, result.filter(_.pdr == "6").count)
    Assert.assertEquals(Some(2), result.filter(_.pdr == "6").first().measure)
    Assert.assertEquals(0, result.filter(flow => flow.pdr == "6" && flow.isInstanceOf[Tmv]).count)

    Assert.assertEquals(1, result.filter(_.pdr == "7").count)
    Assert.assertEquals(0, result.filter(flow => flow.pdr == "7" && flow.isInstanceOf[D01]).count)
  }

  def testAssociateIm1IgmgWithSameDayFlow(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")
    //using same local file but changing "progressivo" to grant flow ordering
    val localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_")
    val ext = ".xml"

    val measures: Iterable[Flow] = List(
      Rgl(service = "RGL", pdr = "2", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = localFile.map(lf => s"${lf}_1$ext"),
        motivation = Some(7), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Im1(service = "IM1", pdr = "2", date = Some(formatter.parseDateTime("26-02-2021")), readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = None,
        cau_int_cor = None, localFile = localFile.map(lf => s"${lf}_2$ext"), pivaDistr = None, pivaUtente = None, dataCaricamento = None, pre = null, post = null),
      Rgl(service = "RGL", pdr = "2", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = localFile.map(lf => s"${lf}_3$ext"), motivation = Some(4), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Igmg(service = "IM1", pdr = "2", date = Some(formatter.parseDateTime("26-02-2021")), readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = None,
        cau_int_cor = None, localFile = localFile.map(lf => s"${lf}_4$ext"), pivaDistr = None, pivaUtente = None, dataCaricamento = None, pre = null, post = null),
      Igmg(service = "IM1", pdr = "2", date = Some(formatter.parseDateTime("26-02-2021")), readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = None,
        cau_int_cor = None, localFile = localFile.map(lf => s"${lf}_4$ext"), pivaDistr = None, pivaUtente = None, dataCaricamento = None, pre = null, post = null)
    )
    val result = PriorityController.associateIm1IgmgWithSameDayFlow(measures)
    result.foreach(println)
    Assert.assertEquals(result.size, measures.size)

    Assert.assertTrue(result.find(f => f.pdr.equals("2") && f.isInstanceOf[Im1]).get.asInstanceOf[Im1].sameDayFlow.isDefined)
    Assert.assertEquals(
      result.filter(_.pdr.equals("2")).toList.sorted(Flow.orderingSameDayFlows).reverse.filter(!_.isInstanceOf[Im1Igmg]).head,
      result.find(f => f.pdr.equals("2") && f.isInstanceOf[Im1]).get.asInstanceOf[Im1].sameDayFlow.get
    )

    result.filter(f => f.pdr.equals("2") && f.isInstanceOf[Igmg]).foreach(f => Assert.assertTrue(f.asInstanceOf[Igmg].sameDayFlow.isDefined))
    Assert.assertEquals(
      result.filter(_.pdr.equals("2")).toList.sorted(Flow.orderingSameDayFlows).reverse.filter(!_.isInstanceOf[Im1Igmg]).head,
      result.find(f => f.pdr.equals("2") && f.isInstanceOf[Igmg]).get.asInstanceOf[Igmg].sameDayFlow.get
    )
  }

  def testActivationFlow(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val measures: Iterable[Flow] = List(
      A40(service = "A40", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")), readType = None, measure = Some(1.0),
        converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), outcome = None),
      Rgl(service = "RGL", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(7), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rgl(service = "RGL", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(4), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),

      A40(service = "A40", pdr = "2", date = Some(formatter.parseDateTime("26-02-2021")), readType = None, measure = Some(1.0),
        converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), outcome = None),
      A40(service = "A40", pdr = "2", date = Some(formatter.parseDateTime("26-02-2021")), readType = None, measure = Some(1.0),
        converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), outcome = None),

      Rml(service = "RML", pdr = "3", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(1), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),

      A40(service = "A40", pdr = "4", date = Some(formatter.parseDateTime("26-02-2021")), readType = None, measure = Some(1.0),
        converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), outcome = None),

      Rml(service = "RML", pdr = "5", readType = Some(0), date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"),
        motivation = Some(3), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      A40(service = "A40", pdr = "5", date = Some(formatter.parseDateTime("26-02-2021")), readType = None, measure = Some(1.0),
        converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), outcome = None),
      Rgl(service = "RGL", pdr = "5", date = Some(formatter.parseDateTime("28-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(3), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")))
    )

    val result = PriorityController.associateActivationFlow(measures.filter(_.pdr.equals("1")))
    println(result.map(r => (r.pdr, r.service, r.activationFlow)).mkString("\n"))

    Assert.assertTrue(result.exists(r => r.service.equals("A40") && r.activationFlow.isEmpty))
    Assert.assertFalse(result.exists(r => r.service.equals("RGL") && r.activationFlow.isEmpty))

    val result2 = PriorityController.associateActivationFlow(measures.filter(_.pdr.equals("2")))
    println(result2.map(r => (r.pdr, r.service, r.activationFlow)).mkString("\n"))

    Assert.assertFalse(result2.exists(r => r.service.equals("A40") && r.activationFlow.nonEmpty))

    val result3 = PriorityController.associateActivationFlow(measures.filter(_.pdr.equals("3")))
    println(result3.map(r => (r.pdr, r.service, r.activationFlow)).mkString("\n"))

    Assert.assertTrue(result3.exists(r => r.service.equals("RML") && r.activationFlow.isEmpty))

    val result4 = PriorityController.associateActivationFlow(measures.filter(_.pdr.equals("4")))
    println(result4.map(r => (r.pdr, r.service, r.activationFlow)).mkString("\n"))

    Assert.assertTrue(result4.exists(r => r.service.equals("A40") && r.activationFlow.isEmpty))

    val result5 = PriorityController.associateActivationFlow(measures.filter(_.pdr.equals("5")))
    println(result5.map(r => (r.pdr, r.service, r.activationFlow)).mkString("\n"))

    Assert.assertTrue(result5.exists(r => r.service.equals("A40") && r.activationFlow.isEmpty))
    Assert.assertFalse(result5.exists(r => r.service.equals("RML") && r.activationFlow.isEmpty))
  }
}
