package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO
import it.eng.au.aggiustamentoGas.model.measure._
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Igmg, IgmgPost, IgmgPre, Im1Igmg}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.junit.Assert

class Im1IgmgCorrectionControllerTestEnvironment extends EnvironmentSparkTest {
  private val igmgCorrectionController = new Im1IgmgCorrectionController()

  def testIgmgCorrectionController(): Unit = {

    //SCENARIO 1: BOTH matr_mis and matr_conv of IgmgPre and IgmgPost are subject to adjustment
    val adjustedRdd = igmgCorrectionController.getAdjustedIgmg(testRDDScenario1).cache()

    Assert.assertEquals(1, adjustedRdd.count())
    Assert.assertEquals(1.0, adjustedRdd.first().asInstanceOf[Im1Igmg].post.measure.get, 0.0)
    Assert.assertEquals(2.5, adjustedRdd.first().asInstanceOf[Im1Igmg].post.converted.get, 0.0)
    Assert.assertEquals(1.0, adjustedRdd.first().asInstanceOf[Im1Igmg].pre.measure.get, 0.0)
    Assert.assertEquals(2.5, adjustedRdd.first().asInstanceOf[Im1Igmg].pre.converted.get, 0.0)
    Assert.assertTrue(adjustedRdd.first().asInstanceOf[Im1Igmg].post.isCorrected)
    Assert.assertTrue(adjustedRdd.first().asInstanceOf[Im1Igmg].pre.isCorrected)
    testCorrection(adjustedRdd)

    //SCENARIO 1.1: BOTH matr_mis and matr_conv of IgmgPre and IgmgPost are subject to adjustment and there is more than one flow
    //CR: RGL doesn't adjustment the measure anymore
    val adjustedRdd1dot1 = igmgCorrectionController.getAdjustedIgmg(testRDD2Scenario1dot1).cache()
    adjustedRdd1dot1.collect.foreach(println)
    Assert.assertEquals(3, adjustedRdd1dot1.count())
    Assert.assertEquals(33.0, adjustedRdd1dot1.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].post.measure.get, 0.0)
    Assert.assertEquals(22.5, adjustedRdd1dot1.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].post.converted.get, 0.0)
    Assert.assertEquals(33.0, adjustedRdd1dot1.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].pre.measure.get, 0.0)
    Assert.assertEquals(22.5, adjustedRdd1dot1.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].pre.converted.get, 0.0)
//    Assert.assertEquals(0.0, adjustedRdd1dot1.filter(_.pdr.equalsIgnoreCase("3")).first().asInstanceOf[Im1Igmg].post.measure.get, 0.0)
//    Assert.assertEquals(0.0, adjustedRdd1dot1.filter(_.pdr.equalsIgnoreCase("3")).first().asInstanceOf[Im1Igmg].post.converted.get, 0.0)
//    Assert.assertEquals(0.0, adjustedRdd1dot1.filter(_.pdr.equalsIgnoreCase("3")).first().asInstanceOf[Im1Igmg].pre.measure.get, 0.0)
//    Assert.assertEquals(0.0, adjustedRdd1dot1.filter(_.pdr.equalsIgnoreCase("3")).first().asInstanceOf[Im1Igmg].pre.converted.get, 0.0)
//    Assert.assertEquals(1.0, adjustedRdd1dot1.filter(_.pdr.equalsIgnoreCase("2")).first().asInstanceOf[Rgl].measure.get, 0.0)
//    Assert.assertEquals(2.5, adjustedRdd1dot1.filter(_.pdr.equalsIgnoreCase("2")).first().asInstanceOf[Rgl].converted.get, 0.0)
//    Assert.assertTrue(adjustedRdd1dot1.first().asInstanceOf[Im1Igmg].post.isCorrected)
//    Assert.assertTrue(adjustedRdd1dot1.first().asInstanceOf[Im1Igmg].pre.isCorrected)
    testCorrection(adjustedRdd1dot1)

    //SCENARIO 2:  cau_int_mis = None and cau_int_corr=1,2,3,4 i.e. case (cau_int_mis, cau_int_cor) == (None, x if x.isDefined)
    // Should update measure despite the serial
    val adjustedRdd2 = igmgCorrectionController.getAdjustedIgmg(testRDDScenario2).cache()
    Assert.assertEquals(1, adjustedRdd2.count())
    Assert.assertEquals(1.0, adjustedRdd2.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].post.measure.get, 0.0)
    Assert.assertEquals(80.0, adjustedRdd2.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].post.converted.get, 0.0)
    Assert.assertEquals(1.0, adjustedRdd2.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].pre.measure.get, 0.0)
    Assert.assertEquals(2.5, adjustedRdd2.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].pre.converted.get, 0.0)
    Assert.assertTrue(adjustedRdd2.first().asInstanceOf[Im1Igmg].post.isCorrected)
    Assert.assertTrue(adjustedRdd2.first().asInstanceOf[Im1Igmg].pre.isCorrected)
    testCorrection(adjustedRdd2)

    //SCENARIO 3:  (cau_int_mis, cau_int_cor) == (None, None),  return un-adjusted IGMG measure
    val adjustedRdd3 = igmgCorrectionController.getAdjustedIgmg(testRDDScenario3).cache()
    Assert.assertEquals(1, adjustedRdd3.count())
    Assert.assertEquals(0.0, adjustedRdd3.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].post.measure.get, 0.0)
    Assert.assertEquals(0.0, adjustedRdd3.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].post.converted.get, 0.0)
    Assert.assertEquals(0.0, adjustedRdd3.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].pre.measure.get, 0.0)
    Assert.assertEquals(0.0, adjustedRdd3.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].pre.converted.get, 0.0)
    Assert.assertFalse(adjustedRdd3.first().asInstanceOf[Im1Igmg].post.isCorrected)
    Assert.assertFalse(adjustedRdd3.first().asInstanceOf[Im1Igmg].pre.isCorrected)
    testCorrection(adjustedRdd3)

    //SCENARIO 4:  RML/Rgl not matching any igmg,  return un-adjusted IGMG measure
    val adjustedRdd4 = igmgCorrectionController.getAdjustedIgmg(testRDDScenario4).cache()
    Assert.assertEquals(1, adjustedRdd4.count())
    Assert.assertEquals(0.0, adjustedRdd4.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].post.measure.get, 0.0)
    Assert.assertEquals(0.0, adjustedRdd4.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].post.converted.get, 0.0)
    Assert.assertEquals(0.0, adjustedRdd4.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].pre.measure.get, 0.0)
    Assert.assertEquals(0.0, adjustedRdd4.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].pre.converted.get, 0.0)
    Assert.assertFalse(adjustedRdd4.first().asInstanceOf[Im1Igmg].post.isCorrected)
    Assert.assertFalse(adjustedRdd4.first().asInstanceOf[Im1Igmg].pre.isCorrected)
    testCorrection(adjustedRdd4)

    //SCENARIO 5: only matching pre matr
    val adjustedRdd5 = igmgCorrectionController.getAdjustedIgmg(testRDDScenario5).cache()
    Assert.assertEquals(1, adjustedRdd5.count())
    Assert.assertEquals(0.0, adjustedRdd5.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].post.measure.get, 0.0)
    Assert.assertEquals(0.0, adjustedRdd5.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].post.converted.get, 0.0)
    Assert.assertEquals(1.0, adjustedRdd5.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].pre.measure.get, 0.0)
    Assert.assertEquals(2.5, adjustedRdd5.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].pre.converted.get, 0.0)
    Assert.assertFalse(adjustedRdd5.first().asInstanceOf[Im1Igmg].post.isCorrected)
    Assert.assertTrue(adjustedRdd5.first().asInstanceOf[Im1Igmg].pre.isCorrected)
    testCorrection(adjustedRdd5)

    //SCENARIO 6: only matching post matr
    val adjustedRdd6 = igmgCorrectionController.getAdjustedIgmg(testRDDScenario6).cache()
    Assert.assertEquals(1, adjustedRdd6.count())
    Assert.assertEquals(1.0, adjustedRdd6.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].post.measure.get, 0.0)
    Assert.assertEquals(2.5, adjustedRdd6.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].post.converted.get, 0.0)
    Assert.assertEquals(0.0, adjustedRdd6.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].pre.measure.get, 0.0)
    Assert.assertEquals(0.0, adjustedRdd6.filter(_.pdr.equalsIgnoreCase("1")).first().asInstanceOf[Im1Igmg].pre.converted.get, 0.0)
    Assert.assertTrue(adjustedRdd6.first().asInstanceOf[Im1Igmg].post.isCorrected)
    Assert.assertFalse(adjustedRdd6.first().asInstanceOf[Im1Igmg].pre.isCorrected)
    testCorrection(adjustedRdd6)

    //SCENARIO 7: no condition for igmg/im1 correction is met and correction measure not concerning im1 nor igmg are not
    // removed from list
    val adjustedRdd7 = igmgCorrectionController.getAdjustedIgmg(testRDDScenario7).cache()
    Assert.assertEquals(2, adjustedRdd7.count())
    Assert.assertEquals(0.0, adjustedRdd7.filter(_.isInstanceOf[Im1Igmg]).first().asInstanceOf[Im1Igmg].post.measure.get, 0.0)
    Assert.assertEquals(80.0, adjustedRdd7.filter(_.isInstanceOf[Im1Igmg]).first().asInstanceOf[Im1Igmg].post.converted.get, 0.0)
    Assert.assertEquals(0.0, adjustedRdd7.filter(_.isInstanceOf[Im1Igmg]).first().asInstanceOf[Im1Igmg].pre.measure.get, 0.0)
    Assert.assertEquals(0.0, adjustedRdd7.filter(_.isInstanceOf[Im1Igmg]).first().asInstanceOf[Im1Igmg].pre.converted.get, 0.0)
    Assert.assertFalse(adjustedRdd7.filter(_.isInstanceOf[Im1Igmg]).first().asInstanceOf[Im1Igmg].post.isCorrected)
    Assert.assertFalse(adjustedRdd7.filter(_.isInstanceOf[Im1Igmg]).first().asInstanceOf[Im1Igmg].pre.isCorrected)
    Assert.assertTrue(adjustedRdd7.filter(!_.isInstanceOf[Im1Igmg]).first().isInstanceOf[Rgl])
    testCorrection(adjustedRdd7)

    //SCENARIO 8: nothing to do
    val adjustedRdd8 = igmgCorrectionController.getAdjustedIgmg(testRDDScenario8)
    Assert.assertEquals(3, adjustedRdd8.count())
    testCorrection(adjustedRdd8)

    val adjustedRdd10 = igmgCorrectionController.getAdjustedIgmg(testRDD2Scenario1dot10).cache()
    adjustedRdd10.collect.foreach(println)

    Assert.assertEquals(2, adjustedRdd10.count())

  }

  private def testCorrection(in: RDD[Flow]): Unit = in.foreach {
    case im1Igmg: Im1Igmg if im1Igmg.pre.isCorrected && im1Igmg.post.isCorrected =>
      Assert.assertTrue(im1Igmg.pre.correctionFlow.isDefined && (im1Igmg.pre.correctionFlow.get.isInstanceOf[Rml] || im1Igmg.pre.correctionFlow.get.isInstanceOf[Rgl]))
      Assert.assertTrue(im1Igmg.post.correctionFlow.isDefined && (im1Igmg.post.correctionFlow.get.isInstanceOf[Rml] || im1Igmg.post.correctionFlow.get.isInstanceOf[Rgl]))
    case im1Igmg: Im1Igmg if im1Igmg.pre.isCorrected =>
      Assert.assertTrue(im1Igmg.pre.correctionFlow.isDefined && (im1Igmg.pre.correctionFlow.get.isInstanceOf[Rml] || im1Igmg.pre.correctionFlow.get.isInstanceOf[Rgl]))
    case im1Igmg: Im1Igmg if im1Igmg.post.isCorrected =>
      Assert.assertTrue(im1Igmg.post.correctionFlow.isDefined && (im1Igmg.post.correctionFlow.get.isInstanceOf[Rml] || im1Igmg.post.correctionFlow.get.isInstanceOf[Rgl]))
    case _ => Unit
  }

  private val dateTime1 = Some(DateTime.parse("16/03/2021", MeasureDAO.genericDateTimeFormatter))

  private lazy val testRDDScenario1: RDD[Flow] = Environment.getSpark.sparkContext.parallelize(List(
    Igmg(service = "IGMG", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = Some(1),
      cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None,
      pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = Some(1),
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None),
      post = IgmgPost(service = "IGMGPOST", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = Some(1),
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None)),
    Rml(service = "RML", pdr = "1", date = dateTime1, readType = None, measure = Some(1.0), converted = Some(2.5),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), motivation = Some(1), freqLet = Some(1),
      tipoRettifica = Some("T"), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None)
  ))

  private lazy val testRDD2Scenario1dot1: RDD[Flow] = Environment.getSpark.sparkContext.parallelize(List(
    Igmg(service = "IGMG", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = Some(1),
      cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None,
      pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = dateTime1, readType = None, measure = Some(50.0), converted = Some(80.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = Some(1),
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None),
      post = IgmgPost(service = "IGMGPOST", pdr = "1", date = dateTime1, readType = None, measure = Some(90.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = Some(1),
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None)),
    Rml(service = "RML", pdr = "1", date = dateTime1, readType = None, measure = Some(33.0), converted = Some(22.5),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), motivation = Some(2), freqLet = Some(1),
      tipoRettifica = None, localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None),
    Igmg(service = "IGMG", pdr = "2", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
      serialNumberMis = Some("matr_mis_2"), serialNumberConv = Some("matr_conv_2"), coefCorr = None, cau_int_mis = Some(1),
      cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None,
      pre = IgmgPre(service = "IGMGPRE", pdr = "2", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_2"), serialNumberConv = Some("matr_conv_2"), coefCorr = None, cau_int_mis = Some(1),
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None),
      post = IgmgPost(service = "IGMGPOST", pdr = "2", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_2"), serialNumberConv = Some("matr_conv_2"), coefCorr = None, cau_int_mis = Some(1),
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None)),
    Rgl(service = "RGL", pdr = "2", date = dateTime1, measure = Some(1.0), converted = Some(2.5),
      serialNumberMis = Some("matr_mis_2"), serialNumberConv = Some("matr_conv_2"), motivation = Some(4),
      localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None)
  ))

  private lazy val testRDDScenario2: RDD[Flow] = Environment.getSpark.sparkContext.parallelize(List(
    Igmg(service = "IGMG", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
      serialNumberMis = Some("matr_mis_not_matching"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = None,
      cau_int_cor = Some(4), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None,
      pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_not_matching"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = None,
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None),
      post = IgmgPost(service = "IGMGPOST", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(80.0),
        serialNumberMis = Some("matr_mis_not_matching"), serialNumberConv = Some("matr_conv_2"), coefCorr = None, cau_int_mis = None,
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None)),
    Rml(service = "RML", pdr = "1", date = dateTime1, readType = None, measure = Some(1.0), converted = Some(2.5),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), motivation = Some(1), freqLet = Some(1),
      tipoRettifica = Some("T"), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None)
  ))
  private lazy val testRDDScenario3: RDD[Flow] = Environment.getSpark.sparkContext.parallelize(List(
    Igmg(service = "IGMG", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = None,
      cau_int_cor = None, localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None,
      pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = None,
        cau_int_cor = None, localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None),
      post = IgmgPost(service = "IGMGPOST", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_2"), coefCorr = None, cau_int_mis = None,
        cau_int_cor = None, localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None)),
    Rml(service = "RML", pdr = "1", date = dateTime1, readType = None, measure = Some(1.0), converted = Some(2.5),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), motivation = Some(1), freqLet = Some(1),
      tipoRettifica = Some("T"), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None)
  ))
  private lazy val testRDDScenario4: RDD[Flow] = Environment.getSpark.sparkContext.parallelize(List(
    Igmg(service = "IGMG", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = None,
      cau_int_cor = None, localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None,
      pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = None,
        cau_int_cor = None, localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None),
      post = IgmgPost(service = "IGMGPOST", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_2"), coefCorr = None, cau_int_mis = None,
        cau_int_cor = None, localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None)),
    Rml(service = "RML", pdr = "1", date = dateTime1, readType = None, measure = Some(1.0), converted = Some(2.5),
      serialNumberMis = Some("matr_mis_not_matching"), serialNumberConv = Some("matr_conv_not_matching"), motivation = Some(1), freqLet = Some(1),
      tipoRettifica = Some("T"), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None)
  ))

  private lazy val testRDDScenario5: RDD[Flow] = Environment.getSpark.sparkContext.parallelize(List(
    Igmg(service = "IGMG", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
      serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = Some(1),
      cau_int_cor = Some(4), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None,
      pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = Some(1),
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None),
      post = IgmgPost(service = "IGMGPOST", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_not_matching"), serialNumberConv = Some("matr_conv_not_matching"), coefCorr = None, cau_int_mis = Some(1),
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None)),
    Rml(service = "RML", pdr = "1", date = dateTime1, readType = None, measure = Some(1.0), converted = Some(2.5),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), motivation = Some(1), freqLet = Some(1),
      tipoRettifica = Some("T"), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None)
  ))

  private lazy val testRDDScenario6: RDD[Flow] = Environment.getSpark.sparkContext.parallelize(List(
    Igmg(service = "IGMG", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
      serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = Some(1),
      cau_int_cor = Some(4), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None,
      pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_not_matching"), serialNumberConv = Some("matr_conv_not_matching"), coefCorr = None, cau_int_mis = Some(1),
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None),
      post = IgmgPost(service = "IGMGPOST", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(80.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = Some(1),
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None)),
    Rml(service = "RML", pdr = "1", date = dateTime1, readType = None, measure = Some(1.0), converted = Some(2.5),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), motivation = Some(1), freqLet = Some(1),
      tipoRettifica = Some("T"), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None)
  ))

  private lazy val testRDDScenario7: RDD[Flow] = Environment.getSpark.sparkContext.parallelize(List(
    Igmg(service = "IGMG", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
      serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = Some(1),
      cau_int_cor = Some(4), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None,
      pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_not_matching"), serialNumberConv = Some("matr_conv_not_matching"), coefCorr = None, cau_int_mis = Some(1),
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None),
      post = IgmgPost(service = "IGMGPOST", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(80.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = Some(1),
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None)),
    Rgl(service = "RML", pdr = "1", date = dateTime1, measure = Some(1.0), converted = Some(2.5),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), motivation = None,
      localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None)
  ))

  private lazy val testRDDScenario8: RDD[Flow] = Environment.getSpark.sparkContext.parallelize(List(
    Rgl(service = "RGL", pdr = "1", date = dateTime1, measure = Some(1.0), converted = Some(2.5),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), motivation = Some(5),
      localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None),
    Rgl(service = "RGL", pdr = "1", date = dateTime1, measure = Some(1.0), converted = Some(2.5),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), motivation = Some(5),
      localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None),
    Rgl(service = "RGL", pdr = "1", date = dateTime1, measure = Some(1.0), converted = Some(2.5),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), motivation = Some(5),
      localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None)
  ))
  private lazy val testRDD2Scenario1dot10: RDD[Flow] = Environment.getSpark.sparkContext.parallelize(List(
    Igmg(service = "IGMG", pdr = "1", date = dateTime1, readType = None, measure = Some(0.0), converted = Some(0.0),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = Some(1),
      cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None,
      pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = dateTime1, readType = None, measure = Some(50.0), converted = Some(80.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = Some(1),
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None),
      post = IgmgPost(service = "IGMGPOST", pdr = "1", date = dateTime1, readType = None, measure = Some(90.0), converted = Some(0.0),
        serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), coefCorr = None, cau_int_mis = Some(1),
        cau_int_cor = Some(1), localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        , pivaDistr = None, pivaUtente = None, dataCaricamento = None)),
    Rml(service = "RML", pdr = "1", date = dateTime1, readType = None, measure = Some(33.0), converted = Some(22.5),
      serialNumberMis = Some("matr_mis_1"), serialNumberConv = Some("matr_conv_1"), motivation = Some(2), freqLet = Some(1),
      tipoRettifica = None, localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None),
    Rgl(service = "RGL", pdr = "1", date = dateTime1, measure = Some(1.0), converted = Some(2.5),
      serialNumberMis = Some("matr_mis_2"), serialNumberConv = Some("matr_conv_2"), motivation = Some(4),
      localFile = Option("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
      , pivaDistr = None, pivaUtente = None, dataCaricamento = None)
  ))
}
