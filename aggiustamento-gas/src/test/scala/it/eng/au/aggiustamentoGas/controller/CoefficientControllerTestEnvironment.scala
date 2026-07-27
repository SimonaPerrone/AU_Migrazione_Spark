package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.utility.constants.{DimensionalType, ForcingFlags}
import it.eng.au.aggiustamentoGas.model.agg.{ExternalDailyInfo, FlowWithInfo}
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Igmg, IgmgPost, IgmgPre, Im1, Im1Post, Im1Pre}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Tgl, Tml}
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasTech
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.joda.time.DateTime
import org.junit.Assert

class CoefficientControllerTestEnvironment extends EnvironmentSparkTest {
/*
  private lazy val dummyFlow: Flow = Im1("IM1", "1", None, None, None, None, None, None, None, None, None, None, None, None, None, null, null)
  //defining files with  consequent days to allow flow ordering
  private lazy val dummyFile: String = "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"
  private lazy val dummyDate: DateTime = DateTime.now()
  private lazy val dummyDate1: DateTime = dummyDate.plusHours(1)
  private lazy val dummyDate2: DateTime = dummyDate.plusHours(2)
  private lazy val dummyDate3: DateTime = dummyDate.plusHours(3)
  private lazy val dummyDate4: DateTime = dummyDate.plusHours(4)
  private lazy val coefficientController = new CoefficientController

  /**
   * SCENARIO 1: no im1/igmg special case
   * */
  def testGetScenario1(): Unit = {

    val rcuPdr1 = RcuGasTech(startDateTech = DateTime.now(), endDateTech = DateTime.now(), nIdPdr = "1", nCoeffCorr = Some(14.4),
      tPreConv = None, gruppoMisInt = None, nCifreMis = None, nCifreConv = None)

    val pre = Im1Pre(service = "IM1PRE", pdr = "1", date = Option(dummyDate), readType = None, measure = None, converted = None,
      serialNumberMis = None, serialNumberConv = None, coefCorr = Some(5.0), cau_int_mis = None,
      cau_int_cor = None, localFile = None, pivaDistr = None, pivaUtente = None, dataCaricamento = None)
    val post = Im1Post(service = "IM1POST", pdr = "1", date = Option(dummyDate), readType = None, measure = None, converted = None,
      serialNumberMis = None, serialNumberConv = None, coefCorr = Some(5.5), cau_int_mis = None,
      cau_int_cor = None, localFile = None, pivaDistr = None, pivaUtente = None, dataCaricamento = None)

    val measuresWithInfo1 = Environment.getSpark.sparkContext.parallelize(
      List(
        FlowWithInfo(
          flow = Im1(service = "IM1", pdr = "1", date = Option(dummyDate), readType = None, measure = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
            cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = Some(dummyFile), dataCaricamento = None,
            pre = pre,
            post = post),
          rcuGasTech = Option(rcuPdr1)
        ),
        FlowWithInfo(
          flow = Tgl(service = "TGL", pdr = "1", date = Option(dummyDate), readType = None, measure = None, isValid = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = Some(dummyFile),
            dataCaricamento = None),
          rcuGasTech = Option(rcuPdr1)
        ),
        FlowWithInfo(
          flow = Tml(service = "TML", pdr = "1", date = Option(dummyDate), readType = None, measure = None, isValid = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = Some(dummyFile),
            dataCaricamento = None, coefCorr = None, freqLet = None),
          rcuGasTech = Option(rcuPdr1.copy(tPreConv = Option("NO")))
        )
      )).keyBy(_.flow.pdr).groupByKey().mapValues(f => (f.toList, ExternalDailyInfo()))


    val measureWithCoeffAndDimType = coefficientController.get(measuresWithInfo1).values.flatMap(f => f._1).cache()
    val filterByPdrAndService: (String, String, FlowWithInfo) => Boolean = (pdr, service, flowWithInfo) => {
      flowWithInfo.flow.pdr.equals(pdr) && flowWithInfo.flow.service.equals(service)
    }

    Assert.assertEquals("IM1", measureWithCoeffAndDimType.filter(filterByPdrAndService("1", "IM1", _)).first().flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType.filter(filterByPdrAndService("1", "IM1", _)).first().coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType.filter(filterByPdrAndService("1", "IM1", _)).first().dimensionalType)
    Assert.assertEquals(Option(5.0), measureWithCoeffAndDimType.filter(filterByPdrAndService("1", "IM1", _)).first().im1IgmgCoeff)

    Assert.assertEquals("TGL", measureWithCoeffAndDimType.filter(filterByPdrAndService("1", "TGL", _)).first().flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType.filter(filterByPdrAndService("1", "TGL", _)).first().coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType.filter(filterByPdrAndService("1", "TGL", _)).first().dimensionalType)
    Assert.assertEquals(Option(5.5), measureWithCoeffAndDimType.filter(filterByPdrAndService("1", "TGL", _)).first().im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType.filter(filterByPdrAndService("1", "TML", _)).first().flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType.filter(filterByPdrAndService("1", "TML", _)).first().coeff)
    Assert.assertEquals(Option(DimensionalType.P), measureWithCoeffAndDimType.filter(filterByPdrAndService("1", "TML", _)).first().dimensionalType)
    Assert.assertEquals(Option(5.5), measureWithCoeffAndDimType.filter(filterByPdrAndService("1", "TML", _)).first().im1IgmgCoeff)


    measureWithCoeffAndDimType.foreach(fwi => Assert.assertTrue(fwi.flow.dimTypeForced.isEmpty))
  }

  /**
   * SCENARIO 2: im1 special case
   * 2.1: at time i there is an im1 with cau_int_cor = 3 and exist an im1 with cau_int_cor != 3 at instant j with j>i+1
   * *
   * TML       IM(cau_int_corr = 3)         TML         IM(cau_int_corr = 4)        TML
   * |------------------|--------------------|------------------|--------------------|
   * C                  PK                   PK                 C                    C      Expected dimensionalType
   * forced             forced               forced            computed              computed
   *
   * 2.2: at time i there is an im1 with cau_int_cor = 3 and does not exist an im1 with cau_int_cor != 3 at instant j with j>i+1
   * TML       IM(cau_int_corr = 3)         TML                 TML
   * |------------------|--------------------|------------------|
   * C                  PK                   PK                 PK             Expected dimensionalType
   * forced             forced               forced            forced
   *
   * 2.3: at time i there is an im1 with cau_int_cor = 5 and exist an im1 with cau_int_cor != 5 at instant j with j<i-1
   * TML       IM(cau_int_corr = 4)         TML         IM(cau_int_corr = 5)        TML
   * |------------------|--------------------|------------------|--------------------|
   * P                  PK                   PK                 C                    C      Expected dimensionalType
   * computed           forced               forced            forced              forced
   *
   * 2.4: at time i there is an im1 with cau_int_cor = 5 and does not exist an im1 with cau_int_cor != 5 at instant j with j<i-1
   * TML                 TML         IM(cau_int_corr = 5)        TML
   * |--------------------|------------------|--------------------|
   * PK                   PK                 C                    C      Expected dimensionalType
   * forced               forced            forced              forced
   *
   * */
  def testGetScenario2(): Unit = {
    val pre = Im1Pre(service = "IM1PRE", pdr = "1", date = Option(dummyDate), readType = None, measure = None, converted = None,
      serialNumberMis = None, serialNumberConv = None, coefCorr = Some(5.0), cau_int_mis = None,
      cau_int_cor = None, localFile = None, pivaDistr = None, pivaUtente = None, dataCaricamento = None)
    val post = Im1Post(service = "IM1POST", pdr = "1", date = Option(dummyDate), readType = None, measure = None, converted = None,
      serialNumberMis = None, serialNumberConv = None, coefCorr = Some(5.5), cau_int_mis = None,
      cau_int_cor = None, localFile = None, pivaDistr = None, pivaUtente = None, dataCaricamento = None)
    // 2.1
    val rcuPdr1 = RcuGasTech(startDateTech = DateTime.now(), endDateTech = DateTime.now(), nIdPdr = "1", nCoeffCorr = Some(14.4),
      tPreConv = None, gruppoMisInt = None, nCifreMis = None, nCifreConv = None)
    val measuresWithInfo2dot1 = Environment.getSpark.sparkContext.parallelize(
      List(
        FlowWithInfo(
          flow = Tgl(service = "TML", pdr = "1", date = Option(dummyDate), readType = None, measure = None, isValid = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = Some(dummyFile),
            dataCaricamento = None),
          rcuGasTech = Option(rcuPdr1)
        ),
        FlowWithInfo(
          flow = Im1(service = "IM1", pdr = "1", date = Option(dummyDate1), readType = None, measure = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
            cau_int_cor = Option(3), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None, pre = pre, post = post),
          rcuGasTech = Option(rcuPdr1)
        ),
        FlowWithInfo(
          flow = Tml(service = "TML", pdr = "1", date = Option(dummyDate2), readType = None, measure = None, isValid = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
            dataCaricamento = None, coefCorr = None, freqLet = None),
          rcuGasTech = Option(rcuPdr1.copy(tPreConv = Option("NO")))
        ),
        FlowWithInfo(
          flow = Im1(service = "IM1", pdr = "1", date = Option(dummyDate3), readType = None, measure = None,
            converted = Option(1.0), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
            cau_int_cor = Option(4), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None, pre = pre.copy(coefCorr = Some(6.0)), post = post.copy(coefCorr = Some(6.5))),
          rcuGasTech = Option(rcuPdr1)
        ),
        FlowWithInfo(
          flow = Tml(service = "TML", pdr = "1", date = Option(dummyDate4), readType = None, measure = None, isValid = None,
            converted = Option(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
            dataCaricamento = None, coefCorr = None, freqLet = None),
          rcuGasTech = Option(rcuPdr1)
        )
      )).keyBy(_.flow.pdr).groupByKey().mapValues(f => (f.toList, ExternalDailyInfo()))

    val measureWithCoeffAndDimType2Dot1 = coefficientController.get(measuresWithInfo2dot1).values.flatMap(f => f._1)
      .collect().sorted(FlowWithInfo.orderingFlowsByDateTime)

    Assert.assertEquals("TML", measureWithCoeffAndDimType2Dot1(0).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot1(0).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType2Dot1(0).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FB), measureWithCoeffAndDimType2Dot1(0).flow.dimTypeForced)
    Assert.assertEquals(Option(5.0), measureWithCoeffAndDimType2Dot1(0).im1IgmgCoeff)


    Assert.assertEquals("IM1", measureWithCoeffAndDimType2Dot1(1).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot1(1).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType2Dot1(1).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType2Dot1(1).flow.dimTypeForced)
    Assert.assertEquals(Option(5.0), measureWithCoeffAndDimType2Dot1(1).im1IgmgCoeff)


    Assert.assertEquals("TML", measureWithCoeffAndDimType2Dot1(2).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot1(2).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType2Dot1(2).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType2Dot1(2).flow.dimTypeForced)
    Assert.assertEquals(Option(6.0), measureWithCoeffAndDimType2Dot1(2).im1IgmgCoeff)

    Assert.assertEquals("IM1", measureWithCoeffAndDimType2Dot1(3).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot1(3).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType2Dot1(3).dimensionalType)
    Assert.assertEquals(None, measureWithCoeffAndDimType2Dot1(3).flow.dimTypeForced)
    Assert.assertEquals(Option(6.0), measureWithCoeffAndDimType2Dot1(3).im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType2Dot1(4).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot1(4).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType2Dot1(4).dimensionalType)
    Assert.assertEquals(None, measureWithCoeffAndDimType2Dot1(4).flow.dimTypeForced)
    Assert.assertEquals(Option(6.5), measureWithCoeffAndDimType2Dot1(4).im1IgmgCoeff)

    //2.2

    val measuresWithInfo2dot2 = measuresWithInfo2dot1.values.flatMap(f => f._1).filter(flowWithInfo => (!flowWithInfo.flow.isInstanceOf[Im1]) || (flowWithInfo.flow.asInstanceOf[Im1].cau_int_cor != Option(4)))
    val measureWithCoeffAndDimType2Dot2 = coefficientController.get(measuresWithInfo2dot2.keyBy(_.flow.pdr).groupByKey().mapValues(f => (f.toList, ExternalDailyInfo())))
      .values.flatMap(f => f._1).collect().sorted(FlowWithInfo.orderingFlowsByDateTime)
    Assert.assertEquals("TML", measureWithCoeffAndDimType2Dot2(0).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot2(0).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType2Dot2(0).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FB), measureWithCoeffAndDimType2Dot2(0).flow.dimTypeForced)
    Assert.assertEquals(Option(5.0), measureWithCoeffAndDimType2Dot2(0).im1IgmgCoeff)

    Assert.assertEquals("IM1", measureWithCoeffAndDimType2Dot2(1).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot2(1).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType2Dot2(1).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType2Dot2(1).flow.dimTypeForced)
    Assert.assertEquals(Option(5.0), measureWithCoeffAndDimType2Dot2(1).im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType2Dot2(2).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot2(2).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType2Dot2(2).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType2Dot2(2).flow.dimTypeForced)
    Assert.assertEquals(Option(5.5), measureWithCoeffAndDimType2Dot2(2).im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType2Dot2(3).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot2(3).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType2Dot2(3).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType2Dot2(3).flow.dimTypeForced)
    Assert.assertEquals(Option(5.5), measureWithCoeffAndDimType2Dot2(3).im1IgmgCoeff)

    //2.3
    val measuresWithInfo2dot3 = Environment.getSpark.sparkContext.parallelize(
      List(
        FlowWithInfo(
          flow = Tgl(service = "TML", pdr = "1", date = Option(dummyDate), readType = None, measure = None, isValid = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = Some(dummyFile),
            dataCaricamento = None),
          rcuGasTech = Option(rcuPdr1.copy(tPreConv = Option("NO")))
        ),
        FlowWithInfo(
          flow = Im1(service = "IM1", pdr = "1", date = Option(dummyDate1), readType = None, measure = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
            cau_int_cor = Option(4), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None, pre = pre, post = post),
          rcuGasTech = Option(rcuPdr1)
        ),
        FlowWithInfo(
          flow = Tml(service = "TML", pdr = "1", date = Option(dummyDate2), readType = None, measure = None, isValid = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
            dataCaricamento = None, coefCorr = None, freqLet = None),
          rcuGasTech = Option(rcuPdr1.copy(tPreConv = Option("NO")))
        ),
        FlowWithInfo(
          flow = Im1(service = "IM1", pdr = "1", date = Option(dummyDate3), readType = None, measure = None,
            converted = Option(1.0), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
            cau_int_cor = Option(5), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None, pre = pre, post = post),
          rcuGasTech = Option(rcuPdr1)
        ),
        FlowWithInfo(
          flow = Tml(service = "TML", pdr = "1", date = Option(dummyDate4), readType = None, measure = None, isValid = None,
            converted = Option(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
            dataCaricamento = None, coefCorr = None, freqLet = None),
          rcuGasTech = Option(rcuPdr1)
        )
      )).keyBy(_.flow.pdr).groupByKey().mapValues(f => (f.toList, ExternalDailyInfo()))

    val measureWithCoeffAndDimType2Dot3 = coefficientController.get(measuresWithInfo2dot3).values.flatMap(f => f._1)
      .collect().sorted(FlowWithInfo.orderingFlowsByDateTime)

    Assert.assertEquals("TML", measureWithCoeffAndDimType2Dot3(0).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot3(0).coeff)
    Assert.assertEquals(Option(DimensionalType.P), measureWithCoeffAndDimType2Dot3(0).dimensionalType)
    Assert.assertEquals(None, measureWithCoeffAndDimType2Dot3(0).flow.dimTypeForced)
    Assert.assertEquals(Option(5.0), measureWithCoeffAndDimType2Dot3(0).im1IgmgCoeff)

    Assert.assertEquals("IM1", measureWithCoeffAndDimType2Dot3(1).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot3(1).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType2Dot3(1).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FB), measureWithCoeffAndDimType2Dot3(1).flow.dimTypeForced)
    Assert.assertEquals(Option(5.0), measureWithCoeffAndDimType2Dot3(1).im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType2Dot3(2).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot3(2).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType2Dot3(2).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FB), measureWithCoeffAndDimType2Dot3(2).flow.dimTypeForced)
    Assert.assertEquals(Option(5.0), measureWithCoeffAndDimType2Dot3(2).im1IgmgCoeff)

    Assert.assertEquals("IM1", measureWithCoeffAndDimType2Dot3(3).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot3(3).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType2Dot3(3).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType2Dot3(3).flow.dimTypeForced)
    Assert.assertEquals(Option(5.0), measureWithCoeffAndDimType2Dot3(3).im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType2Dot3(4).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot3(4).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType2Dot3(4).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType2Dot3(4).flow.dimTypeForced)
    Assert.assertEquals(Option(5.5), measureWithCoeffAndDimType2Dot3(4).im1IgmgCoeff)

    //2.4
    val measuresWithInfo2dot4 = measuresWithInfo2dot3.values.flatMap(f => f._1).filter(fwi => (!fwi.flow.isInstanceOf[Im1]) || (fwi.flow.asInstanceOf[Im1].cau_int_cor != Some(4)))
    val measureWithCoeffAndDimType2Dot4 = coefficientController.get(measuresWithInfo2dot4.keyBy(_.flow.pdr).groupByKey().mapValues(f => (f.toList, ExternalDailyInfo())))
      .values.flatMap(f => f._1).collect().sorted(FlowWithInfo.orderingFlowsByDateTime)
    Assert.assertEquals("TML", measureWithCoeffAndDimType2Dot4(0).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot4(0).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType2Dot4(0).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FB), measureWithCoeffAndDimType2Dot4(0).flow.dimTypeForced)
    Assert.assertEquals(Option(5.0), measureWithCoeffAndDimType2Dot4(0).im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType2Dot4(1).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot4(1).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType2Dot4(1).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FB), measureWithCoeffAndDimType2Dot4(1).flow.dimTypeForced)
    Assert.assertEquals(Option(5.0), measureWithCoeffAndDimType2Dot4(1).im1IgmgCoeff)

    Assert.assertEquals("IM1", measureWithCoeffAndDimType2Dot4(2).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot4(2).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType2Dot4(2).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType2Dot4(2).flow.dimTypeForced)
    Assert.assertEquals(Option(5.0), measureWithCoeffAndDimType2Dot4(2).im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType2Dot4(3).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType2Dot4(3).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType2Dot4(3).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType2Dot4(3).flow.dimTypeForced)
    Assert.assertEquals(Option(5.5), measureWithCoeffAndDimType2Dot4(3).im1IgmgCoeff)

  }

  /**
   * SCENARIO 3: im1 special case
   * 3.1: at time i there is an igmg with cau_int_cor = 2 and exist an igmg with cau_int_cor != 2 at instant j with j>i+1
   * *
   * TML       IGMG(cau_int_corr = 2)         TML         IGMG(cau_int_corr = 1)        TML
   * |------------------|--------------------|------------------|--------------------|
   * C                  PK                   PK                 C                    C      Expected dimensionalType
   * forced             forced               forced            computed              computed
   *
   * 3.2: at time i there is an igmg with cau_int_cor = 2 and does not exist an igmg with cau_int_cor != 2 at instant j with j>i+1
   * TML       IGMG(cau_int_corr = 2)         TML                 TML
   * |------------------|--------------------|------------------|
   * C                  PK                   PK                 PK             Expected dimensionalType
   * forced             forced               forced            forced
   *
   * 3.3: at time i there is an igmg with cau_int_cor = 4 and exist an igmg with cau_int_cor != 4 at instant j with j<i-1
   * TML       IGMG(cau_int_corr = 3)         TML         IGMG(cau_int_corr = 4)        TML
   * |------------------|--------------------|------------------|--------------------|
   * PK                  C                   PK                 C                    C      Expected dimensionalType
   * forced           forced               forced            forced              forced
   *
   * 3.4: at time i there is an igmg with cau_int_cor = 4 and does not exist an igmg with cau_int_cor != 4 at instant j with j<i-1
   * TML                 TML         IGMG(cau_int_corr = 4)        TML
   * |--------------------|------------------|--------------------|
   * PK                   PK                 C                    C      Expected dimensionalType
   * forced               forced            forced              forced
   *
   * */
  def testGetScenario3(): Unit = {
    val pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = Option(dummyDate), readType = None, measure = None, converted = None,
      serialNumberMis = None, serialNumberConv = None, coefCorr = Some(7.0), cau_int_mis = None,
      cau_int_cor = None, localFile = None, pivaDistr = None, pivaUtente = None, dataCaricamento = None)
    val post = IgmgPost(service = "IGMGPOST", pdr = "1", date = Option(dummyDate), readType = None, measure = None, converted = None,
      serialNumberMis = None, serialNumberConv = None, coefCorr = Some(7.5), cau_int_mis = None,
      cau_int_cor = None, localFile = None, pivaDistr = None, pivaUtente = None, dataCaricamento = None)
    // 3.1
    val rcuPdr1 = RcuGasTech(startDateTech = DateTime.now(), endDateTech = DateTime.now(), nIdPdr = "1", nCoeffCorr = Some(14.4),
      tPreConv = None, gruppoMisInt = None, nCifreMis = None, nCifreConv = None)

    val measuresWithInfo3dot1 = Environment.getSpark.sparkContext.parallelize(
      List(
        FlowWithInfo(
          flow = Tgl(service = "TML", pdr = "1", date = Option(dummyDate), readType = None, measure = None, isValid = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = Some(dummyFile),
            dataCaricamento = None),
          rcuGasTech = Option(rcuPdr1)
        ),
        FlowWithInfo(
          flow = Igmg(service = "IGMG", pdr = "1", date = Option(dummyDate1), readType = None, measure = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
            cau_int_cor = Option(2), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None, pre = pre, post = post),
          rcuGasTech = Option(rcuPdr1)
        ),
        FlowWithInfo(
          flow = Tml(service = "TML", pdr = "1", date = Option(dummyDate2), readType = None, measure = None, isValid = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
            dataCaricamento = None, coefCorr = None, freqLet = None),
          rcuGasTech = Option(rcuPdr1.copy(tPreConv = Option("NO")))
        ),
        FlowWithInfo(
          flow = Igmg(service = "IGMG", pdr = "1", date = Option(dummyDate3), readType = None, measure = None,
            converted = Option(1.0), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
            cau_int_cor = Option(1), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None, pre = pre.copy(coefCorr = Some(8.0)), post = post.copy(coefCorr = Some(8.5))),
          rcuGasTech = Option(rcuPdr1)
        ),
        FlowWithInfo(
          flow = Tml(service = "TML", pdr = "1", date = Option(dummyDate4), readType = None, measure = None, isValid = None,
            converted = Option(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
            dataCaricamento = None, coefCorr = None, freqLet = None),
          rcuGasTech = Option(rcuPdr1)
        )
      )).keyBy(_.flow.pdr).groupByKey().mapValues(f => (f.toList, ExternalDailyInfo()))

    val measureWithCoeffAndDimType3Dot1 = coefficientController.get(measuresWithInfo3dot1)
      .values.flatMap(f => f._1).collect().sorted(FlowWithInfo.orderingFlowsByDateTime)

    Assert.assertEquals("TML", measureWithCoeffAndDimType3Dot1(0).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot1(0).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType3Dot1(0).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FB), measureWithCoeffAndDimType3Dot1(0).flow.dimTypeForced)
    Assert.assertEquals(Option(7.0), measureWithCoeffAndDimType3Dot1(0).im1IgmgCoeff)

    Assert.assertEquals("IGMG", measureWithCoeffAndDimType3Dot1(1).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot1(1).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType3Dot1(1).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType3Dot1(1).flow.dimTypeForced)
    Assert.assertEquals(Option(7.0), measureWithCoeffAndDimType3Dot1(1).im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType3Dot1(2).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot1(2).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType3Dot1(2).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType3Dot1(2).flow.dimTypeForced)
    Assert.assertEquals(Option(8.0), measureWithCoeffAndDimType3Dot1(2).im1IgmgCoeff)

    Assert.assertEquals("IGMG", measureWithCoeffAndDimType3Dot1(3).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot1(3).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType3Dot1(3).dimensionalType)
    Assert.assertEquals(None, measureWithCoeffAndDimType3Dot1(3).flow.dimTypeForced)
    Assert.assertEquals(Option(8.0), measureWithCoeffAndDimType3Dot1(3).im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType3Dot1(4).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot1(4).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType3Dot1(4).dimensionalType)
    Assert.assertEquals(None, measureWithCoeffAndDimType3Dot1(4).flow.dimTypeForced)
    Assert.assertEquals(Option(8.5), measureWithCoeffAndDimType3Dot1(4).im1IgmgCoeff)

    //3.2

    val measuresWithInfo3dot2 = measuresWithInfo3dot1.values.flatMap(f => f._1).filter(flowWithInfo => (!flowWithInfo.flow.isInstanceOf[Igmg]) || (flowWithInfo.flow.asInstanceOf[Igmg].cau_int_cor != Option(1)))
    val measureWithCoeffAndDimType3Dot2 = coefficientController.get(measuresWithInfo3dot2.keyBy(_.flow.pdr).groupByKey().mapValues(f => (f.toList, ExternalDailyInfo())))
      .values.flatMap(f => f._1).collect().sorted(FlowWithInfo.orderingFlowsByDateTime)
    Assert.assertEquals("TML", measureWithCoeffAndDimType3Dot2(0).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot2(0).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType3Dot2(0).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FB), measureWithCoeffAndDimType3Dot2(0).flow.dimTypeForced)
    Assert.assertEquals(Option(7.0), measureWithCoeffAndDimType3Dot2(0).im1IgmgCoeff)

    Assert.assertEquals("IGMG", measureWithCoeffAndDimType3Dot2(1).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot2(1).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType3Dot2(1).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType3Dot2(1).flow.dimTypeForced)
    Assert.assertEquals(Option(7.0), measureWithCoeffAndDimType3Dot2(1).im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType3Dot2(2).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot2(2).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType3Dot2(2).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType3Dot2(2).flow.dimTypeForced)
    Assert.assertEquals(Option(7.5), measureWithCoeffAndDimType3Dot2(2).im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType3Dot2(3).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot2(3).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType3Dot2(3).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType3Dot2(3).flow.dimTypeForced)
    Assert.assertEquals(Option(7.5), measureWithCoeffAndDimType3Dot2(3).im1IgmgCoeff)

    //3.3
    val measuresWithInfo3dot3 = Environment.getSpark.sparkContext.parallelize(
      List(
        FlowWithInfo(
          flow = Tgl(service = "TML", pdr = "1", date = Option(dummyDate), readType = None, measure = None, isValid = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = Some(dummyFile),
            dataCaricamento = None),
          rcuGasTech = Option(rcuPdr1.copy(tPreConv = Option("NO")))
        ),
        FlowWithInfo(
          flow = Igmg(service = "IGMG", pdr = "1", date = Option(dummyDate1), readType = None, measure = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
            cau_int_cor = Option(3), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None, pre = pre, post = post),
          rcuGasTech = Option(rcuPdr1)
        ),
        FlowWithInfo(
          flow = Tml(service = "TML", pdr = "1", date = Option(dummyDate2), readType = None, measure = None, isValid = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
            dataCaricamento = None, coefCorr = None, freqLet = None),
          rcuGasTech = Option(rcuPdr1.copy(tPreConv = Option("NO")))
        ),
        FlowWithInfo(
          flow = Igmg(service = "IGMG", pdr = "1", date = Option(dummyDate3), readType = None, measure = None,
            converted = Option(1.0), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
            cau_int_cor = Option(4), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None, pre = pre.copy(coefCorr = Some(4.0)), post = post.copy(coefCorr = Some(4.5))),
          rcuGasTech = Option(rcuPdr1)
        ),
        FlowWithInfo(
          flow = Tml(service = "TML", pdr = "1", date = Option(dummyDate4), readType = None, measure = None, isValid = None,
            converted = Option(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
            dataCaricamento = None, coefCorr = None, freqLet = None),
          rcuGasTech = Option(rcuPdr1)
        )
      )).keyBy(_.flow.pdr).groupByKey().mapValues(f => (f.toList, ExternalDailyInfo()))

    val measureWithCoeffAndDimType3Dot3 = coefficientController.get(measuresWithInfo3dot3).values.flatMap(f => f._1).collect().sorted(FlowWithInfo.orderingFlowsByDateTime)

    Assert.assertEquals("TML", measureWithCoeffAndDimType3Dot3(0).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot3(0).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType3Dot3(0).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FB), measureWithCoeffAndDimType3Dot3(0).flow.dimTypeForced)
    Assert.assertEquals(Option(7.0), measureWithCoeffAndDimType3Dot3(0).im1IgmgCoeff)

    Assert.assertEquals("IGMG", measureWithCoeffAndDimType3Dot3(1).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot3(1).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType3Dot3(1).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FB), measureWithCoeffAndDimType3Dot3(1).flow.dimTypeForced)
    Assert.assertEquals(Option(7.0), measureWithCoeffAndDimType3Dot3(1).im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType3Dot3(2).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot3(2).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType3Dot3(2).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FB), measureWithCoeffAndDimType3Dot3(2).flow.dimTypeForced)
    Assert.assertEquals(Option(4.0), measureWithCoeffAndDimType3Dot3(2).im1IgmgCoeff)

    Assert.assertEquals("IGMG", measureWithCoeffAndDimType3Dot3(3).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot3(3).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType3Dot3(3).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType3Dot3(3).flow.dimTypeForced)
    Assert.assertEquals(Option(4.0), measureWithCoeffAndDimType3Dot3(3).im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType3Dot3(4).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot3(4).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType3Dot3(4).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType3Dot3(4).flow.dimTypeForced)
    Assert.assertEquals(Option(4.5), measureWithCoeffAndDimType3Dot3(4).im1IgmgCoeff)

    //3.4
    val measuresWithInfo3dot4 = measuresWithInfo3dot3.values.flatMap(f => f._1).filter(fwi => (!fwi.flow.isInstanceOf[Igmg]) || (fwi.flow.asInstanceOf[Igmg].cau_int_cor != Some(3)))
    val measureWithCoeffAndDimType3Dot4 = coefficientController.get(measuresWithInfo3dot4.keyBy(_.flow.pdr).groupByKey().mapValues(f => (f.toList, ExternalDailyInfo())))
      .values.flatMap(f => f._1).collect().sorted(FlowWithInfo.orderingFlowsByDateTime)
    Assert.assertEquals("TML", measureWithCoeffAndDimType3Dot4(0).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot4(0).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType3Dot4(0).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FB), measureWithCoeffAndDimType3Dot4(0).flow.dimTypeForced)
    Assert.assertEquals(Option(4.0), measureWithCoeffAndDimType3Dot4(0).im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType3Dot4(1).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot4(1).coeff)
    Assert.assertEquals(Option(DimensionalType.PK), measureWithCoeffAndDimType3Dot4(1).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FB), measureWithCoeffAndDimType3Dot4(1).flow.dimTypeForced)
    Assert.assertEquals(Option(4.0), measureWithCoeffAndDimType3Dot4(1).im1IgmgCoeff)

    Assert.assertEquals("IGMG", measureWithCoeffAndDimType3Dot4(2).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot4(2).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType3Dot4(2).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType3Dot4(2).flow.dimTypeForced)
    Assert.assertEquals(Option(4.0), measureWithCoeffAndDimType3Dot4(2).im1IgmgCoeff)

    Assert.assertEquals("TML", measureWithCoeffAndDimType3Dot4(3).flow.service)
    Assert.assertEquals(rcuPdr1.nCoeffCorr, measureWithCoeffAndDimType3Dot4(3).coeff)
    Assert.assertEquals(Option(DimensionalType.C), measureWithCoeffAndDimType3Dot4(3).dimensionalType)
    Assert.assertEquals(Some(ForcingFlags.FF), measureWithCoeffAndDimType3Dot4(3).flow.dimTypeForced)
    Assert.assertEquals(Option(4.5), measureWithCoeffAndDimType3Dot4(3).im1IgmgCoeff)
  }

  /**
   * Testing Controller resilience to empty input data
   * */
  def testGetScenario4(): Unit = {
    val measuresWithInfo4 = Environment.getSpark.sparkContext.parallelize(
      List().asInstanceOf[List[FlowWithInfo]]
    ).keyBy(_.flow.pdr).groupByKey().mapValues(f => (f.toList, ExternalDailyInfo()))

    val measureWithCoeffAndDimType4 = coefficientController.get(measuresWithInfo4)
      .values.flatMap(f => f._1).collect().sorted(FlowWithInfo.orderingFlowsByDateTime)

    Assert.assertTrue(measureWithCoeffAndDimType4.isEmpty)
  }

  /**
   * No im1/igmg scenario: checking that no im1IgmgCoeff is assigned to flowWithInfo
   */
  def testScenario5(): Unit = {
    val rcuPdr1 = RcuGasTech(startDateTech = DateTime.now(), endDateTech = DateTime.now(), nIdPdr = "1", nCoeffCorr = Some(14.4),
      tPreConv = None, gruppoMisInt = None, nCifreMis = None, nCifreConv = None)
    val measuresWithInfo2dot1 = Environment.getSpark.sparkContext.parallelize(
      List(
        FlowWithInfo(
          flow = Tgl(service = "TML", pdr = "1", date = Option(dummyDate), readType = None, measure = None, isValid = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = Some(dummyFile),
            dataCaricamento = None),
          rcuGasTech = Option(rcuPdr1)
        ),
        FlowWithInfo(
          flow = Tml(service = "TML", pdr = "1", date = Option(dummyDate2), readType = None, measure = None, isValid = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
            dataCaricamento = None, coefCorr = None, freqLet = None),
          rcuGasTech = Option(rcuPdr1.copy(tPreConv = Option("NO")))
        ),
        FlowWithInfo(
          flow = Tml(service = "TML", pdr = "1", date = Option(dummyDate4), readType = None, measure = None, isValid = None,
            converted = Option(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
            dataCaricamento = None, coefCorr = None, freqLet = None),
          rcuGasTech = Option(rcuPdr1)
        )
      )).keyBy(_.flow.pdr).groupByKey().mapValues(f => (f.toList, ExternalDailyInfo()))

    val measureWithCoeffAndDimType2Dot1 = coefficientController.get(measuresWithInfo2dot1).values.flatMap(f => f._1)
      .collect().sorted(FlowWithInfo.orderingFlowsByDateTime)

    Assert.assertEquals(3, measureWithCoeffAndDimType2Dot1.length)
    measureWithCoeffAndDimType2Dot1.foreach(fwi => Assert.assertTrue(fwi.im1IgmgCoeff.isEmpty))

  }

  def testCoefficientSanitization(): Unit = {
    Assert.assertEquals(Some(CoefficientController.COEFFICIENT_DEFAULT), CoefficientController.sanitizeCoefficient(Some(CoefficientController.COEFFICIENT_MAX + 1)))
    Assert.assertEquals(Some(CoefficientController.COEFFICIENT_MAX), CoefficientController.sanitizeCoefficient(Some(CoefficientController.COEFFICIENT_MAX)))
    Assert.assertEquals(Some(CoefficientController.COEFFICIENT_MAX - 1), CoefficientController.sanitizeCoefficient(Some(CoefficientController.COEFFICIENT_MAX - 1)))
    Assert.assertEquals(Some(CoefficientController.COEFFICIENT_DEFAULT + 1), CoefficientController.sanitizeCoefficient(Some(CoefficientController.COEFFICIENT_DEFAULT + 1)))
    Assert.assertEquals(Some(CoefficientController.COEFFICIENT_DEFAULT - 1), CoefficientController.sanitizeCoefficient(Some(CoefficientController.COEFFICIENT_DEFAULT - 1)))
    Assert.assertEquals(Some(CoefficientController.COEFFICIENT_DEFAULT), CoefficientController.sanitizeCoefficient(Some(CoefficientController.COEFFICIENT_DEFAULT)))
  }

  def testGetDimensionalType(): Unit = {
    val flow = Tgl("Tgl", "1", None, None, None, None, None, None, None, None, None, None, None)
    val rcu = RcuGasTech(startDateTech = DateTime.now(), endDateTech = DateTime.now(), nIdPdr = "1", nCoeffCorr = Some(14.4),
      tPreConv = None, gruppoMisInt = None, nCifreMis = None, nCifreConv = None)

    val flowWithInfo = FlowWithInfo(flow = flow, rcuGasTech = Option(rcu))
    Assert.assertEquals(DimensionalType.PK, CoefficientController.getDimensionalType(flowWithInfo)) //default case when converted is not defined
    Assert.assertEquals(DimensionalType.PK, CoefficientController.getDimensionalType(flowWithInfo.copy(rcuGasTech = None))) //default case when converted is not defined

    val flowWithConverted = flowWithInfo.copy(flow = flow.copy(converted = Option(1.0)))
    Assert.assertEquals(DimensionalType.C, CoefficientController.getDimensionalType(flowWithConverted)) //default case when converted is not defined
    Assert.assertEquals(DimensionalType.C, CoefficientController.getDimensionalType(flowWithConverted.copy(rcuGasTech = None))) //default case when converted is not defined

    val rcuDTP = rcu.copy(gruppoMisInt = Option("SI"), tPreConv = None)
    val rcuDTC = rcu.copy(gruppoMisInt = None, tPreConv = Option("SI"))
    val rcuDTPK = rcu.copy(gruppoMisInt = Option("N"), tPreConv = Option("NO"))
    val rcuDTDef = rcu.copy(gruppoMisInt = Option("ahsbcaibk"), tPreConv = Option("asdfg"))
    Assert.assertEquals(DimensionalType.P, CoefficientController.getDimensionalType(flowWithConverted.copy(rcuGasTech = Option(rcuDTP))))
    Assert.assertEquals(DimensionalType.C, CoefficientController.getDimensionalType(flowWithConverted.copy(rcuGasTech = Option(rcuDTC))))
    Assert.assertEquals(DimensionalType.PK, CoefficientController.getDimensionalType(flowWithConverted.copy(rcuGasTech = Option(rcuDTPK))))
    Assert.assertEquals(DimensionalType.C, CoefficientController.getDimensionalType(flowWithConverted.copy(rcuGasTech = Option(rcuDTDef)))) //default case with converted is defined and gruppoMisInt tPreConv not legal


  }

  def testGetRcuCoefficient(): Unit = {
    Assert.assertEquals(Some(CoefficientController.COEFFICIENT_DEFAULT), CoefficientController.getRcuCoeffcient(FlowWithInfo(flow = dummyFlow)))
    Assert.assertEquals(
      Some(14.4),
      CoefficientController.getRcuCoeffcient(
        FlowWithInfo(
          flow = dummyFlow,
          rcuGasTech = Some(RcuGasTech(startDateTech = DateTime.now(), endDateTech = DateTime.now(), nIdPdr = "1", nCoeffCorr = Some(14.4),
            tPreConv = None, gruppoMisInt = None, nCifreMis = None, nCifreConv = None))))
    )
    Assert.assertEquals(
      Some(CoefficientController.COEFFICIENT_DEFAULT),
      CoefficientController.getRcuCoeffcient(
        FlowWithInfo(
          flow = dummyFlow,
          rcuGasTech = Some(RcuGasTech(startDateTech = DateTime.now(), endDateTech = DateTime.now(), nIdPdr = "1", nCoeffCorr = None,
            tPreConv = None, gruppoMisInt = None, nCifreMis = None, nCifreConv = None))))
    )
  }
*/
}