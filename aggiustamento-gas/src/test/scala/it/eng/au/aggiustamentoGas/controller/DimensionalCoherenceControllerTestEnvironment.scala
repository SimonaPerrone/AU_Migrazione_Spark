package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.utility.constants.{DimensionalType, ForcingFlags, Treatment}
import it.eng.au.aggiustamentoGas.model.agg.{ExternalDailyInfo, FlowWithInfo, MonthTreatment}
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Igmg, IgmgPost, IgmgPre, Im1, Im1Post, Im1Pre}
import it.eng.au.aggiustamentoGas.model.measure.{Rgl, Rml, Tgl}
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasTech
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class DimensionalCoherenceControllerTestEnvironment extends EnvironmentSparkTest{

  def testGetCoherentSegmentsRDD(): Unit = {
    val dcc = new DimensionalCoherenceController()
    val pdr = "1"
    val tglWithConvertedValued = Tgl(service = "TML", pdr = pdr, date = None, readType = None, measure = Some(2.0), isValid = None,
      converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None)
    val dummyFlowWithInfo = FlowWithInfo(flow = tglWithConvertedValued, dimensionalType = Some(DimensionalType.C))
    var dummySegmentRDD = createRDD(pdr, dummyFlowWithInfo, dummyFlowWithInfo)
    var coherentDimTypeSegmentRDD = dcc.getCoherentSegmentsRDD(dummySegmentRDD)

    // (C,C) and converted is valued
    val (startMeasure, endMeasure) = coherentDimTypeSegmentRDD.values.keys.first().head
    Assert.assertEquals(Some(DimensionalType.C), startMeasure.dimensionalType)
    Assert.assertEquals(Some(DimensionalType.C), endMeasure.dimensionalType)

    // (C,C) and converted is not valued
    val fwiWithConvertedNotValued =  FlowWithInfo(flow = tglWithConvertedValued.copy(converted = None), dimensionalType = Some(DimensionalType.C))
    dummySegmentRDD = createRDD(pdr, dummyFlowWithInfo, fwiWithConvertedNotValued)
    coherentDimTypeSegmentRDD = dcc.getCoherentSegmentsRDD(dummySegmentRDD)

    val (startMeasure1, endMeasure1) = coherentDimTypeSegmentRDD.values.keys.first().head
    Assert.assertEquals(Some(DimensionalType.PK), startMeasure1.dimensionalType)
    Assert.assertEquals(Some(DimensionalType.PK), endMeasure1.dimensionalType)

    // (C,P) and converted is valued
    dummySegmentRDD = createRDD(pdr, dummyFlowWithInfo, dummyFlowWithInfo.copy(dimensionalType = Some(DimensionalType.P)))
    coherentDimTypeSegmentRDD = dcc.getCoherentSegmentsRDD(dummySegmentRDD)

    val (startMeasure2, endMeasure2) = coherentDimTypeSegmentRDD.values.keys.first().head
    Assert.assertEquals(Some(DimensionalType.C), startMeasure2.dimensionalType)
    Assert.assertEquals(Some(DimensionalType.C), endMeasure2.dimensionalType)

    // (C,P) and converted is not valued
    dummySegmentRDD = createRDD(pdr, dummyFlowWithInfo, dummyFlowWithInfo.copy(flow = tglWithConvertedValued.copy(converted = None), dimensionalType = Some(DimensionalType.P)))
    coherentDimTypeSegmentRDD = dcc.getCoherentSegmentsRDD(dummySegmentRDD)

    val (startMeasure3, endMeasure3) = coherentDimTypeSegmentRDD.values.keys.first().head
    Assert.assertEquals(Some(DimensionalType.P), startMeasure3.dimensionalType)
    Assert.assertEquals(Some(DimensionalType.P), endMeasure3.dimensionalType)

    // (PK, C) and converted is valued
    dummySegmentRDD = createRDD(pdr, dummyFlowWithInfo.copy(dimensionalType = Some(DimensionalType.PK)), dummyFlowWithInfo.copy(dimensionalType = Some(DimensionalType.C)))
    coherentDimTypeSegmentRDD = dcc.getCoherentSegmentsRDD(dummySegmentRDD)

    val (startMeasure4, endMeasure4) = coherentDimTypeSegmentRDD.values.keys.first().head
    Assert.assertEquals(Some(DimensionalType.C), startMeasure4.dimensionalType)
    Assert.assertEquals(Some(DimensionalType.C), endMeasure4.dimensionalType)

    // (PK, C) and converted is not valued
    dummySegmentRDD = createRDD(pdr, dummyFlowWithInfo.copy(dimensionalType = Some(DimensionalType.PK)), dummyFlowWithInfo.copy(flow = tglWithConvertedValued.copy(converted = None), dimensionalType = Some(DimensionalType.C)))
    coherentDimTypeSegmentRDD = dcc.getCoherentSegmentsRDD(dummySegmentRDD)

    val (startMeasure5, endMeasure5) = coherentDimTypeSegmentRDD.values.keys.first().head
    Assert.assertEquals(Some(DimensionalType.PK), startMeasure5.dimensionalType)
    Assert.assertEquals(Some(DimensionalType.PK), endMeasure5.dimensionalType)

    //(PK, PK)
    dummySegmentRDD = createRDD(pdr, dummyFlowWithInfo.copy(dimensionalType = Some(DimensionalType.PK)), dummyFlowWithInfo.copy(dimensionalType = Some(DimensionalType.PK)))
    coherentDimTypeSegmentRDD = dcc.getCoherentSegmentsRDD(dummySegmentRDD)

    val (startMeasure6, endMeasure6) = coherentDimTypeSegmentRDD.values.keys.first().head
    Assert.assertEquals(Some(DimensionalType.PK), startMeasure6.dimensionalType)
    Assert.assertEquals(Some(DimensionalType.PK), endMeasure6.dimensionalType)


    //(PK, P) and C is valued
    dummySegmentRDD = createRDD(pdr, dummyFlowWithInfo.copy(dimensionalType = Some(DimensionalType.PK)), dummyFlowWithInfo.copy(dimensionalType = Some(DimensionalType.P)))
    coherentDimTypeSegmentRDD = dcc.getCoherentSegmentsRDD(dummySegmentRDD)

    val (startMeasure7, endMeasure7) = coherentDimTypeSegmentRDD.values.keys.first().head
    Assert.assertEquals(Some(DimensionalType.PK), startMeasure7.dimensionalType)
    Assert.assertEquals(Some(DimensionalType.PK), endMeasure7.dimensionalType)

    //(PK, P) and C is not valued
    dummySegmentRDD = createRDD(pdr, dummyFlowWithInfo.copy(dimensionalType = Some(DimensionalType.PK)), dummyFlowWithInfo.copy(flow = tglWithConvertedValued.copy(converted = None), dimensionalType = Some(DimensionalType.P)))
    coherentDimTypeSegmentRDD = dcc.getCoherentSegmentsRDD(dummySegmentRDD)

    val (startMeasure71, endMeasure71) = coherentDimTypeSegmentRDD.values.keys.first().head
    Assert.assertEquals(Some(DimensionalType.PK), startMeasure71.dimensionalType)
    Assert.assertEquals(Some(DimensionalType.PK), endMeasure71.dimensionalType)

    //(P,P)
    dummySegmentRDD = createRDD(pdr, dummyFlowWithInfo.copy(dimensionalType = Some(DimensionalType.P)), dummyFlowWithInfo.copy(dimensionalType = Some(DimensionalType.P)))
    coherentDimTypeSegmentRDD = dcc.getCoherentSegmentsRDD(dummySegmentRDD)

    val (startMeasure8, endMeasure8) = coherentDimTypeSegmentRDD.values.keys.first().head
    Assert.assertEquals(Some(DimensionalType.P), startMeasure8.dimensionalType)
    Assert.assertEquals(Some(DimensionalType.P), endMeasure8.dimensionalType)

  }
  //TODO da rivedere formula 4
  /*def testGetCoherentSegmentsRDDIm1Igmg(): Unit = {
    val dcc = new DimensionalCoherenceController()
    val pdr = "1"
    val tglWithConvertedValued = Tgl(service = "TML", pdr = pdr, date = None, readType = None, measure = Some(2.0), isValid = None,
      converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None)


    val pre = Im1Pre(service = "IM1PRE", pdr = "1", date = None, readType = None, measure = Some(2.0), converted = Some(1.0),
      serialNumberMis = None, serialNumberConv = None, coefCorr = Some(5.0), cau_int_mis = None,
      cau_int_cor = None, localFile = None, pivaDistr = None, pivaUtente = None, dataCaricamento = None)
    val post = Im1Post(service = "IM1POST", pdr = "1", date = None, readType = None, measure = Some(2.0), converted = Some(1.0),
      serialNumberMis = None, serialNumberConv = None, coefCorr = Some(5.5), cau_int_mis = None,
      cau_int_cor = None, localFile = None, pivaDistr = None, pivaUtente = None, dataCaricamento = None)

    val measuresWithInfoIm1 =
        FlowWithInfo(
          flow = Im1(service = "IM1", pdr = "1", date = None, readType = None, measure = None,
            converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
            cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
            pre = pre,
            post = post,
            sameDayFlow = Option(tglWithConvertedValued)
          ),
          rcuGasTech = None,
          dimensionalType = Some(DimensionalType.C)
        )

    val dummyFlowWithInfo = FlowWithInfo(flow = tglWithConvertedValued, dimensionalType = Some(DimensionalType.C))
    val dummySegmentRDD = createRDD(pdr, dummyFlowWithInfo, measuresWithInfoIm1)
    val coherentDimTypeSegmentRDD = dcc.getCoherentSegmentsRDD(dummySegmentRDD).cache()
    val (startMeasure, endMeasure) = coherentDimTypeSegmentRDD.values.keys.first().head

    Assert.assertEquals(Some(DimensionalType.C), startMeasure.dimensionalType)
    Assert.assertEquals(Some(DimensionalType.C), endMeasure.dimensionalType)

    val dummyFlowWithInfo2 = FlowWithInfo(flow = tglWithConvertedValued.copy(converted = None), dimensionalType = Some(DimensionalType.C))
    val dummySegmentRDD2 = createRDD(pdr, dummyFlowWithInfo2, measuresWithInfoIm1)
    val coherentDimTypeSegmentRDD2 = dcc.getCoherentSegmentsRDD(dummySegmentRDD2).cache()
    val (startMeasure2, endMeasure2) = coherentDimTypeSegmentRDD2.values.keys.first().head

    Assert.assertEquals(Some(DimensionalType.H), startMeasure2.dimensionalType)
    Assert.assertEquals(Some(DimensionalType.H), endMeasure2.dimensionalType)

    val measuresWithInfoIm13 =
      FlowWithInfo(
        flow = Im1(service = "IM1", pdr = "1", date = None, readType = None, measure = None,
          converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
          pre = pre,
          post = post.copy(converted = None),
          sameDayFlow = Option(tglWithConvertedValued)
        ),
        rcuGasTech = None,
        dimensionalType = Some(DimensionalType.C)

      )
    val dummyFlowWithInfo3 = FlowWithInfo(flow = tglWithConvertedValued.copy(converted = None), dimensionalType = Some(DimensionalType.C))
    val dummySegmentRDD3 = createRDD(pdr, dummyFlowWithInfo3, measuresWithInfoIm13)
    val coherentDimTypeSegmentRDD3 = dcc.getCoherentSegmentsRDD(dummySegmentRDD3).cache()
    val (startMeasure3, endMeasure3) = coherentDimTypeSegmentRDD3.values.keys.first().head

    Assert.assertEquals(Some(DimensionalType.PK), startMeasure3.dimensionalType)
    Assert.assertEquals(Some(DimensionalType.PK), endMeasure3.dimensionalType)

    val dummyFlowWithInfo4 = FlowWithInfo(flow = tglWithConvertedValued, dimensionalType = Some(DimensionalType.C))
    val dummySegmentRDD4 = createRDD(pdr, dummyFlowWithInfo4, measuresWithInfoIm13)
    val coherentDimTypeSegmentRDD4 = dcc.getCoherentSegmentsRDD(dummySegmentRDD4).cache()
    val (startMeasure4, endMeasure4) = coherentDimTypeSegmentRDD4.values.keys.first().head

    Assert.assertEquals(Some(DimensionalType.H), startMeasure4.dimensionalType)
    Assert.assertEquals(Some(DimensionalType.H), endMeasure4.dimensionalType)
  }
*/
  def testGetCoherentSegmentsWithNullSegments(): Unit = {
    val dcc = new DimensionalCoherenceController()
    val noSegmentsRDD = Environment.getSpark.sparkContext.parallelize(List(
      ("pdr", (List[(FlowWithInfo, FlowWithInfo)](), ExternalDailyInfo()))
    ))

    val result = dcc.getCoherentSegmentsRDD(noSegmentsRDD)

    Assert.assertTrue(result.filter({case(k,_)=> k.equals("pdr")}).first()._2._1.isEmpty)
  }

  def createRDD(pdr:String, start: FlowWithInfo, end: FlowWithInfo): RDD[(String, (List[(FlowWithInfo, FlowWithInfo)], ExternalDailyInfo))] = {
    Environment.getSpark.sparkContext.parallelize(
      List(
        (pdr, (List((start, end)), ExternalDailyInfo(None, None)))
      )
    )
  }

  def testHandleMot4andMot5DimType(): Unit = {
    val pdr = "1"
    val date = Some(DateTime.parse("01/01/2020", DateTimeFormat.forPattern("dd/MM/yyyy")))
    val tgl = FlowWithInfo(
      Tgl(service = "TML", pdr = pdr, date = date, readType = None, measure = Some(2.0), isValid = None,
      converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None)
    )
    val rgl =  FlowWithInfo(
      Rgl(service = "RGL", pdr = pdr, date = date.map(_.plusDays(1)), measure = Some(2.0), converted = Some(1.0),
      serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None, motivation = Some(4)).setDimTypeForced(ForcingFlags.FB)
    )
    val rml =  FlowWithInfo(
      Rml(service = "RML", pdr = pdr, date = date.map(_.plusDays(2)), measure = Some(2.0), converted = Some(1.0),
        serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
        dataCaricamento = None, motivation = Some(5),  readType = None, freqLet = None, tipoRettifica = None)
        .setDimTypeForced(ForcingFlags.FB)
    )
    val tgl2 = FlowWithInfo(
      Tgl(service = "TML", pdr = pdr, date = date.map(_.plusDays(3)), readType = None, measure = Some(2.0), isValid = None,
        converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
        dataCaricamento = None)
    )
    val segments = List((tgl, rgl),(rgl, rml),(rml, tgl2)).map(DimensionalCoherenceController.handleMot4andMot5DimType)

    segments.map(_._1).filter(_.flow.isInstanceOf[Tgl]).foreach(fwi => Assert.assertTrue(fwi.dimensionalType.isEmpty))
    segments.map(_._2).filter(_.flow.isInstanceOf[Tgl]).foreach(fwi => Assert.assertTrue(fwi.dimensionalType.isEmpty))

    segments.map(_._1).filter(fwi => fwi.flow.isInstanceOf[Rml] || fwi.flow.isInstanceOf[Rgl]).foreach(fwi => Assert.assertEquals(Some(DimensionalType.C), fwi.dimensionalType))
    segments.map(_._2).filter(fwi => fwi.flow.isInstanceOf[Rml] || fwi.flow.isInstanceOf[Rgl]).foreach(fwi => Assert.assertEquals(Some(DimensionalType.C), fwi.dimensionalType))
  }

//  def testIgmg(): Unit = {
//    val dcc = new DimensionalCoherenceController()
//
//    val format = DateTimeFormat.forPattern("dd/MM/yyyy")
//    val dateTgl =  Option(DateTime.parse("15/12/2021", format))
//
//    val mTreatmentTgl = MonthTreatment(pdr = "1", month = "202112", treatment = Treatment.M.toString, calcmode = "", autofilled = false)
//    val rcuGasTech = Some(RcuGasTech(nIdPdr = "1", startDateTech = dateTgl.get, endDateTech = dateTgl.get))
//    val dummyFlowWithInfoIgmg = FlowWithInfo(
//      flow = Tgl(service = "TGL", pdr = "1", date = dateTgl, readType = None, measure = Some(2.0), isValid = None,
//        converted = Some(1.0), serialNumberMis = Option("a1"), serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
//        dataCaricamento = None),
//      dimensionalType = Some(DimensionalType.PK),
//      monthTreatment = Some(mTreatmentTgl),
//      rcuGasTech = rcuGasTech
//    )
//
//    val dateIgmg =  Option(DateTime.parse("18/01/2022", format))
//    val mTreatmentIgmg = MonthTreatment(pdr = "1", month = "202201", treatment = Treatment.M.toString, calcmode = "", autofilled = false)
//
//    val Im1WithInfoAndSameDayMeasureIgmg = FlowWithInfo(
//      flow = Igmg(service = "IGMG", pdr = "1", date = dateIgmg, readType = None, measure = None,
//        converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = Some(1),
//        cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
//        pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = dateIgmg, readType = None, measure = Some(4.0),
//          converted = Some(0.90), serialNumberMis = Option("a1"), serialNumberConv = None, coefCorr = None, cau_int_mis = Some(1),
//          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
//        post = IgmgPost(service = "IGMGPRE", pdr = "1", date = dateIgmg, readType = None, measure = Some(4.0),
//          converted = Some(41.0), serialNumberMis = Option("a1"), serialNumberConv = None, coefCorr = None, cau_int_mis = Some(1),
//          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
//        sameDayFlow = Some(Tgl(service = "TGL", pdr = "1", date = dateIgmg, readType = None, measure = Some(2.0), isValid = None,
//          converted = Some(1.0), serialNumberMis = Option("a1"), serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
//          dataCaricamento = None))
//      ),
//      dimensionalType = Some(DimensionalType.C),
//      monthTreatment = Some(mTreatmentIgmg),
//      rcuGasTech = rcuGasTech
//    )
//
//    var dummySegmentRDD = createRDD("1", dummyFlowWithInfoIgmg, Im1WithInfoAndSameDayMeasureIgmg)
//    var coherentDimTypeSegmentRDD = dcc.getCoherentSegmentsRDD(dummySegmentRDD)
//    val (startMeasure, endMeasure) = coherentDimTypeSegmentRDD.values.keys.first().head
//
//    val x = 1
//  }
}
