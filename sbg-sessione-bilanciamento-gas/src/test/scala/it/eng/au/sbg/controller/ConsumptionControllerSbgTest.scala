package it.eng.au.sbg.controller

import it.eng.au.aggiustamentoGas.controller.ConsumptionController
import it.eng.au.aggiustamentoGas.model.agg.{FlowWithInfo, MonthTreatment}
import it.eng.au.aggiustamentoGas.model.measure.{Rgl, Tgl}
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Igmg, IgmgPost, IgmgPre, Im1, Im1Igmg, Im1Post, Im1Pre, Post, Pre}
import it.eng.au.aggiustamentoGas.utility.constants.{DimensionalType, Treatment}
import it.eng.au.sbg.EnvironmentSparkTest
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class ConsumptionControllerSbgTest extends EnvironmentSparkTest{
  def testIm1IgmgPrePostSplit(): Unit = {
    val consumptionController = new ConsumptionControllerSbg

    val date = Some(DateTime.parse("1/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy")))
    val monthTreatment = Some(MonthTreatment(pdr = "", treatment = Treatment.G.toString, month = "", calcmode = "", autofilled = true))

    val tgldate01 = FlowWithInfo( flow = Tgl(service = "tgl", pdr = "1", date = date.map(_.minusDays(1)), readType = None, measure = Some(2.0), isValid = None,
      converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None),
      dimensionalType = Some(DimensionalType.P),
      monthTreatment = monthTreatment
    )

    val tgldate0 = FlowWithInfo( flow = Tgl(service = "tgl", pdr = "1", date = date, readType = None, measure = Some(2.0), isValid = None,
      converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None),
      dimensionalType = Some(DimensionalType.P),
      monthTreatment = monthTreatment
    )

    val igmgdate1 = FlowWithInfo(
      flow = Igmg(service = "IGMG", pdr = "1", date = date.map(_.plusDays(1)), readType = None, measure = None,
        converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
        cau_int_cor = Some(2), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
        pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = date.map(_.plusDays(1)), readType = None, measure = None,
          converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = Some(2), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
        post = IgmgPost(service = "IGMGPOST", pdr = "1", date = date.map(_.plusDays(1)), readType = None, measure = None,
          converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = Some(2), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
          correctionFlow = None, isCorrected = false),
        sameDayFlow = Some(Tgl(service = "tgl", pdr = "1", date = date.map(_.plusDays(1)), readType = None, measure = Some(2.0), isValid = None,
          converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
          dataCaricamento = None))
      ),
      dimensionalType = Some(DimensionalType.P),
      monthTreatment = monthTreatment
    )

    val igmgdate2 = FlowWithInfo(
      flow = Igmg(service = "IGMG", pdr = "1", date = date.map(_.plusDays(2)), readType = None, measure = None,
        converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
        cau_int_cor = Some(3), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
        pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = date.map(_.plusDays(2)), readType = None, measure = None,
          converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = Some(3), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
        post = IgmgPost(service = "IGMGPOST", pdr = "1", date = date.map(_.plusDays(2)), readType = None, measure = None,
          converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = Some(3), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
          correctionFlow = None, isCorrected = false),
        sameDayFlow = Some(Tgl(service = "tgl", pdr = "1", date = date.map(_.plusDays(2)), readType = None, measure = Some(2.0), isValid = None,
          converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
          dataCaricamento = None))
      ),
      dimensionalType = Some(DimensionalType.P),
      monthTreatment = monthTreatment
    )

    val tgldate3 = FlowWithInfo( flow = Tgl(service = "tgl", pdr = "1", date = date.map(_.plusDays(3)), readType = None, measure = Some(2.0), isValid = None,
      converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None),
      dimensionalType = Some(DimensionalType.P),
      monthTreatment = monthTreatment
    )

    val segments = List((tgldate01, tgldate0), (tgldate0, igmgdate1), (igmgdate1, igmgdate2), (igmgdate2, tgldate3))
    val resultsPrime = consumptionController.splitIm1Igmg2PrePost(segments, 0)

    resultsPrime.foreach(println)
    resultsPrime.foreach(x => {
      println(x._1)
      println(x._2)
    })

    println("sep")
    val tgldate2 = FlowWithInfo( flow = Tgl(service = "tgl", pdr = "1", date = date.map(_.plusDays(2)), readType = None, measure = Some(2.0), isValid = None,
      converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None),
      dimensionalType = Some(DimensionalType.P),
      monthTreatment = monthTreatment
    )
    val segments2 = List((tgldate01, tgldate0), (tgldate0, igmgdate1), (igmgdate1, tgldate2))
    val resultsPrime2 = consumptionController.splitIm1Igmg2PrePost(segments2, 0)
    resultsPrime2.foreach(println)
    resultsPrime2.foreach(x => {
      println(x._1)
      println(x._2)
    })
    println("sep")

    val igmgdate1iscorrettedfalsewithoutsamedayflows = FlowWithInfo(
      flow = Igmg(service = "IGMG", pdr = "1", date = date.map(_.plusDays(1)), readType = None, measure = None,
        converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
        cau_int_cor = Some(2), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
        pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = date.map(_.plusDays(1)), readType = None, measure = None,
          converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = Some(2), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
        post = IgmgPost(service = "IGMGPOST", pdr = "1", date = date.map(_.plusDays(1)), readType = None, measure = None,
          converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = Some(2), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
          correctionFlow = None, isCorrected = false)

      ),
      dimensionalType = Some(DimensionalType.P),
      monthTreatment = monthTreatment
    )

    val segments3 = List((tgldate0, igmgdate1iscorrettedfalsewithoutsamedayflows), (igmgdate1iscorrettedfalsewithoutsamedayflows, tgldate2))
    val resultsPrime3 = consumptionController.splitIm1Igmg2PrePost(segments3, 0)
    resultsPrime3.foreach(println)
    resultsPrime3.foreach(x => {
      println(x._1)
      println(x._2)
    })
    println("sep")

  }
}
