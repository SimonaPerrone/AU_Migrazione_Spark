package it.eng.au.aggiustamentoGas.model.agg

import it.eng.au.aggiustamentoGas.utility.constants.DimensionalType
import it.eng.au.aggiustamentoGas.controller.{CoefficientController, ConsumptionController}
import it.eng.au.aggiustamentoGas.model.measure.Flow
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg._
import it.eng.au.aggiustamentoGas.model.measure.measureTypes.{MisuraFlow, RettificaFlow}
import it.eng.au.aggiustamentoGas.utility.constants.FieldConstants.{G_FORMULA_ID, IM1IGMG_SPECIAL_FORMULA_ID}
import it.eng.au.aggiustamentoGas.utility.constants.OptionMath._
import org.joda.time.DateTime

/** Modella i consumi calcolati dalla procedura */
case class Consumption(
                        value: Option[Double],
                        startMeasure: FlowWithInfo,
                        endMeasure: FlowWithInfo,
                        pdr: String,
                        coefficient: Option[Double] = None,
                        pprof: Option[Double] = None,
                        date: DateTime,
                        ca: Option[Double] = None, //default to none since they're specific for each case
                        idRegClim: Option[Int] = None,
                        codProfStd: Option[String] = None,
                        idFormula: Int,
                        errorCode: Array[ErrorEnum.Value] = Array(ErrorEnum.NO_ERROR_CODE),
                        segnante: Option[String] = None, //field used according to flowWithInfo.dimensionalType + dimCoherence algorithm
                        isPdrSuspended: Boolean = false,
                        valueNotSterilized: Option[Double] = None,
                        valueF3: Option[Double] = None, //only need SBG
                        dateStartF2: Option[String] = None,
                        dateEndF2: Option[String] = None
                      )

object Consumption {
  /**
   * This method create a valid consumption iff startMeasure and endMeasure are consequent with one day of difference between
   * each others. Otherwise the measure needs to be normalized, multiplied for a coefficient and must have Consumption.data
   * modified accordingly.<br>
   * <b>To use this method we assume the dimensional coherence is already verified</b>
   *
   * @param startMeasure left side of the segment
   * @param endMeasure   right side of the segment
   * @param idFormula    id of the formula we are using
   * @return a Consumption
   */
  def createDifferenceOnlyConsumption(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo, idFormula: Int): Consumption = {

    val areDimTypeCoherent = startMeasure.dimensionalType.equals(endMeasure.dimensionalType)
    val convertedCondition = areDimTypeCoherent && startMeasure.dimensionalType.equals(Some(DimensionalType.C))
    //&& List(Some(DimensionalType.C), Some(DimensionalType.H)).contains(startMeasure.dimensionalType)
    val prelCondition = areDimTypeCoherent && startMeasure.dimensionalType.equals(Some(DimensionalType.P))

    val k: Double = (startMeasure.flow, endMeasure.flow) match {
      case (post: Post, _) => post.coefCorr.getOrElse(CoefficientController.COEFFICIENT_DEFAULT)
      case (_, pre: Pre) => pre.coefCorr.getOrElse(CoefficientController.COEFFICIENT_DEFAULT)
      case _ => (endMeasure.im1IgmgCoeff.isDefined, startMeasure.im1IgmgCoeff.isDefined, endMeasure.coeff.isDefined, startMeasure.coeff.isDefined) match {
        case (true, _, _, _) => endMeasure.im1IgmgCoeff.getOrElse(CoefficientController.COEFFICIENT_DEFAULT)
        case (_, true, _, _) => startMeasure.im1IgmgCoeff.getOrElse(CoefficientController.COEFFICIENT_DEFAULT)
        case (_, _, true, _) => endMeasure.coeff.getOrElse(CoefficientController.COEFFICIENT_DEFAULT)
        case (_, _, _, true) => startMeasure.coeff.getOrElse(CoefficientController.COEFFICIENT_DEFAULT)
        case (_, _, _, _) => CoefficientController.COEFFICIENT_DEFAULT
      }
    }
    var formula4Flag = false
    var segmentIgmg1ADimensionalTypeToUse: Option[DimensionalType.Value] = None
    var segmentIgmg1BDimensionalTypeToUse: Option[DimensionalType.Value] = None
    var segmentIgmr1ADimensionalTypeToUse: Option[DimensionalType.Value] = None
    var segmentIgmr1BDimensionalTypeToUse: Option[DimensionalType.Value] = None

    val preCoefCorr = endMeasure.flow match {
      case im1Igmg: Im1Igmg => im1Igmg.pre.coefCorr.getOrElse(CoefficientController.COEFFICIENT_DEFAULT)
      case _ => CoefficientController.COEFFICIENT_DEFAULT
    }

    val postCoefCorr = endMeasure.flow match {
      case im1Igmg: Im1Igmg => im1Igmg.post.coefCorr.getOrElse(CoefficientController.COEFFICIENT_DEFAULT)
      case _ => CoefficientController.COEFFICIENT_DEFAULT
    }
    /**
     * DUE TO CR from email of 16/06/2021 - 15:56 - "R: formula 4 anticipatoria" IT IS NO MORE POSSIBLE TO HAVE THE
     * SEPARATION OF CONCERNS PRINCIPLE PUT IN PRACTICE IN THIS PROJECT. INDEED, WE MUST PERFORM FORCING LOGIC ON
     * THE TRIPLE (M_{k-1},M_{k}, IGMG_{k}) IN THE PHASE OF CONSUMPTIONS CREATION BECAUSE ONLY HERE IT IS POSSIBLE TO
     * SPLIT IGMG AND IGMG.sameDayFlow AND CREATE SEGMENT-1A AND SEGMENT-1B AS SPECIFIED IN "Proposta-F4_v.2.0.xlsx"
     * */
    val difference = (startMeasure.flow, endMeasure.flow) match {
      //IM1 forcing
      case (s: RettificaFlow, e: Im1) if (e.cau_int_cor.equals(Some(3)) || e.cau_int_cor.equals(Some(4))) /*&& (e.pre.isCorrected || e.post.isCorrected)*/ =>
        formula4Flag = true
        val segment1A = getConvertedOrElsePKDifference(s, e.pre, preCoefCorr) //PRE - M_{k-1}    C or else PK
        val segment1B = if (e.post.isCorrected) getConvertedOrElsePKDifference(e.post, e.sameDayFlow.get, postCoefCorr) else getPKDifference(e.post, e.sameDayFlow.get, postCoefCorr) // M_{k} - POST   PK
        sum(segment1A._1, segment1B._1)

      case (s: MisuraFlow, e: Im1) if e.cau_int_cor.equals(Some(3)) || e.cau_int_cor.equals(Some(4)) =>
        formula4Flag = true
        val segment1A = getConvertedOrElsePKDifference(s, e.pre, preCoefCorr) //PRE - M_{k-1}    C or else PK
        val segment1B = getPKDifference(e.post, e.sameDayFlow.get, postCoefCorr) // M_{k} - POST   PK
        sum(segment1A._1, segment1B._1)

      case (s: RettificaFlow, e: Im1) if e.cau_int_cor.equals(Some(5)) /*&& (e.pre.isCorrected || e.post.isCorrected)*/ =>
        formula4Flag = true
        val segment1A = if (e.pre.isCorrected) getConvertedOrElsePKDifference(s, e.pre, preCoefCorr) else getPKDifference(s, e.pre, preCoefCorr) //PRE - M_{k-1}    PK
        val segment1B = getConvertedOrElsePKDifference(e.post, e.sameDayFlow.get, postCoefCorr) // M_{k} - POST  C or else PK
        sum(segment1A._1, segment1B._1)
      case (s: MisuraFlow, e: Im1) if e.cau_int_cor.equals(Some(5)) =>
        formula4Flag = true
        val segment1A = getPKDifference(s, e.pre, preCoefCorr) //PRE - M_{k-1}    PK
        val segment1B = getConvertedOrElsePKDifference(e.post, e.sameDayFlow.get, postCoefCorr) // M_{k} - POST  C or else PK
        sum(segment1A._1, segment1B._1)

      //IGMG forcing
      case (s: RettificaFlow, e: Igmg) if e.cau_int_cor.equals(Some(2)) /*&& (e.pre.isCorrected || e.post.isCorrected)*/ =>
        formula4Flag = true
        val segment1A = getConvertedOrElsePKDifference(s, e.pre, preCoefCorr) //PRE - M_{k-1}    C or else PK
        val segment1B = if (e.post.isCorrected) getConvertedOrElsePKDifference(e.post, e.sameDayFlow.get, postCoefCorr) else getPKDifference(e.post, e.sameDayFlow.get, postCoefCorr) // M_{k} - POST   PK
        segmentIgmg1ADimensionalTypeToUse = segment1A._2
        segmentIgmg1BDimensionalTypeToUse = segment1B._2
        sum(segment1A._1, segment1B._1)
      case (s: MisuraFlow, e: Igmg) if e.cau_int_cor.equals(Some(2)) =>
        formula4Flag = true
        val segment1A = getConvertedOrElsePKDifference(s, e.pre, preCoefCorr) //PRE - M_{k-1}    C or else PK
        val segment1B = getPKDifference(e.post, e.sameDayFlow.get, postCoefCorr) // M_{k} - POST   PK
        segmentIgmg1ADimensionalTypeToUse = segment1A._2
        segmentIgmg1BDimensionalTypeToUse = segment1B._2
        sum(segment1A._1, segment1B._1)

      case (s: RettificaFlow, e: Igmg) if e.cau_int_cor.equals(Some(3)) || e.cau_int_cor.equals(Some(4)) /* && (e.pre.isCorrected || e.post.isCorrected)*/ =>
        formula4Flag = true
        val segment1A = if (e.pre.isCorrected) getConvertedOrElsePKDifference(s, e.pre, preCoefCorr) else getPKDifference(s, e.pre, preCoefCorr) //PRE - M_{k-1}    PK
        val segment1B = getConvertedOrElsePKDifference(e.post, e.sameDayFlow.get, postCoefCorr) // M_{k} - POST  C or else PK
        segmentIgmg1ADimensionalTypeToUse = segment1A._2
        segmentIgmg1BDimensionalTypeToUse = segment1B._2
        sum(segment1A._1, segment1B._1)
      case (s: MisuraFlow, e: Igmg) if e.cau_int_cor.equals(Some(3)) || e.cau_int_cor.equals(Some(4)) =>
        formula4Flag = true
        val segment1A = getPKDifference(s, e.pre, preCoefCorr) //PRE - M_{k-1}    PK
        val segment1B = getConvertedOrElsePKDifference(e.post, e.sameDayFlow.get, postCoefCorr) // M_{k} - POST  C or else PK
        segmentIgmg1ADimensionalTypeToUse = segment1A._2
        segmentIgmg1BDimensionalTypeToUse = segment1B._2
        sum(segment1A._1, segment1B._1)

     /* //IGMR forcing
      case (s: RettificaFlow, e: Igmr) if e.cau_int_cor.equals(Some(4)) =>
        formula4Flag = true
        val segment1A = getConvertedOrElsePKDifference(s, e.pre, preCoefCorr) //PRE - M_{k-1}    C or else PK
        val segment1B = if (e.post.isCorrected) getConvertedOrElsePKDifference(e.post, e.sameDayFlow.get, postCoefCorr) else getPKDifference(e.post, e.sameDayFlow.get, postCoefCorr) // M_{k} - POST   PK
        segmentIgmr1ADimensionalTypeToUse = segment1A._2
        segmentIgmr1BDimensionalTypeToUse = segment1B._2
        sum(segment1A._1, segment1B._1)
      case (s: MisuraFlow, e: Igmr) if e.cau_int_cor.equals(Some(4)) =>
        formula4Flag = true
        val segment1A = getConvertedOrElsePKDifference(s, e.pre, preCoefCorr) //PRE - M_{k-1}    C or else PK
        val segment1B = getPKDifference(e.post, e.sameDayFlow.get, postCoefCorr) // M_{k} - POST   PK
        segmentIgmr1ADimensionalTypeToUse = segment1A._2
        segmentIgmr1BDimensionalTypeToUse = segment1B._2
        sum(segment1A._1, segment1B._1)

      case (s: RettificaFlow, e: Igmr) if e.cau_int_cor.equals(Some(5)) =>
        formula4Flag = true
        val segment1A = if (e.pre.isCorrected) getConvertedOrElsePKDifference(s, e.pre, preCoefCorr) else getPKDifference(s, e.pre, preCoefCorr) //PRE - M_{k-1}    PK
        val segment1B = getConvertedOrElsePKDifference(e.post, e.sameDayFlow.get, postCoefCorr) // M_{k} - POST  C or else PK
        segmentIgmr1ADimensionalTypeToUse = segment1A._2
        segmentIgmr1BDimensionalTypeToUse = segment1B._2
        sum(segment1A._1, segment1B._1)
      case (s: MisuraFlow, e: Igmr) if e.cau_int_cor.equals(Some(5)) =>
        formula4Flag = true
        val segment1A = getPKDifference(s, e.pre, preCoefCorr) //PRE - M_{k-1}    PK
        val segment1B = getConvertedOrElsePKDifference(e.post, e.sameDayFlow.get, postCoefCorr) // M_{k} - POST  C or else PK
        segmentIgmr1ADimensionalTypeToUse = segment1A._2
        segmentIgmr1BDimensionalTypeToUse = segment1B._2
        sum(segment1A._1, segment1B._1)*/

      //Standard Cases
      case (s: Flow, e: Im1Igmg) if convertedCondition =>
        formula4Flag = true

        /**
         * CR - Gabrini Federico - 16/12/2021 - add hybrid colcualtion mode to igmg im1
         */
        //TODO da rivedere formula 4
        //        if (endMeasure.dimensionalType.equals(Some(DimensionalType.H))) {
        //          val segment1A = getConvertedOrElseMesure(s, e.pre)
        //          val segment1B = getConvertedOrElseMesure(e.post, e.sameDayFlow.get)
        //          sum(segment1A, segment1B)
        //        }
        //        else sum(diff(e.pre.converted, s.converted), diff(e.sameDayFlow.flatMap(_.converted), e.post.converted))
        if (endMeasure.flow.isInstanceOf[Igmg]) segmentIgmg1ADimensionalTypeToUse = Option(DimensionalType.C)
        if (endMeasure.flow.isInstanceOf[Igmg]) segmentIgmg1BDimensionalTypeToUse = Option(DimensionalType.C)
        if (endMeasure.flow.isInstanceOf[Igmr]) segmentIgmr1ADimensionalTypeToUse = Option(DimensionalType.C)
        if (endMeasure.flow.isInstanceOf[Igmr]) segmentIgmr1BDimensionalTypeToUse = Option(DimensionalType.C)
        sum(diff(e.pre.converted, s.converted), diff(e.sameDayFlow.flatMap(_.converted), e.post.converted))

      case (s: Flow, e: Im1Igmg) if prelCondition =>
        formula4Flag = true
        if (endMeasure.flow.isInstanceOf[Igmg]) segmentIgmg1ADimensionalTypeToUse = Option(DimensionalType.P)
        if (endMeasure.flow.isInstanceOf[Igmg]) segmentIgmg1BDimensionalTypeToUse = Option(DimensionalType.P)
        if (endMeasure.flow.isInstanceOf[Igmr]) segmentIgmr1ADimensionalTypeToUse = Option(DimensionalType.P)
        if (endMeasure.flow.isInstanceOf[Igmr]) segmentIgmr1BDimensionalTypeToUse = Option(DimensionalType.P)
        sum(diff(e.pre.measure, s.measure), diff(e.sameDayFlow.flatMap(_.measure), e.post.measure))
      case (s: Flow, e: Im1Igmg) =>
        formula4Flag = true
        if (endMeasure.flow.isInstanceOf[Igmg]) segmentIgmg1ADimensionalTypeToUse = Option(DimensionalType.PK)
        if (endMeasure.flow.isInstanceOf[Igmg]) segmentIgmg1BDimensionalTypeToUse = Option(DimensionalType.PK)
        if (endMeasure.flow.isInstanceOf[Igmr]) segmentIgmr1ADimensionalTypeToUse = Option(DimensionalType.PK)
        if (endMeasure.flow.isInstanceOf[Igmr]) segmentIgmr1BDimensionalTypeToUse = Option(DimensionalType.PK)
        sum(diff(e.pre.measure, s.measure).map(_ * preCoefCorr), diff(e.sameDayFlow.flatMap(_.measure), e.post.measure).map(_ * postCoefCorr))

      case (_: Flow, _: Flow) if convertedCondition => diff(endMeasure.flow.converted, startMeasure.flow.converted)
      case (_: Flow, _: Flow) if prelCondition => diff(endMeasure.flow.measure, startMeasure.flow.measure)
      case (_: Flow, _: Flow) => diff(endMeasure.flow.measure, startMeasure.flow.measure).map(_ * k)
    }

    //only igmg/igmr control here the mismatch matricole
    if (segmentIgmr1ADimensionalTypeToUse.isDefined &&
      segmentIgmr1BDimensionalTypeToUse.isDefined &&
      !areSerialNumbersCoherentIgmr(startMeasure.flow, endMeasure.flow, segmentIgmr1ADimensionalTypeToUse.get, segmentIgmr1BDimensionalTypeToUse.get)
    ) {
      val c = createNullConsumption(startMeasure, endMeasure, if (formula4Flag) IM1IGMG_SPECIAL_FORMULA_ID else idFormula)
      c.copy(errorCode = c.errorCode ++ Array(ErrorEnum.NON_MATCHING_SERIALS_ERROR_CODE))
    }

    else if (segmentIgmg1ADimensionalTypeToUse.isDefined &&
      segmentIgmg1BDimensionalTypeToUse.isDefined &&
      !areSerialNumbersCoherentIgmg(startMeasure.flow, endMeasure.flow, segmentIgmg1ADimensionalTypeToUse.get, segmentIgmg1BDimensionalTypeToUse.get)
    ) {
      val c = createNullConsumption(startMeasure, endMeasure, if (formula4Flag) IM1IGMG_SPECIAL_FORMULA_ID else idFormula)
      c.copy(errorCode = c.errorCode ++ Array(ErrorEnum.NON_MATCHING_SERIALS_ERROR_CODE))
    }

    else {
      Consumption(
        value = difference,
        coefficient = Some(k),
        startMeasure = startMeasure,
        endMeasure = endMeasure,
        pdr = endMeasure.flow.pdr,
        pprof = None,
        date = endMeasure.flow.date.get,
        idFormula = if (formula4Flag) IM1IGMG_SPECIAL_FORMULA_ID else idFormula,
        errorCode = if (difference.isEmpty) Array(ErrorEnum.MEASURE_NOT_PRESENT_ERROR_CODE) else Array(ErrorEnum.NO_ERROR_CODE),
        segnante = if (segmentIgmr1BDimensionalTypeToUse.isDefined) segmentIgmr1BDimensionalTypeToUse.map(_.toString) else if (segmentIgmg1BDimensionalTypeToUse.isDefined) segmentIgmg1BDimensionalTypeToUse.map(_.toString) else endMeasure.dimensionalType.map(_.toString)
      )
    }
  }

  /** Controlla se i numeri seriali delle due misure Igmg nel segmento sono coerenti */
  private def areSerialNumbersCoherentIgmg(
                                            startMeasure: Flow
                                            , endMeasure: Flow
                                            , segmentIgmg1ADimensionalTypeToUse: DimensionalType.Value
                                            , segmentIgmg1BDimensionalTypeToUse: DimensionalType.Value
                                          ): Boolean = {

    val endMeasureIgmg = endMeasure.asInstanceOf[Igmg]

    val serialNumEqual: (Option[String], Option[String]) => Boolean = {
      case (None, None) => true
      case (None, y: Some[String]) => true
      case (x: Some[String], None) => true
      case (x: Some[String], y: Some[String]) => x == y
    }

    val equalityConditionSegmentIgmg1A = segmentIgmg1ADimensionalTypeToUse match {
      case (DimensionalType.P | DimensionalType.PK) => serialNumEqual(startMeasure.serialNumberMis, endMeasureIgmg.pre.serialNumberMis)
      case DimensionalType.C => serialNumEqual(startMeasure.serialNumberConv, endMeasureIgmg.pre.serialNumberConv)
    }

    val equalityConditionSegmentIgmg1B = segmentIgmg1BDimensionalTypeToUse match {
      case (DimensionalType.P | DimensionalType.PK) => endMeasureIgmg.sameDayFlow.isDefined && serialNumEqual(endMeasureIgmg.sameDayFlow.get.serialNumberMis, endMeasureIgmg.post.serialNumberMis)
      case DimensionalType.C => endMeasureIgmg.sameDayFlow.isDefined && serialNumEqual(endMeasureIgmg.sameDayFlow.get.serialNumberConv, endMeasureIgmg.post.serialNumberConv)
    }

    equalityConditionSegmentIgmg1A && equalityConditionSegmentIgmg1B
  }

  private def areSerialNumbersCoherentIgmr(
                                            startMeasure: Flow
                                            , endMeasure: Flow
                                            , segmentIgmr1ADimensionalTypeToUse: DimensionalType.Value
                                            , segmentIgmr1BDimensionalTypeToUse: DimensionalType.Value
                                          ): Boolean = {

    val endMeasureIgmr = endMeasure.asInstanceOf[Igmr]

    val serialNumEqual: (Option[String], Option[String]) => Boolean = {
      case (None, None) => true
      case (None, y: Some[String]) => true
      case (x: Some[String], None) => true
      case (x: Some[String], y: Some[String]) => x == y
    }

    val equalityConditionSegmentIgmr1A = segmentIgmr1ADimensionalTypeToUse match {
      case (DimensionalType.P | DimensionalType.PK) => serialNumEqual(startMeasure.serialNumberMis, endMeasureIgmr.pre.serialNumberMis)
      case DimensionalType.C => serialNumEqual(startMeasure.serialNumberConv, endMeasureIgmr.pre.serialNumberConv)
    }

    val equalityConditionSegmentIgmr1B = segmentIgmr1BDimensionalTypeToUse match {
      case (DimensionalType.P | DimensionalType.PK) => endMeasureIgmr.sameDayFlow.isDefined && serialNumEqual(endMeasureIgmr.sameDayFlow.get.serialNumberMis, endMeasureIgmr.post.serialNumberMis)
      case DimensionalType.C => endMeasureIgmr.sameDayFlow.isDefined && serialNumEqual(endMeasureIgmr.sameDayFlow.get.serialNumberConv, endMeasureIgmr.post.serialNumberConv)
    }

    equalityConditionSegmentIgmr1A && equalityConditionSegmentIgmr1B
  }

  private def getConvertedOrElsePKDifference(s: Flow, e: Pre, k: Double): (Option[Double], Option[DimensionalType.Value]) = {
    if (s.converted.isDefined && e.converted.isDefined) //C
      (diff(e.converted, s.converted), Option(DimensionalType.C))
    else //PK
      (diff(e.measure, s.measure).map(_ * k), Option(DimensionalType.PK))
  }

  private def getConvertedOrElsePKDifference(s: Post, e: Flow, k: Double): (Option[Double], Option[DimensionalType.Value]) = {
    if (s.converted.isDefined && e.converted.isDefined) //C
      (diff(e.converted, s.converted), Option(DimensionalType.C))
    else //PK
      (diff(e.measure, s.measure).map(_ * k), Option(DimensionalType.PK))
  }

  private def getPKDifference(s: Flow, e: Pre, k: Double): (Option[Double], Option[DimensionalType.Value]) = {
    (diff(e.measure, s.measure).map(_ * k), Option(DimensionalType.PK))
  }

  private def getPKDifference(s: Post, e: Flow, k: Double): (Option[Double], Option[DimensionalType.Value]) = {
    (diff(e.measure, s.measure).map(_ * k), Option(DimensionalType.PK))
  }
  //TODO da rivedere formula 4
  //  private def getConvertedOrElseMesure(s: Flow, e: Pre): Option[Double] = {
  //    if (s.converted.isDefined && e.converted.isDefined)
  //      diff(e.converted, s.converted)
  //    else
  //      diff(e.measure, s.measure)
  //  }
  //  private def getConvertedOrElseMesure(s: Post, e: Flow): Option[Double] = {
  //    if (s.converted.isDefined && e.converted.isDefined)
  //      diff(e.converted, s.converted)
  //    else
  //      diff(e.measure, s.measure)
  //  }


  /**
   * Creates a measure with undefined value
   * */
  def createNullConsumption(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo, idFormula: Int): Consumption = {
    Consumption(
      value = None,
      coefficient = None,
      startMeasure = startMeasure,
      endMeasure = endMeasure,
      pdr = endMeasure.flow.pdr,
      pprof = None,
      date = endMeasure.flow.date.get,
      idFormula = idFormula,
      segnante = None
    )
  }

  /**
   * Creates a measure with undefined value
   * */
  def createNullConsumption(date: DateTime, pdr: String): Consumption = {
    Consumption(
      value = None,
      coefficient = None,
      startMeasure = null,
      endMeasure = null,
      pdr = pdr,
      pprof = None,
      date = date,
      idFormula = 0,
      segnante = None
    )
  }

  def createZeroValuedConsumption(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo, idFormula: Int): Consumption = {
    Consumption(
      value = Some(0),
      coefficient = None,
      startMeasure = startMeasure,
      endMeasure = endMeasure,
      pdr = endMeasure.flow.pdr,
      pprof = None,
      date = endMeasure.flow.date.get,
      idFormula = idFormula,
      segnante = endMeasure.dimensionalType.map(_.toString)
    )
  }
}