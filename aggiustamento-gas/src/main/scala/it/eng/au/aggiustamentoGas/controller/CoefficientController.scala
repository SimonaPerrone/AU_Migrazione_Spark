package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.utility.constants.{DimensionalType, ForcingFlags}
import it.eng.au.aggiustamentoGas.controller.CoefficientController._
import it.eng.au.aggiustamentoGas.model.agg.{ExternalDailyInfo, FlowWithInfo}
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Igmg, Im1, Im1Igmg}
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.util.Try
/*
* NB Im1/Igmg dimType handling is in DimensionalCoherenceController.handlePostSplitIm1IgmgCoefficient
* */
/** Gestisce l'assegnazione alle misure del coefficiente di correzione e del tipo dimensionale. */
class CoefficientController {

  /**
   * Assign to each measure its proper coefficient and determine which filed should be used to compute the consumptions
   * according to AU specs.
   *
   * @param measuresWithInfo the rdd with tech data from RCU.
   * @return an RDD[FlowWithInfo] with the FlowWithInfo.coeff and FlowWithInfo.dimensionalType valued with the
   *         appropriate coefficient according to specs.
   */
  def get(measuresWithInfo: RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))]): RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))] = {
    measuresWithInfo
      .mapValues({ case (orderedFlows, externalDailyInfo) => (attachIm1IgmgCoefficient(orderedFlows), externalDailyInfo) })
      .mapValues({ case (orderedFlows, externalDailyInfo) => (attachDimTypeAndRcuCoefficient(orderedFlows), externalDailyInfo) })
      .mapValues({ case (orderedFlows, externalDailyInfo) => (orderedFlows.map(flow => flow.copy(coeff = sanitizeCoefficient(flow.coeff))), externalDailyInfo) })
  }
}

object CoefficientController {
  /** default coefficient value */
  lazy val COEFFICIENT_DEFAULT: Double = 1.0
  /** coefficient maximum allowed value */
  lazy val COEFFICIENT_MAX: Double = 30.0

  /**
   * Ensuring rcu coefficient respect all the constraints otherwise returning the default value
   * */
  val sanitizeCoefficient: Option[Double] => Option[Double] = coeff => {
    if (coeff.isDefined && coeff.get <= COEFFICIENT_MAX) {
      coeff
    } else {
      Some(COEFFICIENT_DEFAULT)
    }
  }
  /** Telling whether or not the ordered list of flows has an im1 that requires forcing the coefficient for
   * past and/or future measures
   */
  val im1ForcingCaseExpression: FlowWithInfo => Boolean = fwi => {
    fwi.flow.isInstanceOf[Im1] && (fwi.flow.asInstanceOf[Im1].cau_int_cor == Some(3) || fwi.flow.asInstanceOf[Im1].cau_int_cor == Some(5))
  }
  /** Telling whether or not the ordered list of flows has an igmg that requires forcing the coefficient for
   * past and/or future measures
   */
  val igmgForcingCaseExpression: FlowWithInfo => Boolean = fwi => {
    fwi.flow.isInstanceOf[Igmg] &&
      (fwi.flow.asInstanceOf[Igmg].cau_int_cor == Some(4)
        || fwi.flow.asInstanceOf[Igmg].cau_int_cor == Some(2)
        || fwi.flow.asInstanceOf[Igmg].cau_int_cor == Some(3))
  }

  /** Getting rcu coefficient or default value */
  val getRcuCoeffcient: FlowWithInfo => Option[Double] = flow => {
    Some(Try(flow.rcuGasTech.get.nCoeffCorr.getOrElse(COEFFICIENT_DEFAULT)).getOrElse(COEFFICIENT_DEFAULT))
  }
  /** Labeling the measure with the filed to use to compute the consumption */
  val getDimensionalType: FlowWithInfo => DimensionalType.Value = flowWithInfo => {
    val defaultExpression = if (flowWithInfo.flow.converted.isDefined) DimensionalType.C else DimensionalType.PK

    if (flowWithInfo.rcuGasTech.isEmpty) {
      defaultExpression
    }
    else {
      val tPreConv = if (flowWithInfo.rcuGasVarConvertitore.isEmpty) Some("NO") else flowWithInfo.rcuGasVarConvertitore.get.tPreConv
      (flowWithInfo.rcuGasTech.get.gruppoMisInt, tPreConv) match {
        case (Some("SI"), Some("SI")) | (Some("SI"), Some("NO")) | (Some("SI"), None) | (Some("S"), Some("NO")) | (None, Some("NO")) => DimensionalType.PK
        case (Some("NO"), Some("SI")) | (Some("N"), Some("SI")) | (None, Some("SI")) => DimensionalType.C
        case (Some("NO"), Some("NO")) | (Some("N"), Some("NO")) => DimensionalType.PK
        case (_, _) => defaultExpression
      }
    }
  }

  /** Labeling each measure with the right coefficient and the right dimensionalType */
  def attachDimTypeAndRcuCoefficient(orderedFlows: List[FlowWithInfo]): List[FlowWithInfo] = {
    val defaultAssignement = orderedFlows
      .map(flowWithInfo => flowWithInfo.copy(coeff = getRcuCoeffcient(flowWithInfo), dimensionalType = Some(getDimensionalType(flowWithInfo))))
      .zipWithIndex
    val sortedForcingCases = (defaultAssignement.filter({ case (flow, index) => igmgForcingCaseExpression(flow) }) ++
      defaultAssignement.filter({ case (flow, index) => im1ForcingCaseExpression(flow) }))
      .sortBy(_._2)
    if (sortedForcingCases.isEmpty) {
      defaultAssignement.map(_._1)
    } else {
      handleIm1IgmgForcingDimensionalType(defaultAssignement.map(_._1), sortedForcingCases)
    }
  }

  val cauIntCorDifferentFrom3And2: FlowWithInfo => Boolean = im1WithInfo => {
    (im1WithInfo.flow.isInstanceOf[Im1] && im1WithInfo.flow.asInstanceOf[Im1].cau_int_cor != Some(3)) ||
      (im1WithInfo.flow.isInstanceOf[Igmg] && im1WithInfo.flow.asInstanceOf[Igmg].cau_int_cor != Some(2))
  }
  val cauIntCorDifferentFrom5And4: FlowWithInfo => Boolean = im1WithInfo => {
    (im1WithInfo.flow.isInstanceOf[Im1] && im1WithInfo.flow.asInstanceOf[Im1].cau_int_cor != Some(5)) ||
      (im1WithInfo.flow.isInstanceOf[Igmg] && im1WithInfo.flow.asInstanceOf[Igmg].cau_int_cor != Some(4))
  }
  val cauIntCorDifferentFrom3: FlowWithInfo => Boolean = igmgWithInfo => {
    igmgWithInfo.flow.isInstanceOf[Igmg] && igmgWithInfo.flow.asInstanceOf[Igmg].cau_int_cor != Some(3)
  }

  /**
   * From specs:<ul>
   * <li> For the whole period before IM1 with  cau_int_corr=3 must be considered let_tot_conv if available. For the whole
   * period after IM1 with  cau_int_corr=3 we must consider let_tot_prel*K untill a new Im1 with cau_int_corr
   * different from 3 is met.</li>
   *
   * <li> For the whole period before IM1 with  cau_int_corr=5 (until an IM1 with cau_int_corr!=5), force
   * let_tot_prel*k, for the period after IM1 with  cau_int_corr=5 do as usual </li>
   * </ul>
   * <br>
   * Precondition of this method are: <ul>
   * <li>im1 has only cou_int_coor = 3 or 5, igmg has only cou_int_coor = 2 or 4. </li>
   * <li>It is invoked only for im1 or igmg special cases.</li>
   * <li>im1igmgListWithIndex contains all im1 or all igmg, can not be an hybrid list</li>
   * </ul>
   * */
  @tailrec
  def handleIm1IgmgForcingDimensionalType(orderedFlows: List[FlowWithInfo], im1igmgListWithIndex: List[(FlowWithInfo, Int)]): List[FlowWithInfo] = {

    if (im1igmgListWithIndex.isEmpty) { //base case
      orderedFlows
    } else {
      val (im1IgmgSpecialCase, index) = im1igmgListWithIndex.head

      val partiallyForcedFlowList = im1IgmgSpecialCase.flow match {
        case i: Im1 if i.cau_int_cor.isDefined && i.cau_int_cor.get == 3 =>
          //get index of the most recent measure that respect the condition
          force(stopForcingCondition = cauIntCorDifferentFrom3And2,
            leftValueToForce = DimensionalType.C,
            rightValueToForce = DimensionalType.PK,
            orderedFlows = orderedFlows,
            im1IgmgIndex = index)
        case i: Im1 if i.cau_int_cor.isDefined && i.cau_int_cor.get == 5 =>
          force(stopForcingCondition = cauIntCorDifferentFrom5And4,
            leftValueToForce = DimensionalType.PK,
            rightValueToForce = DimensionalType.C,
            orderedFlows = orderedFlows,
            im1IgmgIndex = index)
        case i: Igmg if i.cau_int_cor.isDefined && i.cau_int_cor.get == 2 =>
          force(stopForcingCondition = cauIntCorDifferentFrom3And2,
          leftValueToForce = DimensionalType.C,
          rightValueToForce = DimensionalType.PK,
          orderedFlows = orderedFlows,
          im1IgmgIndex = index)
        case i: Igmg if i.cau_int_cor.isDefined && i.cau_int_cor.get == 3 =>
          force(stopForcingCondition = cauIntCorDifferentFrom3,
            leftValueToForce = DimensionalType.PK,
            rightValueToForce = DimensionalType.C,
            orderedFlows = orderedFlows,
            im1IgmgIndex = index)
        case i: Igmg if i.cau_int_cor.isDefined && i.cau_int_cor.get == 4 =>
          force(stopForcingCondition = cauIntCorDifferentFrom5And4,
            leftValueToForce = DimensionalType.PK,
            rightValueToForce = DimensionalType.C,
            orderedFlows = orderedFlows,
            im1IgmgIndex = index)
        case _ => orderedFlows
      }
      handleIm1IgmgForcingDimensionalType(partiallyForcedFlowList, im1igmgListWithIndex.tail)
    }
  }

  /** Applica la forzatura del tipo dimensionale, seguendo le regole previste da AU. Per maggiore dettagli,
   * vedere metodo [[handleIm1IgmgForcingDimensionalType]] o consultare i documenti tecnici. */
  def force(stopForcingCondition: FlowWithInfo => Boolean
            , leftValueToForce: DimensionalType.Value
            , rightValueToForce: DimensionalType.Value
            , orderedFlows: List[FlowWithInfo]
            , im1IgmgIndex: Int): List[FlowWithInfo] = {
    val leftMeasureList = orderedFlows.slice(0, im1IgmgIndex)
    //getting the index of the most recent measure that satisfy the condition to stop forcing dim type before the current im1/igmg
    val leftStopIndex = leftMeasureList.reverse.indexWhere(stopForcingCondition) match{ //note: conditions can not be merged otherwise when i=-1 the index is wrong
      case i: Int if i == -1 => 0
      case i: Int => math.max(0, leftMeasureList.size - 1 - i)
    }
    //getting the index of the most recent measure that satisfy the condition to stop forcing dim type after the current im1/igmg
    val rightStopIndex = orderedFlows.indexWhere(stopForcingCondition, im1IgmgIndex + 1) match {
      case i: Int if i == -1 => orderedFlows.size
      case i: Int => i
    }
    //returning a list with forced dim type
    orderedFlows.slice(0, leftStopIndex) ++
      orderedFlows.slice(leftStopIndex, im1IgmgIndex).map(forceDimTypeAndAddForcingFlag(_, leftValueToForce, ForcingFlags.FB)) ++
      orderedFlows.slice(im1IgmgIndex, rightStopIndex).map(forceDimTypeAndAddForcingFlag(_, rightValueToForce, ForcingFlags.FF)) ++
      orderedFlows.slice(rightStopIndex, orderedFlows.size)
  }

  /**
   * Imposta il campo im1Igmg dei flussi precedenti a un Igmg al coefficiente di correzione del post dell'igmg in questione
   * @param flowList lista dei flussi
   * @return [[flowList]] valorizzando il campo im1IgmgCoeff se previsto dalle logiche
   */
  def attachIm1IgmgCoefficient(flowList: List[FlowWithInfo]): List[FlowWithInfo] = {
    val im1IgmgWithIndex = flowList.zipWithIndex.find({ case (fwi, idx) => fwi.flow.isInstanceOf[Im1Igmg] })
    if (im1IgmgWithIndex.isDefined) { //inductive step
      val (im1Igmg, im1IgmgIndex) = im1IgmgWithIndex.get
      val slicingIndex = math.min(im1IgmgIndex + 1, flowList.size)
      flowList.slice(0, slicingIndex).map(_.copy(im1IgmgCoeff = im1Igmg.flow.asInstanceOf[Im1Igmg].pre.coefCorr)) ++
        attachIm1IgmgCoefficient(flowList.slice(slicingIndex, flowList.size).map(_.copy(im1IgmgCoeff = im1Igmg.flow.asInstanceOf[Im1Igmg].post.coefCorr)))
    } else { //base case
      flowList
    }
  }

  /** Imposta il flag di forzatura, utilizzato per avere evidenza della forzatura del tipo dimensionale */
  def forceDimTypeAndAddForcingFlag(flowWithInfo: FlowWithInfo, dimType: DimensionalType.Value, forcingFlag: ForcingFlags.Value): FlowWithInfo = {
    flowWithInfo.copy(dimensionalType = Some(dimType), flow = flowWithInfo.flow.setDimTypeForced(forcingFlag))
  }
}