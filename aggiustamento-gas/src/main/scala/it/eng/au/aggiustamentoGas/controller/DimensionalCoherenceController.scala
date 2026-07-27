package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.utility.constants.{DimensionalType, ForcingFlags}
import it.eng.au.aggiustamentoGas.model.agg.{ExternalDailyInfo, FlowWithInfo}
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg._
import it.eng.au.aggiustamentoGas.model.measure.measureTypes.RettificaFlow
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Rgl, Rml}
import org.apache.spark.rdd.RDD

/** Controlla la coerenza del tipo dimensionale all'interno dei segmenti di misure */
class DimensionalCoherenceController {

  def getCoherentSegmentsRDD(pdrCouples: RDD[(String, (List[(FlowWithInfo, FlowWithInfo)], ExternalDailyInfo))]): RDD[(String, (List[(FlowWithInfo, FlowWithInfo)], ExternalDailyInfo))] = {
    pdrCouples.mapValues({ case (segments, externalDailyInfo) =>
      val coherentSegmet = DimensionalCoherenceController.handlePostSplitIm1IgmgFormula4Wrapper(segments)
        .map(DimensionalCoherenceController.handleMot4andMot5DimType)
        .map(DimensionalCoherenceController.applyCoherence)

      (coherentSegmet, externalDailyInfo)
    })
  }
}

object DimensionalCoherenceController {

  def canUseConvertedOrHybridCoherenceToIm1Igmg(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo): Boolean = {
    endMeasure.flow match {
      case igmg: Im1Igmg => canUseConvertedOrHybridCoherenceToIm1Igmg(startMeasure.flow, igmg)
      case _ => false
    }
  }

  def canUseConvertedOrHybridCoherenceToIm1Igmg(startMeasure: Flow, endMeasure: Im1Igmg): Boolean = {
    (
      (startMeasure.converted.isDefined && endMeasure.pre.converted.isDefined) || (
        endMeasure.post.converted.isDefined && (
          endMeasure.sameDayFlow.isDefined && endMeasure.sameDayFlow.get.converted.isDefined
          )
        )
      )
  }

  /**
   * Apply dimensional coherence rule: If converted is defined use converted otherwise use prel*coefficient. <br><br>
   *
   * takes as input a segment (a couple of measure orderdered by date, ascending)<br>
   * returns (startMeasure, endMeasure) where startMeasure.dimensionalType and cendMeasure.dimensionalType are equals
   * and valued according to dimensional coherence rules
   *
   * CR - Gabrini Federico - 16/12/2021 - add hybrid colcualtion mode to igmg im1
   */
  def applyCoherence(segment: (FlowWithInfo, FlowWithInfo)): (FlowWithInfo, FlowWithInfo) = {
    val (startMeasure, endMeasure) = segment
    val canUseConverted = (startMeasure.flow.converted.isDefined && endMeasure.flow.converted.isDefined) || (
      endMeasure.flow.isInstanceOf[Im1Igmg] && startMeasure.flow.converted.isDefined &&
        endMeasure.flow.asInstanceOf[Im1Igmg].pre.converted.isDefined &&
        endMeasure.flow.asInstanceOf[Im1Igmg].post.converted.isDefined &&
        endMeasure.flow.asInstanceOf[Im1Igmg].sameDayFlow.isDefined &&
        endMeasure.flow.asInstanceOf[Im1Igmg].sameDayFlow.get.converted.isDefined
      )

    //TODO da rivedere formula 4
    //    val canUseConvertedIgmgIm1 = endMeasure.flow.isInstanceOf[Im1Igmg] && canUseConvertedOrHybridCoherenceToIm1Igmg(startMeasure, endMeasure)
    //
    //    val canUseConverted = (startMeasure.flow.converted.isDefined && endMeasure.flow.converted.isDefined) || (canUseConvertedIgmgIm1)
    //
    //    val isHybridConvertedIgmgIm1 = (
    //      endMeasure.flow.isInstanceOf[Im1Igmg] &&
    //        !(startMeasure.flow.converted.isDefined &&
    //        endMeasure.flow.asInstanceOf[Im1Igmg].pre.converted.isDefined &&
    //        endMeasure.flow.asInstanceOf[Im1Igmg].post.converted.isDefined &&
    //        endMeasure.flow.asInstanceOf[Im1Igmg].sameDayFlow.isDefined &&
    //        endMeasure.flow.asInstanceOf[Im1Igmg].sameDayFlow.get.converted.isDefined)
    //      )

    (startMeasure.dimensionalType, endMeasure.dimensionalType) match {
      /** CR: 15/06/2021 dimensional coherence must override forcing. mail R: Recap Bugfix AGG from  giulia.ferrante@acquirenteunico.it - 18:39
       * //to skip cases otherwise forcing and dim coherence are overlapping rules.
       * //case _ if startMeasure.flow.isInstanceOf[Post] && endMeasure.flow.dimTypeForced.equals(Some(ForcingFlags.FF)) => segment
       * //case _ if endMeasure.flow.isInstanceOf[Pre] && startMeasure.flow.dimTypeForced.equals(Some(ForcingFlags.FB)) => segment
       * //case _ if endMeasure.flow.isInstanceOf[Im1Igmg] && startMeasure.flow.dimTypeForced.equals(Some(ForcingFlags.FB)) => segment
       */
      //coherent cases
      case (Some(DimensionalType.C), Some(DimensionalType.C)) if canUseConverted =>
        //TODO da rivedere formula 4
        //        if (isHybridConvertedIgmgIm1) (startMeasure.copy(dimensionalType = Some(DimensionalType.H)), endMeasure.copy(dimensionalType = Some(DimensionalType.H)))
        //        else
        segment
      case (Some(DimensionalType.P), Some(DimensionalType.P)) | (Some(DimensionalType.PK), Some(DimensionalType.PK)) => segment //coherent -> do nothing
      //incoherent cases
      case (Some(DimensionalType.C), Some(DimensionalType.C)) =>
        (startMeasure.copy(dimensionalType = Some(DimensionalType.PK)), endMeasure.copy(dimensionalType = Some(DimensionalType.PK)))
      case (Some(DimensionalType.P), Some(DimensionalType.C)) | (Some(DimensionalType.C), Some(DimensionalType.P)) if canUseConverted =>
        //TODO da rivedere formula 4
        //        if (isHybridConvertedIgmgIm1) (startMeasure.copy(dimensionalType = Some(DimensionalType.H)), endMeasure.copy(dimensionalType = Some(DimensionalType.H)))
        //        else
        (startMeasure.copy(dimensionalType = Some(DimensionalType.C)), endMeasure.copy(dimensionalType = Some(DimensionalType.C)))
      case (Some(DimensionalType.P), Some(DimensionalType.C)) | (Some(DimensionalType.C), Some(DimensionalType.P)) =>
        (startMeasure.copy(dimensionalType = Some(DimensionalType.P)), endMeasure.copy(dimensionalType = Some(DimensionalType.P)))
      case (Some(DimensionalType.P), Some(DimensionalType.PK)) | (Some(DimensionalType.PK), Some(DimensionalType.P)) =>
        (startMeasure.copy(dimensionalType = Some(DimensionalType.PK)), endMeasure.copy(dimensionalType = Some(DimensionalType.PK)))
      case (Some(DimensionalType.PK), Some(DimensionalType.C)) | (Some(DimensionalType.C), Some(DimensionalType.PK)) if canUseConverted =>
        //TODO da rivedere formula 4
        //        if (isHybridConvertedIgmgIm1) (startMeasure.copy(dimensionalType = Some(DimensionalType.H)), endMeasure.copy(dimensionalType = Some(DimensionalType.H)))
        //        else
        (startMeasure.copy(dimensionalType = Some(DimensionalType.C)), endMeasure.copy(dimensionalType = Some(DimensionalType.C)))
      case (Some(DimensionalType.PK), Some(DimensionalType.C)) | (Some(DimensionalType.C), Some(DimensionalType.PK)) =>
        (startMeasure.copy(dimensionalType = Some(DimensionalType.PK)), endMeasure.copy(dimensionalType = Some(DimensionalType.PK)))
      case (_, _) => (startMeasure.copy(dimensionalType = Some(DimensionalType.PK)), endMeasure.copy(dimensionalType = Some(DimensionalType.PK))) //DEFAULT, for safety
    }
  }

  /**
   * This method delegates to Consumption Class the checks to correctly compute consumptions on (Flow, Im1Igmg)
   * but allows us to force the proper dimensionalType on the next segment (the one after (Flow, Im1Igmg),
   * that is (Im1Igmg.sameDayFlow, Flow_{k+1})   ). No check is performed on flow.converted, it is delegated to
   * applyCoherence method.
   *
   *
   * Intercetta tutti i casi in  mail "formula 4 anticipatoria" allegato "Proposta-F4_v.2.0.xlsx" altrimenti usa la
   * vecchia gestione con dimensionalType =  C or esle PK
   * */
  def handlePostSplitIm1IgmgFormula4Wrapper(segments: List[(FlowWithInfo, FlowWithInfo)]): List[(FlowWithInfo, FlowWithInfo)] = {
    if (segments.size < 1) { //base case
      segments
    }
    else if (segments.size == 1) { //base case
      List(handlePostSplitIm1IgmgDimType(segments.head))
    } else { //Inductive step
      val firstSegment = segments.head
      val secondSegment = segments.tail.head
      val remainingSegments = segments.tail.tail

      (firstSegment._1.flow, firstSegment._2.flow, secondSegment._1.flow, secondSegment._2.flow) match {
        case (_, end1: Im1, _, _) if end1.cau_int_cor.equals(Some(3)) || end1.cau_int_cor.equals(Some(4)) =>
          List(
            firstSegment, // delegate field checks to Consumption difference procedure
            getSecondSegmentCorrectedOrForcedWithFormula4Rules(secondSegment, DimensionalType.PK) //force PK on the second segment if its extremes are not both RettificheFlow
          ) ++ handlePostSplitIm1IgmgFormula4Wrapper(remainingSegments)

        case (_, end1: Im1, _, _) if end1.cau_int_cor.equals(Some(5)) =>
          List(
            firstSegment, // delegate field checks to Consumption difference procedure
            getSecondSegmentCorrectedOrForcedWithFormula4Rules(secondSegment, DimensionalType.C)
          ) ++ handlePostSplitIm1IgmgFormula4Wrapper(remainingSegments)

        case (_, end1: Igmg, _, _) if end1.cau_int_cor.equals(Some(2)) =>
          List(
            firstSegment, // delegate field checks to Consumption difference procedure
            getSecondSegmentCorrectedOrForcedWithFormula4Rules(secondSegment, DimensionalType.PK) //force PK on the second segment if its extremes are not both RettificheFlow
          ) ++ handlePostSplitIm1IgmgFormula4Wrapper(remainingSegments)

        case (_, end1: Igmg, _, _) if end1.cau_int_cor.equals(Some(3)) || end1.cau_int_cor.equals(Some(4)) =>
          List(
            firstSegment, // delegate field checks to Consumption difference procedure
            getSecondSegmentCorrectedOrForcedWithFormula4Rules(secondSegment, DimensionalType.C) //force PK on the second segment if its extremes are not both RettificheFlow
          ) ++ handlePostSplitIm1IgmgFormula4Wrapper(remainingSegments)

        case _ => List(
          handlePostSplitIm1IgmgDimType(firstSegment)
        ) ++ handlePostSplitIm1IgmgFormula4Wrapper(segments.tail)
      }
    }
  }

  def isRettificaSegment(segment: (FlowWithInfo, FlowWithInfo)): Boolean =
    (segment._1.flow, segment._2.flow) match {
      case (_: RettificaFlow, _: RettificaFlow) => true
      case (_: RettificaFlow, e: Im1Igmg) if e.pre.isCorrected || e.post.isCorrected => true
      case (_: RettificaFlow, e: Pre) if e.isCorrected => true
      case _ => false
    }

  def getSecondSegmentCorrectedOrForcedWithFormula4Rules(segment: (FlowWithInfo, FlowWithInfo), forcingValue: DimensionalType.Value): (FlowWithInfo, FlowWithInfo) = {
    if (isRettificaSegment(segment))
      (segment._1.copy(dimensionalType = Some(DimensionalType.C)), segment._2.copy(dimensionalType = Some(DimensionalType.C))) //delegate to dimensional coherence check to check if both sides can use converted
    else
      (segment._1.copy(dimensionalType = Some(forcingValue)), segment._2.copy(dimensionalType = Some(forcingValue)))
  }

  /** Applica le logiche di assegnazione del tipo dimensionale nel caso di presenza di Im1/Igmg. Per maggiori info consultare i documenti tecnici. */
  def handlePostSplitIm1IgmgDimType(segment: (FlowWithInfo, FlowWithInfo)): (FlowWithInfo, FlowWithInfo) = {
    val (startFWI, endFWI) = segment

    val x = 1
    (startFWI.flow, endFWI.flow) match {
      //Apply IM1 tables from specs:
      /*
      * cau_int_mis = 1,2,3,4,5,6 and cau_int_cor = 1,2,3  Pre and Post are C or else PK
      * cau_int_mis = 1,2,3,4,5,6 and cau_int_cor = 4  Pre is C or else PK and Post is PK
      * cau_int_mis = 1,2,3,4,5,6 and cau_int_cor = 5  Pre is PK and Post is C or else PK
      * */
      //IM1 PRE
      case (start: Flow, end: Im1Pre) if start.dimTypeForced.equals(Some(ForcingFlags.FB)) =>
        (startFWI, endFWI.copy(dimensionalType = startFWI.dimensionalType))
      case (start: Flow, end: Im1Pre)
        if start.dimTypeForced.isEmpty && end.dimTypeForced.isEmpty && Set(-1, 1, 2, 3, 4).contains(end.cau_int_cor.getOrElse(-1)) =>
        (startFWI, endFWI.copy(dimensionalType = if (end.converted.isDefined) Some(DimensionalType.C) else Some(DimensionalType.PK)))
      case (start: Flow, end: Im1Pre)
        if start.dimTypeForced.isEmpty && end.dimTypeForced.isEmpty && (5 == end.cau_int_cor.getOrElse(-1)) =>
        (startFWI, endFWI.copy(dimensionalType = Some(DimensionalType.PK)))
      //IM1 POST
      case (start: Im1Post, end: Flow)
        if start.dimTypeForced.isEmpty && end.dimTypeForced.isEmpty && (start.cau_int_cor.getOrElse(-1) == 4) =>
        (startFWI.copy(dimensionalType = Some(DimensionalType.PK)), endFWI)
      case (start: Im1Post, end: Flow)
        if start.dimTypeForced.isEmpty && end.dimTypeForced.isEmpty && (start.cau_int_cor.getOrElse(-1) != 4) =>
        (startFWI.copy(dimensionalType = if (end.converted.isDefined) Some(DimensionalType.C) else Some(DimensionalType.PK)), endFWI)

      //IM1/IGMG formula 4
//    riportare cosi:  case (start: Flow, end: Im1Igmg) if start.converted.isDefined && end.pre.converted.isDefined && end.post.converted.isDefined
//        && end.sameDayFlow.isDefined && end.sameDayFlow.get.converted.isDefined =>
      case (start: Flow, end: Im1Igmg) if canUseConvertedOrHybridCoherenceToIm1Igmg(start, end) =>
        (startFWI.copy(dimensionalType = Some(DimensionalType.C)), endFWI.copy(dimensionalType = Some(DimensionalType.C)))
      case (start: Flow, end: Im1Igmg) =>
        (startFWI.copy(dimensionalType = Some(DimensionalType.PK)), endFWI.copy(dimensionalType = Some(DimensionalType.PK)))

      //Apply IGMG tables from specs:
      /*
      * cau_int_mis = 1,2,3,4,null and cau_int_cor = null,1  Pre C or else PK   POST
      * cau_int_mis = 1,2,3,4,null and cau_int_cor = 2  Pre C or else PK   POST PK
      * cau_int_mis = 1,2,3,4,null and cau_int_cor = 3  Pre PK POST C
      * cau_int_mis = 1,2,3,4,null and cau_int_cor = 4  Pre PK POST C
      * */
      //IGMG PRE
      case (start: Flow, end: IgmgPre) if start.dimTypeForced.equals(Some(ForcingFlags.FB)) =>
        (startFWI, endFWI.copy(dimensionalType = startFWI.dimensionalType))
      case (start: Flow, end: IgmgPre)
        if start.dimTypeForced.isEmpty && end.dimTypeForced.isEmpty && Set(-1, 1, 2).contains(end.cau_int_cor.getOrElse(-1)) =>
        (startFWI, endFWI.copy(dimensionalType = if (end.converted.isDefined) Some(DimensionalType.C) else Some(DimensionalType.PK)))
      case (start: Flow, end: IgmgPre)
        if start.dimTypeForced.isEmpty && end.dimTypeForced.isEmpty && Set(3, 4).contains(end.cau_int_cor.getOrElse(-1)) =>
        (startFWI, endFWI.copy(dimensionalType = Some(DimensionalType.PK)))
      //IGMG POST
      case (start: IgmgPost, end: Flow)
        if start.dimTypeForced.isEmpty && end.dimTypeForced.isEmpty && Set(-1, 1).contains(start.cau_int_cor.getOrElse(-1)) =>
        (startFWI.copy(dimensionalType = Some(getDimensionalType(startFWI))), endFWI)
      case (start: IgmgPost, end: Flow)
        if start.dimTypeForced.isEmpty && end.dimTypeForced.isEmpty && Set(2).contains(start.cau_int_cor.getOrElse(-1)) =>
        (startFWI.copy(dimensionalType = Some(DimensionalType.PK)), endFWI)
      case (start: IgmgPost, end: Flow)
        if start.dimTypeForced.isEmpty && end.dimTypeForced.isEmpty && Set(3, 4).contains(start.cau_int_cor.getOrElse(-1)) =>
        (startFWI.copy(dimensionalType = Some(DimensionalType.C)), endFWI)

      case (_, _) => (startFWI, endFWI)
    }

  }

  def handleMot4andMot5DimType(segment: (FlowWithInfo, FlowWithInfo)): (FlowWithInfo, FlowWithInfo) = {
    val getNewFlowWI: FlowWithInfo => FlowWithInfo = fwi => fwi.flow match {
      case f: Rgl if (f.motivation.equals(Some(4)) || f.motivation.equals(Some(5))) && f.dimTypeForced.isDefined =>
        fwi.copy(dimensionalType = Some(DimensionalType.C))
      case f: Rml if (f.motivation.equals(Some(4)) || f.motivation.equals(Some(5))) && f.dimTypeForced.isDefined =>
        fwi.copy(dimensionalType = Some(DimensionalType.C))
      case f: Igmr if f.motivation.equals(Some(2)) && f.dimTypeForced.isDefined =>
        fwi.copy(dimensionalType = Some(DimensionalType.C))
      case _ => fwi
    }
    (getNewFlowWI(segment._1), getNewFlowWI(segment._2))
  }

  /** Ottiene il tipo dimensionale dalle info su rcugas */
  def getDimensionalType(flowWithInfo: FlowWithInfo): DimensionalType.Value = {
    val defaultExpression = if (flowWithInfo.flow.converted.isDefined) DimensionalType.C else DimensionalType.PK

    if (flowWithInfo.rcuGasTech.isEmpty || flowWithInfo.rcuGasVarConvertitore.isEmpty) {
      defaultExpression
    }
    else {
      val tPreConv = if (flowWithInfo.rcuGasVarConvertitore.isEmpty) Some("NO") else flowWithInfo.rcuGasVarConvertitore.get.tPreConv
      (flowWithInfo.rcuGasTech.get.gruppoMisInt, tPreConv) match {
        case (Some("SI"), Some("SI")) | (Some("SI"), Some("NO")) | (Some("SI"), None) | (Some("S"), Some("NO")) | (None, Some("NO")) | (None, None) => DimensionalType.PK
        case (Some("NO"), Some("SI")) | (Some("N"), Some("SI")) | (None, Some("SI")) => DimensionalType.C
        case (Some("NO"), Some("NO")) | (Some("N"), Some("NO")) => DimensionalType.PK
        case (_, _) => defaultExpression
      }
    }
  }
}
