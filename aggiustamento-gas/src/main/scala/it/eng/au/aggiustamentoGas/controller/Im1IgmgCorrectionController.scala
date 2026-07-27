package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.controller.Im1IgmgCorrectionController._
import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO
import it.eng.au.aggiustamentoGas.model.measure._
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Igmg, Igmr, Im1, Im1Igmg}
import it.eng.au.aggiustamentoGas.model.measure.measureTypes.RettificaFlow
import org.apache.spark.rdd.RDD

class Im1IgmgCorrectionController {

  /**
   * @param im1IgmgTreatmentMeasures RDD of Flow, where flow are Igmg, Im1, Rml, Rgl, Tgl, Tml
   * @return RDD of Flow where all the IGMG needing adjustments are adjusted.
   */
  def getAdjustedIgmg(im1IgmgTreatmentMeasures: RDD[Flow]): RDD[Flow] = {
    val toCorrectRDD = im1IgmgTreatmentMeasures.keyBy(f => (f.pdr, f.date.getOrElse("01/01/1980", MeasureDAO.genericDateTimeFormatter))).groupByKey()
    val correctedIgmgRDD = toCorrectRDD.flatMap({ case (_, measureList) => correctMeasureList(measureList) })
    correctedIgmgRDD
  }
}

object Im1IgmgCorrectionController {
  /**
   * Filter function to read only valid igmg/im1.
   * Used into FlowController.getIm1IgmgMeasures
   * */
  lazy val im1IgmgReadFilter: Flow => Boolean = flow => {
    flow.isInstanceOf[Im1Igmg] && ((!flow.isInstanceOf[Im1]) || flow.asInstanceOf[Im1Igmg].cau_int_mis.isDefined)
  }
  /**
   * Filter telling if the flow is a correction for Im1 or Igmg
   * CR - Gabrini Federico - 16/12/2021 - RGL and RML with motivation 4|5 not rectify anymore
   * CR - Introducing IGMR to rectify IGMG, excluding RML from this logic
   * */
  lazy val isCorrectionFlow: Flow => Boolean = flow => {
    (flow.isInstanceOf[Igmr] && flow.asInstanceOf[Igmr].motivation.getOrElse(-1) == 2)  ||
    //(flow.isInstanceOf[Igmr] && flow.asInstanceOf[Igmr].motivation.getOrElse(-1) == 3)
    (flow.isInstanceOf[Rml] && flow.asInstanceOf[Rml].motivation.getOrElse(-1) == 1) ||
      (flow.isInstanceOf[Rml] && flow.asInstanceOf[Rml].motivation.getOrElse(-1) == 2) //||
//      (flow.isInstanceOf[Rml] && flow.asInstanceOf[Rml].motivation.getOrElse(-1) == 4) ||
//      (flow.isInstanceOf[Rml] && flow.asInstanceOf[Rml].motivation.getOrElse(-1) == 5) ||
//      (flow.isInstanceOf[Rgl] && flow.asInstanceOf[Rgl].motivation.getOrElse(-1) == 4) ||
//      (flow.isInstanceOf[Rgl] && flow.asInstanceOf[Rgl].motivation.getOrElse(-1) == 5)
  }
  /**
   * Apply rules form specs.
   * */
  private val correctIm1Igmg: (Im1Igmg, Flow) => Flow = (oldIgmg, corr) => {
    val correction = corr.asInstanceOf[RettificaFlow]
    val oldPre = oldIgmg.pre
    val oldIPost = oldIgmg.post
    var newIm1Igmg: Im1Igmg = oldIgmg

    val (shouldMatchSerialM, shouldMatchSerialC): Tuple2[Option[Boolean],Option[Boolean]] = (oldIgmg.cau_int_mis, oldIgmg.cau_int_cor) match {
      case  (Some(1), Some(1)) | (Some(1), Some(2)) | (Some(1), Some(3)) | (Some(1), Some(4)) | (Some(1), Some(5)) |
            (Some(2), Some(1)) | (Some(2), Some(2)) | (Some(2), Some(3)) | (Some(2), Some(4)) | (Some(2), Some(5)) |
            (Some(3), Some(1)) | (Some(3), Some(2)) | (Some(3), Some(3)) | (Some(3), Some(4)) | (Some(3), Some(5)) |
            (Some(4), Some(1)) | (Some(4), Some(2)) | (Some(4), Some(3)) | (Some(4), Some(4)) | (Some(4), Some(5)) |
            (Some(5), Some(1)) | (Some(5), Some(2)) | (Some(5), Some(3)) | (Some(5), Some(4)) | (Some(5), Some(5)) |
            (Some(6), Some(1)) | (Some(6), Some(2)) | (Some(6), Some(3)) | (Some(6), Some(4)) | (Some(6), Some(5))
      => (Some(true), Some(true))
      case (None, Some(1)) | (None, Some(2)) | (None, Some(3)) | (None, Some(4)) => (Some(false), Some(true))
      case (Some(1), None) | (Some(2), None) | (Some(3), None) | (Some(4), None) | (Some(5), None) | (Some(6), None) => (Some(true), Some(false))
      case (_, _) => (None, None)
    }

    /**
     * CONDITION FROM SPECS: we are correcting measure if shouldMatchSerial flag is true and serial number matches otherwise
     * always correct measure. This condition is translated into logical implication. Logical implication  A=>B  is
     * equivalent to (not A) or B:
     *
     * A = shouldMatchSerial
     * B = oldPre.serialNumberMis.getOrElse("-1").trim.equalsIgnoreCase(adjustment.serialNumberMis.getOrElse("-2").trim)
     *
     * A | B | A=>B  | (not A) or B
     * ----------------------------
     * T | T |   T   |   T
     * T | F |   F   |   F
     * F | T |   T   |   T
     * F | F |   T   |   T
     *
     * Hence in the first two if:
     * */
    if (shouldMatchSerialM.isDefined && ((!shouldMatchSerialM.get) || oldPre.serialNumberMis.getOrElse("-1").trim.equalsIgnoreCase(correction.serialNumberMis.getOrElse("-2").trim))) {
      newIm1Igmg = newIm1Igmg match {
        case newIgmg: Igmg => newIgmg.copy(pre = newIgmg.pre.copy(measure = correction.measure, isCorrected = true, correctionFlow = Option(correction)))
        case newIm1: Im1 => newIm1.copy(pre = newIm1.pre.copy(measure = correction.measure, isCorrected = true, correctionFlow = Option(correction)))
      }
    }
    if (shouldMatchSerialM.isDefined && ((!shouldMatchSerialM.get) || oldIPost.serialNumberMis.getOrElse("-1").trim.equalsIgnoreCase(correction.serialNumberMis.getOrElse("-2").trim))) {
      newIm1Igmg = newIm1Igmg match {
        case newIgmg: Igmg => newIgmg.copy(post = newIgmg.post.copy(measure = correction.measure, isCorrected = true, correctionFlow = Option(correction)))
        case newIm1: Im1 => newIm1.copy(post = newIm1.post.copy(measure = correction.measure, isCorrected = true, correctionFlow = Option(correction)))
      }
    }

    if (shouldMatchSerialC.isDefined && ((!shouldMatchSerialC.get) || oldPre.serialNumberConv.getOrElse("-1").trim.equalsIgnoreCase(correction.serialNumberConv.getOrElse("-2").trim))) {
      newIm1Igmg = newIm1Igmg match {
        case newIgmg: Igmg => newIgmg.copy(pre = newIgmg.pre.copy(converted = correction.converted, isCorrected = true, correctionFlow = Option(correction)))
        case newIm1: Im1 => newIm1.copy(pre = newIm1.pre.copy(converted = correction.converted, isCorrected = true, correctionFlow = Option(correction)))
      }
    }
    if (shouldMatchSerialC.isDefined && ((!shouldMatchSerialC.get) || oldIPost.serialNumberConv.getOrElse("-1").trim.equalsIgnoreCase(correction.serialNumberConv.getOrElse("-2").trim))) {
      newIm1Igmg = newIm1Igmg match {
        case newIgmg: Igmg => newIgmg.copy(post = newIgmg.post.copy(converted = correction.converted, isCorrected = true, correctionFlow = Option(correction)))
        case newIm1: Im1 => newIm1.copy(post = newIm1.post.copy(converted = correction.converted, isCorrected = true, correctionFlow = Option(correction)))
      }
    }
    newIm1Igmg
  }

  /**
   * Reduce function: Get most recent igmg/im1, get the most recent correction Flow, and correct the im1/igmg. If there
   * isn't an im1Igmg or there isn't a correction delegate to priority controller.
   * */
  private val correctMeasureList: Iterable[Flow] => Iterable[Flow] = flows => {
    if (flows.size < 1) {
      Nil
    } //Base case: list is empty
    else {
      val orderedFlows = flows.toList.sorted(Flow.orderingSameDayFlows).reverse //reversing to get most recent ones
      val correction = orderedFlows.find(isCorrectionFlow)
      val flow2correct = orderedFlows.find(f => f.isInstanceOf[Im1Igmg] && !f.isInstanceOf[Igmr])
      if (correction.isEmpty || flow2correct.isEmpty) { //Base case: no correction to apply
        orderedFlows
      }
      else { //return the corrected im1/igmg
        correctMeasureList(orderedFlows.filter(f => !f.equals(correction.get) && !f.equals(flow2correct.get))) ++ Iterable(correctIm1Igmg(flow2correct.get.asInstanceOf[Im1Igmg], correction.get))
      }
    }
  }


}
