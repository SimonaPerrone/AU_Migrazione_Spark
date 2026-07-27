package it.sferanet.au.controller.validation

import it.sferanet.au.model.Flow
import it.sferanet.au.model.prestazionale.{IgmgPost, IgmgPre, IgmrPost, IgmrPre, Im1Post, Im1Pre}
import it.sferanet.au.model.rettifica.{Rgl, Rml}
import org.apache.spark.rdd.RDD

object CambioMisuratoreController {

  /** Applica il processo di rettifica da parte delle RML con motivazione 1,2 ai flussi di tipo Im1/Igmg (intervento tecnico o cambio misuratore) */
  def rettificaCambioMisuratore(measures: RDD[Flow]): RDD[Flow] = {
    measures
      .keyBy(f => (f.pdr, f.date))
      .groupByKey()
      .flatMap({ case ((pdr, date), flows) =>
        if (flows.exists(f => isCambioMisuratoreFlow(f)) && flows.exists(f => isRettificaCambioMisuratore(Some(f)))) {
          // ordering flows with timestamp rules on filename
          val orderedFlows = flows.toList.distinct.sorted(Flow.priorityOrderingFlows)
          rectifyMeasures(orderedFlows)
        } else
          flows.toList.distinct
      })
  }

  def isCambioMisuratoreFlow(f: Flow): Boolean = {
    f.isInstanceOf[Im1Pre] || f.isInstanceOf[Im1Post] || f.isInstanceOf[IgmgPre] || f.isInstanceOf[IgmgPost]
  }

  def isCambioMisuratoreFlowWithRett(f: Flow): Boolean = {
    f.isInstanceOf[Im1Pre] || f.isInstanceOf[Im1Post] || f.isInstanceOf[IgmgPre] || f.isInstanceOf[IgmgPost] || f.isInstanceOf[IgmrPre] || f.isInstanceOf[IgmrPost]
  }

  def isRettificaCambioMisuratore(measure: Option[Flow]): Boolean = {
    measure.isDefined &&
      (measure.get.isInstanceOf[Rml] && Set(1, 2).contains(measure.get.motivation.getOrElse(-1)))
  }

  def isPre(measure: Flow, flowType: String): Boolean = {
    measure.service.toUpperCase.startsWith(flowType.toUpperCase) &&
      (measure.isInstanceOf[Im1Pre] || measure.isInstanceOf[IgmgPre])
  }

  def isPost(measure: Flow, flowType: String): Boolean = {
    measure.service.toUpperCase.startsWith(flowType.toUpperCase) &&
      (measure.isInstanceOf[Im1Post] || measure.isInstanceOf[IgmgPost])
  }

  /** Applica le regole di rettifica sui valori di misura e convertito dei flussi Im1/Igmg, secondo le regole definite da AU.
   * Per maggiori info, vedere le tabelle di rettifica nei documenti tecnici. */
  def rectifyMeasures(orderedMeasures: List[Flow]): List[Flow] = {

    // ottengo flussi vincitori di cambio misuratore e rettifiche
    val winnerCMMeasures = orderedMeasures.filter(isCambioMisuratoreFlow).takeRight(2)
    val winnerRettificaMeasure = orderedMeasures.filter(f => isRettificaCambioMisuratore(Some(f))).head

    val preMeasure = winnerCMMeasures.head
    val postMeasure = winnerCMMeasures.last

    var outputList = List[Flow]()

    if ((isPre(preMeasure, "IM1") && isPost(postMeasure, "IM1"))
      || (isPre(preMeasure, "IGMG") && isPost(postMeasure, "IGMG"))) {

      val infoRettifica = winnerRettificaMeasure match {
        case rett: Rgl => (rett.serialNumberMis.getOrElse("NON RETTIFICA"), rett.serialNumberConv.getOrElse("NON RETTIFICA"), rett.measure, rett.converted)
        case rett: Rml => (rett.serialNumberMis.getOrElse("NON RETTIFICA"), rett.serialNumberConv.getOrElse("NON RETTIFICA"), rett.measure, rett.converted)
      }

      val infoPreCambioMisuratore = preMeasure match {
        case pre: Im1Pre => (pre.serialNumberMis.getOrElse(""), pre.serialNumberConv.getOrElse(""), pre.cau_int_mis, pre.cau_int_cor)
        case pre: IgmgPre => (pre.serialNumberMis.getOrElse(""), pre.serialNumberConv.getOrElse(""), pre.cau_int_mis, pre.cau_int_cor)
      }

      val infoPostCambioMisuratore = postMeasure match {
        case post: Im1Post => (post.serialNumberMis.getOrElse(""), post.serialNumberConv.getOrElse(""), post.cau_int_mis, post.cau_int_cor)
        case post: IgmgPost => (post.serialNumberMis.getOrElse(""), post.serialNumberConv.getOrElse(""), post.cau_int_mis, post.cau_int_cor)
      }

      val preLetValue = preMeasure.measure
      val postLetValue = postMeasure.measure
      val preConvValue = preMeasure.converted
      val postConvValue = postMeasure.converted

      if (Set[Option[Int]](Some(1), Some(2), Some(3), Some(4)).contains(infoPreCambioMisuratore._3) &&
        Set[Option[Int]](None).contains(infoPreCambioMisuratore._4)) {
        // TABELLA 2 RETTIFICHE (misura conv rettificata a prescindere dalla coincidenza delle matricole)
        if (isPre(preMeasure, "IM1")) {
          outputList = List(
            preMeasure.asInstanceOf[Im1Pre].copy(
              measure = if (infoPreCambioMisuratore._1 == infoRettifica._1) infoRettifica._3 else preLetValue,
              converted = infoRettifica._4,
              fileRettifica = winnerRettificaMeasure.local_file
            ).asInstanceOf[Flow],
            postMeasure.asInstanceOf[Im1Post].copy(
              measure = if (infoPostCambioMisuratore._1 == infoRettifica._1) infoRettifica._3 else postLetValue,
              converted = infoRettifica._4,
              fileRettifica = winnerRettificaMeasure.local_file
            ).asInstanceOf[Flow]
          )
        } else { // IGMG
          outputList = List(
            preMeasure.asInstanceOf[IgmgPre].copy(
              measure = if (infoPreCambioMisuratore._1 == infoRettifica._1) infoRettifica._3 else preLetValue,
              converted = infoRettifica._4,
              fileRettifica = winnerRettificaMeasure.local_file
            ).asInstanceOf[Flow],
            postMeasure.asInstanceOf[IgmgPost].copy(
              measure = if (infoPostCambioMisuratore._1 == infoRettifica._1) infoRettifica._3 else postLetValue,
              converted = infoRettifica._4,
              fileRettifica = winnerRettificaMeasure.local_file
            ).asInstanceOf[Flow]
          )
        }
      } else if (Set[Option[Int]](Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)).contains(infoPreCambioMisuratore._3) &&
        Set[Option[Int]](Some(1), Some(2), Some(3), Some(4), Some(5), None).contains(infoPreCambioMisuratore._4)) {
        // TABELLA 1 RETTIFICHE (caso base)
        if (isPre(preMeasure, "IM1")) {
          outputList = List(
            preMeasure.asInstanceOf[Im1Pre].copy(
              measure = if (infoPreCambioMisuratore._1 == infoRettifica._1) infoRettifica._3 else preLetValue,
              converted = if (infoPreCambioMisuratore._2 == infoRettifica._2) infoRettifica._4 else preConvValue,
              fileRettifica = if (infoPreCambioMisuratore._1 == infoRettifica._1 || infoPreCambioMisuratore._2 == infoRettifica._2) winnerRettificaMeasure.local_file else None
            ).asInstanceOf[Flow],
            postMeasure.asInstanceOf[Im1Post].copy(
              measure = if (infoPostCambioMisuratore._1 == infoRettifica._1) infoRettifica._3 else postLetValue,
              converted = if (infoPostCambioMisuratore._2 == infoRettifica._2) infoRettifica._4 else postConvValue,
              fileRettifica = if (infoPostCambioMisuratore._1 == infoRettifica._1 || infoPostCambioMisuratore._2 == infoRettifica._2) winnerRettificaMeasure.local_file else None
            ).asInstanceOf[Flow]
          )
        } else { // IGMG
          outputList = List(
            preMeasure.asInstanceOf[IgmgPre].copy(
              measure = if (infoPreCambioMisuratore._1 == infoRettifica._1) infoRettifica._3 else preLetValue,
              converted = if (infoPreCambioMisuratore._2 == infoRettifica._2) infoRettifica._4 else preConvValue,
              fileRettifica = if (infoPreCambioMisuratore._1 == infoRettifica._1 || infoPreCambioMisuratore._2 == infoRettifica._2) winnerRettificaMeasure.local_file else None
            ).asInstanceOf[Flow],
            postMeasure.asInstanceOf[IgmgPost].copy(
              measure = if (infoPostCambioMisuratore._1 == infoRettifica._1) infoRettifica._3 else postLetValue,
              converted = if (infoPostCambioMisuratore._2 == infoRettifica._2) infoRettifica._4 else postConvValue,
              fileRettifica = if (infoPostCambioMisuratore._1 == infoRettifica._1 || infoPostCambioMisuratore._2 == infoRettifica._2) winnerRettificaMeasure.local_file else None
            ).asInstanceOf[Flow]
          )
        }
      } else if (Set[Option[Int]](None).contains(infoPreCambioMisuratore._3) &&
        Set[Option[Int]](Some(1), Some(2), Some(3), Some(4)).contains(infoPreCambioMisuratore._4)) {
        // TABELLA 3 RETTIFICHE (misura prel rettificata a prescindere dalla coincidenza delle matricole)
        if (isPre(preMeasure, "IM1")) {
          outputList = List(
            preMeasure.asInstanceOf[Im1Pre].copy(
              measure = infoRettifica._3,
              converted = if (infoPreCambioMisuratore._2 == infoRettifica._2) infoRettifica._4 else preConvValue,
              fileRettifica = winnerRettificaMeasure.local_file
            ).asInstanceOf[Flow],
            postMeasure.asInstanceOf[Im1Post].copy(
              measure = infoRettifica._3,
              converted = if (infoPostCambioMisuratore._2 == infoRettifica._2) infoRettifica._4 else postConvValue,
              fileRettifica = winnerRettificaMeasure.local_file
            ).asInstanceOf[Flow]
          )
        } else { // IGMG
          outputList = List(
            preMeasure.asInstanceOf[IgmgPre].copy(
              measure = infoRettifica._3,
              converted = if (infoPreCambioMisuratore._2 == infoRettifica._2) infoRettifica._4 else preConvValue,
              fileRettifica = winnerRettificaMeasure.local_file
            ).asInstanceOf[Flow],
            postMeasure.asInstanceOf[IgmgPost].copy(
              measure = infoRettifica._3,
              converted = if (infoPostCambioMisuratore._2 == infoRettifica._2) infoRettifica._4 else postConvValue,
              fileRettifica = winnerRettificaMeasure.local_file
            ).asInstanceOf[Flow]
          )
        }
      } else {
        // CASISTICA NON CONSIDERATA NELLE TABELLE 1, 2 E 3 DELLE RETTIFICHE
        outputList = List(preMeasure, postMeasure)
      }
    }

    outputList
  }

}

