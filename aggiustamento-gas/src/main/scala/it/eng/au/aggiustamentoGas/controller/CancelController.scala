package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.controller.CancelController.cancelOtherMeasuresMap
import it.eng.au.aggiustamentoGas.model.measure._
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Igmg, Igmr}
import it.eng.au.aggiustamentoGas.utility.UtilityFunctions._
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/** Applica le logiche di annullamento delle misure. Vi sono tre casistiche:
 *  - RGL, RML con motivazione 3 -> (RGL -> TGL, RML -> TML)
 *  - RGL, RML con motivazione 6 -> (RML, RGL -> TAL, TAS, TAV)
 *  - tutte le altre rettifiche con motivazione 3 -> (A01R -> A01, A02R -> A02, ...)
 *
 * A partire da AU-537, mot6 si comporta come mot3, ovvero annulla tutte le misure (in questo caso autoletture) precedentemente trasmesse. */
class CancelController {

  /** RGL e RML con motivazione 3 annullano, rispettivamente, le TGL e le RML precedentemente trasmesse*/
  def cancelTreatmentMeasures(treatmentMeasures: RDD[Flow]): RDD[Flow] = {
    val flowCancellerRgl = List(classOf[Rgl])
    val cancelWithRgl = cancelPerMotivation(treatmentMeasures.keyBy(f => (f.pdr, f.date, f match{
      case _:Rgl | _:Tgl => 0
      case _ => -1
    })), flowCancellerRgl, 3)

    val flowCancellerRml = List(classOf[Rml])
    cancelPerMotivation(cancelWithRgl.keyBy(f => (f.pdr, f.date, f match{
      case _:Rml | _:Tml => 0
      case _ => -1
    })), flowCancellerRml, 3)
      .filter(f => !(f.isInstanceOf[Rgl] || f.isInstanceOf[Rml]) || (f.motivation.getOrElse(-1) != 3)) //remove mot 3 due to CR: email 14/06/2021, R: Recap Bugfix AGG, from giulia.ferrante@acquirenteunico.it
  }

  /** RGL e RML con motivazione 6 annullano i flussi di autolettura precedentemente trasmessi */
  def cancelMot6(treatmentMeasures: RDD[Flow]): RDD[Flow] = {
    //first we cancel flows using Rgl
    val flowCancellerRgl = List(classOf[Rgl])
    val cancelWithRgl = cancelPerMotivation(treatmentMeasures.keyBy(f => (f.pdr, f.date, 0)), flowCancellerRgl, 6)

    //then we do the same with Rml
    val flowCancellerRml = List(classOf[Rml])
    cancelPerMotivation(cancelWithRgl.keyBy(f => (f.pdr, f.date, 0)), flowCancellerRml, 6)
      .filter(f => !(f.isInstanceOf[Rgl] || f.isInstanceOf[Rml]) || (f.motivation.getOrElse(-1) != 6))
  }

  /** Annulla le "altre" misure, utilizzando i rispettivi flussi di rettifica con motivazione 3 */
  def cancelOtherMeasures(measures: RDD[Flow]): RDD[Flow] = {
    val zipCancelOtherMeasuresMap = cancelOtherMeasuresMap.zipWithIndex
    val groupedMeasures = measures.keyBy(f => (f.pdr, f.date,
      // map is zipped with an index to group rettifiche only with their flows to annull using the index value (-1 for those measures that are not to "rettificare")
      zipCancelOtherMeasuresMap.find({case ((rett, flows), index) => (rett.isInstance(f) && f.motivation == Some(3)) || flows.exists(_.isInstance(f))
    }).map(_._2).getOrElse(-1)
    ))

    cancelPerMotivation3OtherMeasure(groupedMeasures, cancelOtherMeasuresMap.keys.toList)
  }

  def cancelIgmrAndIgmgWithIgmrMot3Measures(measures: RDD[Flow]): RDD[Flow] = {

    val toCancelIgmr = measures
      .filter(f => f.isInstanceOf[Igmr] && !Set(3).contains(f.motivation.getOrElse(-1)))

    val toCancelIgmg = measures
      .filter(_.isInstanceOf[Igmg])

    val toCancel = toCancelIgmr.union(toCancelIgmg)
      .map(f => (makeKey(f), f))

    val igmrMot3Pairs = measures
      .filter(_.isInstanceOf[Igmr])
      .filter(f => Set(3).contains(f.motivation.getOrElse(-1)))
      .map(f => (makeKey(f), f))

    val joined = toCancel.leftOuterJoin(igmrMot3Pairs)

    val toCancelFlows = joined.filter { case (_, (igmg, maybeIgmr3)) => maybeIgmr3.isDefined && maybeIgmr3.get.dataCaricamento.get.isAfter(igmg.dataCaricamento.get) }
      .map { case (_, (toCancelFlow, _)) => toCancelFlow }
      .map(f => (makeKeyWithInfos(f), f))

    val measuresFiltered = measures
      .map(f => (makeKeyWithInfos(f), f))
      .leftOuterJoin(toCancelFlows)
      .filter { case (_, (_, maybeToCancel)) => maybeToCancel.isEmpty }
      .map { case (_, (flow, _)) => flow }

    measuresFiltered.filter(f => !(f.isInstanceOf[Igmr] && Set(3).contains(f.motivation.getOrElse(-1))))
  }

  /**
   *
   * @param measures measure grouped by pdr, date and a number to a group rule defined
   * @param cancellerFlows flow type that cancel
   * @param motValue value of motivation to cancel
   * @return measures with cancelled measures
   *
   * CR - Gabrini Federico - 16/12/2021 - delete all measure before rectification with motivation 3
   * CR - Gabrini Federico - 27/01/2023 - the same apply to motivation 6 as well
   */
  def cancelPerMotivation(measures: RDD[((String, Option[DateTime], Int), Flow)], cancellerFlows: List[Class[_]], motValue: Int): RDD[Flow] = {
    val result = measures.groupByKey().flatMap({case ((pdr, date, groupNumber), flows) =>
      // groupNumber -1 is an alias to not apply cancel algorithm
      if(groupNumber == -1)
        flows
      else {
        // ordering flows with timestamp rules on filename
        val orderedFlows = flows.toList.sorted(Flow.orderingSameDayFlows)

        // single case with all flows are rettifiche -> get the last
        if (orderedFlows.forall(_.motivation == Some(motValue)))
          List(orderedFlows.last)
        else {
          //if motivation 3 (or 6) delete all measure before rectification with motivation 3 (or 6)
          if (motValue == 3 || motValue == 6) {
            val getCancellerFlows = cancellerFlows(groupNumber)
            val lastMeasureToExclude = orderedFlows.reverse.find(f => getCancellerFlows.isInstance(f) && f.motivation.getOrElse(-1) == motValue)
            if(lastMeasureToExclude.isDefined) {
              val index = orderedFlows.indexOf(lastMeasureToExclude.get)
              /*orderedFlows.slice(0, index).filter(f => !value.forall(_.isInstance(f))) ++*/
              orderedFlows.slice(index, orderedFlows.length)
            }
            else orderedFlows
          } else {
            // since no motivations other than 3 or 6 use this function, this block will never execute but it's left here anyway
            // flows are coupled with its next (the last flow has a None next flow)
            val orderedFlowsCouple = orderedFlows.zip(orderedFlows.tail.map(Some(_))) :+ (orderedFlows.last, None)

            // couple are filtered if the current flow is not a rettifica (the doesn't have a motivation) and its next flow is not a rettifica with a motivation equals to motValue
            orderedFlowsCouple.filter({ case (currentFlow, nextFlow) =>
              !(
                // current is the canceller flow with mot 6
                (currentFlow.motivation == Some(motValue) && cancellerFlows.exists(_.isInstance(currentFlow))) ||
                  // next is the canceller flow with mot 6
                  (nextFlow.flatMap(_.motivation) == Some(motValue) && nextFlow.isDefined && cancellerFlows.exists(_.isInstance(nextFlow.get)))
                )
            }).map(_._1)
          }
        }
      }
    })

    result
  }

  /**
   * Cancels measures based on certain conditions and returns a modified RDD of Flow instances.
   *
   * The method groups the measures by key and performs the following operations for each group:
   *   - Orders the flows in the group based on some ordering rules (see [[Flow.orderingSameDayFlows]]).
   *   - Retrieves the canceller flows based on the group number from the cancellerFlows list.
   *   - Identifies the last measure to exclude from cancellation, which satisfies the conditions:
   *       - The flow is an instance of the canceller flow class.
   *       - The motivation value is 3.
   *   - If a last measure to exclude is found, the method slices the orderedFlows to exclude the measures before it
   *     and returns the remaining measures. Otherwise, it returns the orderedFlows as is.
   *
   * @param measures        the RDD of key-value pairs representing measures
   * @param cancellerFlows  a list of Class instances representing canceller flows
   * @return a modified RDD of Flow instances
   */
  def cancelPerMotivation3OtherMeasure(measures: RDD[((String, Option[DateTime], Int), Flow)], cancellerFlows: List[Class[_]]): RDD[Flow] = {
    measures.groupByKey().flatMap({ case ((pdr, date, groupNumber), flows) =>
      // ordering flows with timestamp rules on filename
      val orderedFlows = flows.toList.sorted(Flow.orderingSameDayFlows)
      val getCancellerFlows = cancellerFlows(groupNumber)
      val lastMeasureToExclude = orderedFlows.reverse.find(f => getCancellerFlows.isInstance(f) && f.motivation.getOrElse(-1) == 3)
      if (lastMeasureToExclude.isDefined) {
        val index = orderedFlows.indexOf(lastMeasureToExclude.get)
        /*orderedFlows.slice(0, index).filter(f => !value.forall(_.isInstance(f))) ++*/
        orderedFlows.slice(index + 1, orderedFlows.length)
      }
      else orderedFlows
    })
  }
}

object CancelController {
  /**
   * map with rettifica and its flows to cancel
   */
  val cancelOtherMeasuresMap = Map(
    classOf[Rmv] -> List(classOf[Rmv], classOf[Tmv]),
    classOf[Rsl] -> List(classOf[Rsl], classOf[Swg1], classOf[Fui], classOf[Fdd]),
    classOf[D01r] -> List(classOf[D01r], classOf[D01]),
    classOf[D02r] -> List(classOf[D02r], classOf[D02]),
    classOf[R01r] -> List(classOf[R01r], classOf[R01]),
    classOf[A40r] -> List(classOf[A40r], classOf[A40]),
    classOf[S40r] -> List(classOf[S40r], classOf[S40]),
    classOf[R40r] -> List(classOf[R40r], classOf[R40]),
    classOf[A01r] -> List(classOf[A01r], classOf[A01]),
    classOf[A02r] -> List(classOf[A02r], classOf[A02]),
    classOf[S02r] -> List(classOf[S02r], classOf[S02]),
    classOf[V01r] -> List(classOf[V01r], classOf[V01]),
    classOf[M01r] -> List(classOf[M01r], classOf[M01]),
    classOf[V02r] -> List(classOf[V02r], classOf[V02]),
    classOf[Sm1r] -> List(classOf[Sm1r], classOf[Sm1]),
    classOf[Sm2r] -> List(classOf[Sm2r], classOf[Sm2]),
    classOf[Ad2r] -> List(classOf[Ad2r], classOf[Ad2]),
    classOf[Ad3r] -> List(classOf[Ad3r], classOf[Ad3]),
    classOf[Ad4r] -> List(classOf[Ad4r], classOf[Ad4]),
    classOf[Ad5r] -> List(classOf[Ad5r], classOf[Ad5]),
    classOf[Igmr] -> List(classOf[Igmr], classOf[Igmg])
  )
}
