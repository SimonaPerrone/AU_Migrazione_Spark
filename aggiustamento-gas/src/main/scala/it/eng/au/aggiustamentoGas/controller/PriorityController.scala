package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.controller.PriorityController.{associateActivationFlow, associateIm1IgmgWithSameDayFlow, getOneMeasure, rules}
import it.eng.au.aggiustamentoGas.model.measure._
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Igmg, Igmr, Im1, Im1Igmg}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD


/** Controller per l'applicazione delle regole di priorità tra flussi. */
class PriorityController {
  /**
   * A parità di PdR, data e tipologia di flusso, estrae un unico flusso seguendo la priorità definita in [[rules]].
   * @param measures RDD delle misure
   * @return [[measures]] dopo aver applicato le priorità tra flussi
   */
  def getPriorityMeasures(measures: RDD[Flow]): RDD[Flow] = {
    val broadRules = Environment.getSpark.sparkContext.broadcast(rules)

    measures
      .filter(f => f.motivation.getOrElse(-1) != 3)
      .keyBy(f => (f.pdr, f.date))
      .groupByKey().values
      .map(associateIm1IgmgWithSameDayFlow)
      .map(associateActivationFlow)
      .map(flows => getOneMeasure(flows, broadRules.value))
  }
}

object PriorityController {
  /**
   * list ordered of priority rules with its index
   */
  val rules: List[(Flow => Boolean, Int)] = List(
    (f: Flow) => f.isInstanceOf[Igmr] && Set(2, 3, 4, 5).contains(f.asInstanceOf[Igmr].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Rml] && Set(1, 2, 4, 5).contains(f.asInstanceOf[Rml].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Rgl] && Set(4, 5).contains(f.asInstanceOf[Rgl].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Igmg] && Set('E', 'S').contains(f.asInstanceOf[Igmg].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Im1] && Set('E', 'S').contains(f.asInstanceOf[Im1].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Rgl] && Set(1, 2, 7).contains(f.asInstanceOf[Rgl].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Tgl] && Set('E').contains(f.asInstanceOf[Tgl].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Tml] && Set('E').contains(f.asInstanceOf[Tml].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Rmv] && Set(1, 2, 4, 5).contains(f.asInstanceOf[Rmv].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Tmv] && Set('E').contains(f.asInstanceOf[Tmv].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Tal],
    (f: Flow) => f.isInstanceOf[Tav],
    (f: Flow) => f.isInstanceOf[Tas],
    (f: Flow) => f.isInstanceOf[Rsl] && Set(1, 2, 4, 5).contains(f.asInstanceOf[Rsl].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Swg1] && f.service.equalsIgnoreCase("SWG1") && Set('E').contains(f.asInstanceOf[Swg1].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Fui] && Set('E').contains(f.asInstanceOf[Fui].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Fdd] && Set('E').contains(f.asInstanceOf[Fdd].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Swg1] && f.service.equalsIgnoreCase("SW1") && Set('E').contains(f.asInstanceOf[Swg1].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[A01r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[A01r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[A01] && Set('E').contains(f.asInstanceOf[A01].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[A40r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[A40r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[A40] && Set('E').contains(f.asInstanceOf[A40].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[D01r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[D01r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[D01] && Set('E').contains(f.asInstanceOf[D01].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[D02r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[D02r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[D02] && Set('E').contains(f.asInstanceOf[D02].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Sm1r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[Sm1r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Sm1] && Set('E').contains(f.asInstanceOf[Sm1].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Sm2r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[Sm2r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Sm2] && Set('E').contains(f.asInstanceOf[Sm2].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Ad2r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[Ad2r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Ad2] && Set('E').contains(f.asInstanceOf[Ad2].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Ad3r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[Ad3r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Ad3] && Set('E').contains(f.asInstanceOf[Ad3].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Ad4r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[Ad4r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Ad4] && Set('E').contains(f.asInstanceOf[Ad4].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Ad5r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[Ad5r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Ad5] && Set('E').contains(f.asInstanceOf[Ad5].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[A02r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[A02r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[A02] && Set('E').contains(f.asInstanceOf[A02].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[S02r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[S02r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[S02] && Set('E').contains(f.asInstanceOf[S02].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[S40r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[S40r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[S40] && Set('E').contains(f.asInstanceOf[S40].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[R01r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[R01r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[R01] && Set('E').contains(f.asInstanceOf[R01].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[R40r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[R40r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[R40] && Set('E').contains(f.asInstanceOf[R40].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[M01r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[M01r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[M01] && Set('E').contains(f.asInstanceOf[M01].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[V01r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[V01r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[V01] && Set('E').contains(f.asInstanceOf[V01].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[V02r] && Set(1, 2, 4, 5).contains(f.asInstanceOf[V02r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[V02] && Set('E').contains(f.asInstanceOf[V02].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Tgl] && Set('S').contains(f.asInstanceOf[Tgl].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Tml] && Set('A').contains(f.asInstanceOf[Tml].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Tmv] && Set('S','A').contains(f.asInstanceOf[Tmv].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Swg1] && f.service.equalsIgnoreCase("SWG1") && Set('S','A').contains(f.asInstanceOf[Swg1].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Fui] && Set('S','A').contains(f.asInstanceOf[Fui].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Fdd] && Set('S','A').contains(f.asInstanceOf[Fdd].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Swg1] && f.service.equalsIgnoreCase("SW1") && Set('S').contains(f.asInstanceOf[Swg1].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[A01] && Set('S','A').contains(f.asInstanceOf[A01].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[A40] && Set('S','A').contains(f.asInstanceOf[A40].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[D01] && Set('S','A').contains(f.asInstanceOf[D01].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[D02] && Set('S','A').contains(f.asInstanceOf[D02].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Sm1] && Set('S','A').contains(f.asInstanceOf[Sm1].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Sm2] && Set('S','A').contains(f.asInstanceOf[Sm2].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Ad2] && Set('S','A').contains(f.asInstanceOf[Ad2].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Ad3] && Set('S','A').contains(f.asInstanceOf[Ad3].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Ad4] && Set('S','A').contains(f.asInstanceOf[Ad4].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Ad5] && Set('S','A').contains(f.asInstanceOf[Ad5].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[A02] && Set('S','A').contains(f.asInstanceOf[A02].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[S02] && Set('S','A').contains(f.asInstanceOf[S02].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[S40] && Set('S','A').contains(f.asInstanceOf[S40].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[R01] && Set('S','A').contains(f.asInstanceOf[R01].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[R40] && Set('S','A').contains(f.asInstanceOf[R40].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[M01] && Set('S','A').contains(f.asInstanceOf[M01].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[V01] && Set('S','A').contains(f.asInstanceOf[V01].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[V02] && Set('S','A').contains(f.asInstanceOf[V02].readType.getOrElse('-'))
  ).zipWithIndex

  /**
   * Ottiene un unico flusso utilizzando le priorità in [[rules]].
   * @param flows la lista dei flussi a parità di PdR e data
   * @param rules la lista delle priorità tra flussi
   * @return il flusso con priorità più alta tra i flussi in [[flows]]
   */
  def getOneMeasure(flows: Iterable[Flow], rules: List[(Flow => Boolean, Int)]): Flow = {
    val flowsWithPriority = flows.map(f => {
      val priority = rules.find({ case (rule, _) => rule(f) }).map(_._2)
      (f, priority)
    })

    val max = 10000
    val minPriority = flowsWithPriority.map(_._2.getOrElse(max)).min
    val flowsMinPriority = flowsWithPriority.filter(_._2.getOrElse(max) == minPriority).map(_._1)

    flowsMinPriority.toList.max(Flow.orderingSameDayFlows)
  }

  /**
   * Associate to each im1/igmg/igmr its predecessor if present. The association in needed to apply special consumption
   * formulas for im1/igmg/igmr.
   * @param iterableFlows a list of same day flows
   * @return a sorted list of same day flows
   */
  def associateIm1IgmgWithSameDayFlow(iterableFlows: Iterable[Flow]): Iterable[Flow] = {
    if (iterableFlows.size < 2) { //base case: empty list or single flow list
      return iterableFlows
    }
    val sortedFlows = iterableFlows.toList.sorted(Flow.orderingSameDayFlows)
    val sortedFlowsFiltered = sortedFlows.filter(!_.isInstanceOf[Im1Igmg])
    val sameDayMostRecentFlow = if (sortedFlowsFiltered.nonEmpty) Some(getOneMeasure(sortedFlowsFiltered, rules)) else None
    sortedFlows.map {
      case f: Im1 => f.copy(sameDayFlow = sameDayMostRecentFlow)
      case f: Igmg => f.copy(sameDayFlow = sameDayMostRecentFlow)
      case f: Igmr => f.copy(sameDayFlow = sameDayMostRecentFlow)
      case f: Flow => f
    }
  }

  /**
   *
   * CR - Gabrini Federico - 16/12/2021 - to bypass priority Controller, for the activation flow, associate activation flow to all measure where it is present
   * @param iterableFlows
   * @return
   */
  def associateActivationFlow(iterableFlows: Iterable[Flow]): Iterable[Flow] = {
    if(iterableFlows.size > 1) {
      val listActivationFlow = List(classOf[A01], classOf[A01r], classOf[A40], classOf[A40r])
      val isActivationFlow: Flow => Boolean = f => listActivationFlow.exists(_.isInstance(f))
      val activationFlow = iterableFlows.filter(isActivationFlow)
      val getOnePriorityActivationFlow = if (activationFlow.nonEmpty) Option(getOneMeasure(activationFlow, rules)) else None

      iterableFlows.map(f => {
        if (listActivationFlow.exists(_.isInstance(f)))
          f
        else
          f.setActivationFlow(getOnePriorityActivationFlow)
      })
    } else iterableFlows
  }
}
