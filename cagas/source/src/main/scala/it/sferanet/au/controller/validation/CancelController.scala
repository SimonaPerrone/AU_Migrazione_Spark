package it.sferanet.au.controller.validation

import it.sferanet.au.model.Flow
import it.sferanet.au.model.autolettura._
import it.sferanet.au.model.periodico._
import it.sferanet.au.model.prestazionale._
import it.sferanet.au.model.rettifica._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object CancelController {

  /**
   * @param measures set di misure appartenenti al gruppo definito dal parametro @group
   * @param group    id del gruppo delle misure passate in @measures
   * @return set di misure del gruppo @group in seguito all'applicazione del motore degli annullamenti
   */
  def cancelMeasures(measures: RDD[Flow], group: Int): RDD[Flow] = {
    group match {
      case 1 => cancelPerMotivation(measures, 3, group1CancelMeasuresMap)
      case 3 => cancelPerMotivation(measures, 6, group3CancelMeasuresMap)
      case 4 => cancelPerMotivation(measures, 3, group4CancelMeasuresMap)
      case _ => measures
    }
  }

  /**
   * @param measures        set di misure
   * @param motivationValue motivazione della cancellazione
   * @param cancelMapping   mapping che definisce quale tipo di flusso può cancellare quale tipo di misura
   * @return set di misure in seguito all'applicazione del motore degli annullamenti
   */
  def cancelPerMotivation(measures: RDD[Flow], motivationValue: Int, cancelMapping: List[(Class[_], List[Class[_]])]): RDD[Flow] = {
    measures
      .keyBy(f => (f.pdr, f.date))
      .groupByKey()
      .flatMap({ case ((pdr, date), flows) =>
        // ordering flows with timestamp rules on filename
        val orderedFlows = flows.toList.distinct.sorted(Flow.cancelingOrderingFlows)

        // single case with all flows are rettifiche of cancelation -> return empty
        if (orderedFlows.forall(_.motivation == Some(motivationValue)))
          List[Flow]()
        else {
          executeCancelations(orderedFlows, motivationValue, cancelMapping)
        }
      })
  }

  private def makeKey(f: Flow) = (f.pdr, f.date)

  private def makeKeyWithInfos(f: Flow) = (f.pdr, f.date, f.service, f.d_caricamento)

  def cancelIgmrAndIgmgWithIgmrMot3MeasuresPre(measures: RDD[Flow]): RDD[Flow] = {

    val toCancelIgmr = measures
      .filter(f => f.isInstanceOf[IgmrPre] && !Set(3).contains(f.motivation.getOrElse(-1)))

    val toCancelIgmg = measures
      .filter(_.isInstanceOf[IgmgPre])

    val toCancel = toCancelIgmr.union(toCancelIgmg)
      .map(f => (makeKey(f), f))

    val igmrMot3Pairs = measures
      .filter(_.isInstanceOf[IgmrPre])
      .filter(f => Set(3).contains(f.motivation.getOrElse(-1)))
      .map(f => (makeKey(f), f))

    val joined = toCancel.leftOuterJoin(igmrMot3Pairs)

    val toCancelFlows = joined.filter { case (_, (igmg, maybeIgmr3)) => maybeIgmr3.isDefined && maybeIgmr3.get.timestampLocalFile.after(igmg.timestampLocalFile) }
      .map { case (_, (toCancelFlow, _)) => toCancelFlow }
      .map(f => (makeKeyWithInfos(f), f))

    val measuresFiltered = measures
      .map(f => (makeKeyWithInfos(f), f))
      .leftOuterJoin(toCancelFlows)
      .filter { case (_, (_, maybeToCancel)) => maybeToCancel.isEmpty }
      .map { case (_, (flow, _)) => flow }

    measuresFiltered.filter(f => !(f.isInstanceOf[IgmrPre] && Set(3).contains(f.motivation.getOrElse(-1))))
  }

  def cancelIgmrAndIgmgWithIgmrMot3MeasuresPost(measures: RDD[Flow]): RDD[Flow] = {

    val toCancelIgmr = measures
      .filter(f => f.isInstanceOf[IgmrPost] && !Set(3).contains(f.motivation.getOrElse(-1)))

    val toCancelIgmg = measures
      .filter(_.isInstanceOf[IgmgPost])

    val toCancel = toCancelIgmr.union(toCancelIgmg)
      .map(f => (makeKey(f), f))

    val igmrMot3Pairs = measures
      .filter(_.isInstanceOf[IgmrPost])
      .filter(f => Set(3).contains(f.motivation.getOrElse(-1)))
      .map(f => (makeKey(f), f))

    val joined = toCancel.leftOuterJoin(igmrMot3Pairs)

    val toCancelFlows = joined.filter { case (_, (igmg, maybeIgmr3)) => maybeIgmr3.isDefined && maybeIgmr3.get.timestampLocalFile.after(igmg.timestampLocalFile) }
      .map { case (_, (toCancelFlow, _)) => toCancelFlow }
      .map(f => (makeKeyWithInfos(f), f))

    val measuresFiltered = measures
      .map(f => (makeKeyWithInfos(f), f))
      .leftOuterJoin(toCancelFlows)
      .filter { case (_, (_, maybeToCancel)) => maybeToCancel.isEmpty }
      .map { case (_, (flow, _)) => flow }

    measuresFiltered.filter(f => !(f.isInstanceOf[IgmrPre] && Set(3).contains(f.motivation.getOrElse(-1))))
  }

  /**
   *
   * @param measures        set di misure ordinate di un singolo pdr e data di misura fissata ordinate temporalmente secondo l'ordinamento di caricamento definito in Flow
   * @param motivationValue motivazione della cancellazione
   * @param cancelMapping   mapping che definisce quale tipo di flusso può cancellare quale tipo di misura
   * @return set di misure di un singolo pdr e data di misura fissata in seguito all'applicazione del motore degli annullamenti
   */
  def executeCancelations(measures: List[Flow], motivationValue: Int, cancelMapping: List[(Class[_], List[Class[_]])]): List[Flow] = {
    // gli annullamenti possono solo annullare misure e rettiche che non sono altri annullamenti
    // creo Mappa: (flusso, giàCancellato), dove inizialmente il flag giàCancellato sarà a false solo per gli annullmenti
    var measuresCancelationMap = measures.map(f => if (f.motivation == Some(motivationValue)) (f, true) else (f, false)).to[ArrayBuffer]
    // itero dalla misura/rettifica più recente (ricordo ordinamento crescente)
    for (i <- measuresCancelationMap.length -1 to 0 by -1) {
      val measure = measuresCancelationMap(i)._1
      // controllo se measure è una rettifica di annullamento o meno
      if (measure.motivation == Some(motivationValue)) {
        if (motivationValue == 3) {
          //val listHead = measuresCancelationMap.take(i).reverse
          val (listHead, listTail) = measuresCancelationMap.splitAt(i)

          val listHeadWithCancelledMeasured =
          listHead
            .reverse
            .map({ case (f, alreadyCanceled) =>
              val toBeCancelled = cancelMapping.exists({ case (from, to) =>
                alreadyCanceled || (from.isInstance(measure) && to.exists(_.isInstance(f)))
              })
              (f, toBeCancelled)
            })

          measuresCancelationMap = listHeadWithCancelledMeasured.reverse ++ listTail
        } else {
          // -> measure è una rettifica di annullamento
          // ottendo soltanto la parte della lista che mi interessa controllare (ossia le misure precedenti in base all'ordinamento)
          // e la capovolgo temporalmente in modo da poter trovare facilmente la prima misura utile da rettificare
          val listHead = measuresCancelationMap.take(i).reverse
          // ottengo la misura da cancellare se rispetta i vincoli di mapping e se la misura non era già stata cancellata prima
          val measureToCancel = listHead
            .find({ case (f, alreadyCanceled) =>
              cancelMapping.exists({ case (from, to) => {
                !alreadyCanceled && from.isInstance(measure) && to.exists(_.isInstance(f))
              }
              })
            })
          // se trovo una misura da cancellare aggiorno la lista di misure per non considerarla più in futuro
          if (measureToCancel.isDefined) {
            val indexMeasureToCancel = listHead.indexOf(measureToCancel.get)
            // escludi la misura appena rettificata per le rettifiche successive (indice corrente che scorre verso il basso - indice elementro trovato nella prima parte della lista - 1)
            // es: TGL TGL TML RGL TML -> RGL a indice 3 cancella la TML a indice 2 facendo: 3 - 0 - 1 = 2 (0 perchè la TML è il primo elemento di listHead)
            measuresCancelationMap(i - indexMeasureToCancel - 1) = (measureToCancel.get._1, true)
          }
        }
      }
    }
    // ritorno le misure/rettifiche non cancellate e quelle che non sono rettifiche di annullamento
    measuresCancelationMap.filter(f => !f._2).map(_._1).toList
  }

  /**
   * ALTERNATIVA DA TESTARE IN TERMINI DI PERFORMANCE
   * def executeCancelations(measures: List[Flow], cancelMapping: List[(Class[_], List[Class[_]])], motivationValue: Int): List[Flow] = {
   * val measuresReversed = measures.reverse.to[ArrayBuffer]
   * var currentRettifica = measuresReversed.find(f => f.motivation == Some(motivationValue))
   * while (currentRettifica.isDefined) {
   * val indexCurrentRettifica = measuresReversed.indexOf(currentRettifica.get)
   * val measureToCancel = measuresReversed.takeRight(measuresReversed.length - indexCurrentRettifica - 1)
   * .find(f => cancelMapping.exists({case (from, to) => {
   * from.isInstance(currentRettifica.get) && to.exists(_.isInstance(f))}}))
   * if (measureToCancel.isDefined) {
   * val indexMeasureToCancel = measuresReversed.indexOf(measureToCancel.get, indexCurrentRettifica + 1)
   * measuresReversed.remove(indexMeasureToCancel)
   * }
   *
   * measuresReversed.remove(indexCurrentRettifica)
   * currentRettifica = measuresReversed.find(f => f.motivation == Some(motivationValue))
   * }
   * measuresReversed.toList
   * }
   */

  /** nel caso di rettifica con motivazione 3, la richiesta è quella di annullare tutte le misure precedenti dello stesso tipo secondo quanto segue:
   *    - flussi RGL con mot_rett_lett=3 annullano tutti i *GL precedentemente trasmessi
   *    - flussi RML con mot_rett_lett=3 annullano tutti i flussi *ML precedentemente trasmessi tranne *GL
   **/
  val group1CancelMeasuresMap = List(
    (classOf[Rgl], List(classOf[Tgl], classOf[Rgl])),
    (classOf[Rml], List(classOf[Tml], classOf[Rml]))
  )

  val group3CancelMeasuresMap = List(
    (classOf[Rgl], List(classOf[Tal], classOf[Tas], classOf[Tav])),
    (classOf[Rml], List(classOf[Tal], classOf[Tas], classOf[Tav]))
  )

  /** nel caso di rettifica con motivazione 3, la richiesta è quella di annullare tutte le misure precedenti dello stesso tipo secondo quanto segue:
   *    - flussi RMV con mot_rett_lett=3 annullano tutti i flussi *MV precedentemente trasmessi
   *    - flussi RSL con mot_rett_lett=3 annullano tutti i flussi SW1 e RSL precedentemente trasmessi
   *    - lo stesso vale per D01R, D02R, R01R, A40R, S40R, R40R, A01R, A02R, S02R, V01R, M01R, V02R, SM1R, SM2R, AD2R, AD3R, AD4R, AD5R
   **/
  val group4CancelMeasuresMap = List(
    (classOf[Rmv], List(classOf[Rmv], classOf[Tmv])),
    (classOf[Rsl], List(classOf[Sw1], classOf[Swg1], classOf[FUI], classOf[FDD], classOf[Rsl])),
    (classOf[A01R], List(classOf[A01], classOf[A01R])),
    (classOf[A40R], List(classOf[A40], classOf[A40R])),
    (classOf[SM1R], List(classOf[Sm1], classOf[SM1R])),
    (classOf[AD2R], List(classOf[AD2], classOf[AD2R])),
    (classOf[AD3R], List(classOf[AD3], classOf[AD3R])),
    (classOf[A02R], List(classOf[A02], classOf[A02R])),
    (classOf[S02R], List(classOf[S02], classOf[S02R])),
    (classOf[S40R], List(classOf[S40], classOf[S40R])),
    (classOf[R01r], List(classOf[R01], classOf[R01r])),
    (classOf[R40r], List(classOf[R40], classOf[R40r])),
    (classOf[M01r], List(classOf[M01], classOf[M01r])),
    (classOf[V01R], List(classOf[V01], classOf[V01R])),
    (classOf[V02R], List(classOf[V02], classOf[V02R])),
    (classOf[AD4R], List(classOf[AD4], classOf[AD4R])),
    (classOf[AD5R], List(classOf[AD5], classOf[AD5R]))
  )

}

