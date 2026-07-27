package it.sferanet.au.controller.validation

import it.sferanet.au.controller.validation.ValidationController.{g1, g2, g3, g4}
import it.sferanet.au.model.Flow
import it.sferanet.au.model.autolettura._
import it.sferanet.au.model.periodico._
import it.sferanet.au.model.prestazionale._
import it.sferanet.au.model.rettifica._
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.util.Date

class ValidationController {

  /** Ottiene le misure validate in tre step:
   *   1. Applicazione delle logiche di annullamento e di rettifica
   *   1. Filtro delle misure invalide
   *   1. Applicazione delle priorità tra le misure
   *   */
  def getMeasures(measures: RDD[Flow]): RDD[Flow] = {
    // measures contiene l'ultima versione dei dati caricati in Hive (d_caricamento più alto)
    // 1: applicazione motore di gestione degli annullamenti e delle rettifiche dei flussi di cambio misuratore
    val rectifiedMeasures = applyRectificationLogic(measures)

    // 2: filtraggio misure non valide DOPO applicazione logica annullamenti/rettifiche
    // (è corretto filtrare DOPO gli annullamenti/rettifiche per evitare di filtrare in anticipo informazioni utili da annullare/rettificare con la possibilità di applicarli alle misure errate)
    val measuresFiltered = filterInvalidMeasures(rectifiedMeasures)

    // 3: applicazione motore di validazione e delle priorità
    getPriorityMeasures(measuresFiltered)
  }

  def filterInvalidMeasures(measures: RDD[Flow]): RDD[Flow] = {
    // filtraggio mantenendo soltanto le misure che hanno prelevata definita, misure dei flussi di cambio misuratore e gli annullamenti
    // (importante far passare anche le misure dei flussi di cambio misuratore con prelevata perchè servono per le logiche successive)
    measures.filter(f => f.measure.isDefined || CambioMisuratoreController.isCambioMisuratoreFlowWithRett(f) || Set(3, 6).contains(f.motivation.getOrElse(-1)))
  }

  def applyRectificationLogic(measures: RDD[Flow]): RDD[Flow] = {
    // divisione del set di misure in gruppi ed esecuzione degli annullamenti e rettifiche ai flussi di cambio misuratore

    val group1 = getMeasuresGroup(measures, 1)
    val outputGroup1 = CancelController.cancelMeasures(group1, 1)

    val group2 = getMeasuresGroup(measures, 2).union(outputGroup1).coalesce(measures.getNumPartitions)
    val group3 = getMeasuresGroup(measures, 3)
    val group4 = getMeasuresGroup(measures, 4)

    val partialOutputGroup2 = CambioMisuratoreController.rettificaCambioMisuratore(group2)
    val partialOutputGroup2Pre = CancelController.cancelIgmrAndIgmgWithIgmrMot3MeasuresPre(partialOutputGroup2)
    val outputGroup2 = CancelController.cancelIgmrAndIgmgWithIgmrMot3MeasuresPost(partialOutputGroup2Pre)
    val outputGroup3 = CancelController.cancelMeasures(group3, 3)
    val outputGroup4 = CancelController.cancelMeasures(group4, 4)

    outputGroup2.union(outputGroup3).union(outputGroup4)
      .coalesce(Environment.getNumberPartition.toInt)
  }

  def getMeasuresGroup(measures: RDD[Flow], group: Int): RDD[Flow] = {
    // divisione del set di misure in gruppi sulla base della descrizione dei singoli insiemi
    val groupDefinition = group match {
      case 1 => g1
      case 2 => g2
      case 3 => g3
      case 4 => g4
    }
    measures.filter(f =>
      groupDefinition.exists({ case (flowType, motivations) =>
        flowType.isInstance(f) && (motivations.isEmpty || motivations.contains(f.motivation.getOrElse(-1)))
      })
    )
  }

  /** A parità di PdR e data, mantiene soltanto la misura con la priorità più alta. */
  def getPriorityMeasures(measures: RDD[Flow]): RDD[Flow] = {
    measures
      .keyBy(f => (f.pdr, f.date))
      .groupByKey()
      .values
      .flatMap(ValidationController.getHighestPriorityMeasures)
  }

}

object ValidationController {

  def getLastLoadingMeasuresVersion(measures: RDD[Flow]): RDD[Flow] = {
    measures.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val isTimestampCorrect: Flow => Boolean = f => f.timestampLocalFile != new Date(0)

    val infoCorrectlyFormattedMeasures = measures
      .filter(isTimestampCorrect)
      // viene utilizzata la parte del nome del file da local_file come chiave per ricavare il max_d_caricamento
      // viene considerata la possibilità di presenza dello stesso nome file in diversi giorni e con diversi d_caricamento
      // verrà data maggiore priorità alla versione caricata più recentemente da distributore e ingerita più recentemente in Hive.
      // È importate usare la funzione toUpper sul nome del file per evitare problematiche legate a casistiche strane del case
      .map(f => (f.fileNameLocalFile.toUpperCase, (f.dateLoadFromLocalFile, f.d_caricamento.getOrElse(new Date(0L)).getTime)))
      .reduceByKey({ case (a, b) => List(a, b).max })
      .map(f => ((f._1, f._2._2), ()))

    val infoBadlyFormattedMeasures = measures
      .filter(!isTimestampCorrect(_))
      // nel caso in cui il timestamp risulta essere mal formattato, non possiamo eseguire il controllo precedente
      // (ovvero raggruppare le misure per fileNameLocalFile. In questo caso, è necessario eseguire una keyBy per (pdr, date),
      // e selezionare le misure ordinando per d_caricamento più recente.
      .map(f => ((f.pdr, f.date), f.d_caricamento.getOrElse(new Date(0L)).getTime))
      .reduceByKey(math.max)
      .map(f => ((f._1._1, f._1._2, f._2), ()))

    val correctlyFormattedMeasures = measures
      .filter(isTimestampCorrect)
      .keyBy(f => (f.fileNameLocalFile.toUpperCase, f.d_caricamento.getOrElse(new Date(0L)).getTime))
      .join(infoCorrectlyFormattedMeasures)
      .values
      .map(_._1)

    val badlyFormattedMeasures = measures
      .filter(!isTimestampCorrect(_))
      .keyBy(f => (f.pdr, f.date, f.d_caricamento.getOrElse(new Date(0L)).getTime))
      .join(infoBadlyFormattedMeasures)
      .values
      .map(_._1)

    correctlyFormattedMeasures.union(badlyFormattedMeasures).coalesce(correctlyFormattedMeasures.getNumPartitions)
  }

  def getHighestPriorityMeasures(measures: Iterable[Flow]): List[Flow] = {
    val measuresWithPriority = measures.toList.distinct.map(f => {
      val priority = rules.find({ case (rule, _) => rule(f) }).map(_._2)
      (f, priority)
    })

    // al flusso più importante verrà assegnato il numero intero più piccolo
    val defaultPriority = 10000 // priorità di default per le misure che non devono essere considerate nel calcolo
    val minPriority = measuresWithPriority.map(_._2.getOrElse(defaultPriority)).min
    if (minPriority != defaultPriority) {
      val measuresMinPriority = measuresWithPriority.filter(_._2.getOrElse(defaultPriority) == minPriority).map(_._1)
      // ottengo il numero di flussi da ritornare in output: 2 se i flussi sono di cambio misuratore (IM1/IGMG), 1 altrimenti
      val measuresNumberToReturn = if (CambioMisuratoreController.isCambioMisuratoreFlowWithRett(measuresMinPriority.head)) 2 else 1
      if (measuresMinPriority.size == measuresNumberToReturn) {
        measuresMinPriority
      } else {
        // per eliminare i doppi prendo le misure con data di caricamento nel cloud da parte del distributore più alta, timestamp nel nome del file più alto, tracciato STD con maggiore priorità rispetto a vecchio tracciato, numero progressivo più alto
        // IMPORTANTE: a questo punto non viene effettuato in controllo su d_caricamento in quanto in precedenza è stata già ottenuta soltanto l'ultima versione dei dati caricati in Hive
        val maxLoadingInfo = measuresMinPriority.map(i => (i.dateLoadFromLocalFile, i.timestampLocalFile, i.isNewRoute, i.progressiveLocalFile)).max
        val maxLoadingMeasures = measuresMinPriority.filter(i => (i.dateLoadFromLocalFile, i.timestampLocalFile, i.isNewRoute, i.progressiveLocalFile) == maxLoadingInfo)
        if (maxLoadingMeasures.size == measuresNumberToReturn)
          maxLoadingMeasures
        else if (measuresNumberToReturn == 1) // NON IM1/IGMG
          List[Flow]()
        else { // gestione particolare di entries duplicate IM1/IGMG: se entries duplicate ne scelgo una, altrimenti se almeno un campo differente scarto tutto
          // a questo punto dentro maxLoadingMeasures ho solo IM1 o solo IGMG
          val (misurePre, misurePost) = if (maxLoadingMeasures.head.service.startsWith("IM1"))
            (maxLoadingMeasures.filter(f => f.isInstanceOf[Im1Pre]).map(_.asInstanceOf[Im1Pre]),
              maxLoadingMeasures.filter(f => f.isInstanceOf[Im1Post]).map(_.asInstanceOf[Im1Post]))
          else // IGMG
            (maxLoadingMeasures.filter(f => f.isInstanceOf[IgmgPre]).map(_.asInstanceOf[IgmgPre]),
              maxLoadingMeasures.filter(f => f.isInstanceOf[IgmgPost]).map(_.asInstanceOf[IgmgPost]))

          if (misurePre.distinct.length == 1 && misurePost.distinct.length == 1)
            List[Flow](misurePre.head.asInstanceOf[Flow], misurePost.head.asInstanceOf[Flow])
          else
            List[Flow]()
        }
      }
    } else {
      List[Flow]()
    }
  }

  val g1 = Map(
    classOf[Tgl] -> List[Int](),
    classOf[Tml] -> List[Int](),
    classOf[Rgl] -> List[Int](1, 2, 3, 4, 5),
    classOf[Rml] -> List[Int](1, 2, 3, 4, 5)
  )
  // g2 andrà in union con il risultato delle cancellazioni del gruppo 1 (g1)
  val g2 = Map(
    classOf[Im1Pre] -> List[Int](),
    classOf[Im1Post] -> List[Int](),
    classOf[IgmgPre] -> List[Int](),
    classOf[IgmgPost] -> List[Int](),
    classOf[IgmrPre] -> List[Int](),
    classOf[IgmrPost] -> List[Int]()
  )
  val g3 = Map(
    classOf[Tal] -> List[Int](),
    classOf[Tas] -> List[Int](),
    classOf[Tav] -> List[Int](),
    classOf[Rgl] -> List[Int](6),
    classOf[Rml] -> List[Int](6)
  )
  // g4 tira contiene tutti i restanti tipi di flussi, per poi applicare gli annullamenti (motivazione 3)
  val g4 = Map(
    classOf[Rmv] -> List[Int](),
    classOf[Tmv] -> List[Int](),
    classOf[Rsl] -> List[Int](),
    classOf[Swg1] -> List[Int](),
    classOf[FUI] -> List[Int](),
    classOf[FDD] -> List[Int](),
    classOf[Sw1] -> List[Int](),
    classOf[A01R] -> List[Int](),
    classOf[A01] -> List[Int](),
    classOf[A40R] -> List[Int](),
    classOf[A40] -> List[Int](),
    classOf[SM1R] -> List[Int](),
    classOf[Sm1] -> List[Int](),
    classOf[AD2R] -> List[Int](),
    classOf[AD2] -> List[Int](),
    classOf[AD3R] -> List[Int](),
    classOf[AD3] -> List[Int](),
    classOf[AD4R] -> List[Int](),
    classOf[AD4] -> List[Int](),
    classOf[AD5R] -> List[Int](),
    classOf[AD5] -> List[Int](),
    classOf[A02R] -> List[Int](),
    classOf[A02] -> List[Int](),
    classOf[S02R] -> List[Int](),
    classOf[S02] -> List[Int](),
    classOf[S40R] -> List[Int](),
    classOf[S40] -> List[Int](),
    classOf[R01r] -> List[Int](),
    classOf[R01] -> List[Int](),
    classOf[R40r] -> List[Int](),
    classOf[R40] -> List[Int](),
    classOf[M01r] -> List[Int](),
    classOf[M01] -> List[Int](),
    classOf[V01R] -> List[Int](),
    classOf[V01] -> List[Int](),
    classOf[V02R] -> List[Int](),
    classOf[V02] -> List[Int]()
  )

  /**
   * CR 22/08/2022 Gabrini Federico
   * gestione rettifiche 4,5: avremmo la necessità di ingerire e gestire tutte le rettifiche con motivazione 4 o 5 in modo tale che queste abbiano priorità massima rispetto agli altri flussi;
   */
  val rules: List[(Flow => Boolean, Int)] = List(
    (f: Flow) => Set(2, 3, 4, 5).contains(
      f match {
        case igmrPre: IgmrPre => igmrPre.asInstanceOf[IgmrPre].motivation.getOrElse(-1)
        case igmrPost: IgmrPost => igmrPost.asInstanceOf[IgmrPost].motivation.getOrElse(-1)
        case _ => -1
      }
    ),
    (f: Flow) => f.isInstanceOf[Rml] && Set(1, 2, 4, 5).contains(f.asInstanceOf[Rml].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Rgl] && Set(4, 5).contains(f.asInstanceOf[Rgl].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Rmv] && Set(4, 5).contains(f.asInstanceOf[Rmv].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Rsl] && Set(4, 5).contains(f.asInstanceOf[Rsl].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[A01R] && Set(4, 5).contains(f.asInstanceOf[A01R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[A40R] && Set(4, 5).contains(f.asInstanceOf[A40R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[SM1R] && Set(4, 5).contains(f.asInstanceOf[SM1R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[AD2R] && Set(4, 5).contains(f.asInstanceOf[AD2R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[AD3R] && Set(4, 5).contains(f.asInstanceOf[AD3R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[AD4R] && Set(4, 5).contains(f.asInstanceOf[AD4R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[AD5R] && Set(4, 5).contains(f.asInstanceOf[AD5R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[A02R] && Set(4, 5).contains(f.asInstanceOf[A02R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[S02R] && Set(4, 5).contains(f.asInstanceOf[S02R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[S40R] && Set(4, 5).contains(f.asInstanceOf[S40R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[R01r] && Set(4, 5).contains(f.asInstanceOf[R01r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[R40r] && Set(4, 5).contains(f.asInstanceOf[R40r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[M01r] && Set(4, 5).contains(f.asInstanceOf[M01r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[V01R] && Set(4, 5).contains(f.asInstanceOf[V01R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[V02R] && Set(4, 5).contains(f.asInstanceOf[V02R].motivation.getOrElse(-1)),
    (f: Flow) => Set('E', 'S').contains(
      f match {
        case igmgPre: IgmgPre => igmgPre.asInstanceOf[IgmgPre].readType.getOrElse('-')
        case igmgPost: IgmgPost => igmgPost.asInstanceOf[IgmgPost].readType.getOrElse('-')
        case _ => '-'
      }
    ),
    (f: Flow) => Set('E', 'S').contains(
      f match {
        case im1Pre: Im1Pre => im1Pre.asInstanceOf[Im1Pre].readType.getOrElse('-')
        case im1Post: Im1Post => im1Post.asInstanceOf[Im1Post].readType.getOrElse('-')
        case _ => '-'
      }
    ),
    (f: Flow) => f.isInstanceOf[Rgl] && Set(1, 2).contains(f.asInstanceOf[Rgl].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Tgl] && ((f.isNewRoute || (!f.isNewRoute && Set("SI").contains(f.asInstanceOf[Tgl].isValid.getOrElse("-")))) && Set('E').contains(f.asInstanceOf[Tgl].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[Tml] && ((f.isNewRoute || (!f.isNewRoute && Set("SI").contains(f.asInstanceOf[Tml].isValid.getOrElse("-")))) && Set('E').contains(f.asInstanceOf[Tml].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[Rmv] && Set(1, 2).contains(f.asInstanceOf[Rmv].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Tmv] && Set('E').contains(f.asInstanceOf[Tmv].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Tal] && Set('V').contains(f.asInstanceOf[Tal].outcome.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Tav] && Set('V').contains(f.asInstanceOf[Tav].outcome.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Tas] && Set('V').contains(f.asInstanceOf[Tas].outcome.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Rsl] && Set(1, 2).contains(f.asInstanceOf[Rsl].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Swg1] && Set('E').contains(f.asInstanceOf[Swg1].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[FUI] && Set('E').contains(f.asInstanceOf[FUI].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[FDD] && Set('E').contains(f.asInstanceOf[FDD].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Sw1] && Set('E').contains(f.asInstanceOf[Sw1].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[A01R] && Set(1, 2).contains(f.asInstanceOf[A01R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[A01] && ((!f.isNewRoute && f.asInstanceOf[A01].outcome.contains('1')) || (f.isNewRoute && Set('E').contains(f.asInstanceOf[A01].readType.getOrElse('-')))),
    (f: Flow) => f.isInstanceOf[A40R] && Set(1, 2).contains(f.asInstanceOf[A40R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[A40] && ((!f.isNewRoute && f.asInstanceOf[A40].outcome.contains('1')) || (f.isNewRoute && Set('E').contains(f.asInstanceOf[A40].readType.getOrElse('-')))),
    (f: Flow) => f.isInstanceOf[SM1R] && Set(1, 2).contains(f.asInstanceOf[SM1R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Sm1] && ((!f.isNewRoute && f.asInstanceOf[Sm1].outcome.contains('1')) && Set('E').contains(f.asInstanceOf[Sm1].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[AD2R] && Set(1, 2).contains(f.asInstanceOf[AD2R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[A01R] && Set(1, 2).contains(f.asInstanceOf[A01R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[A01] && ((!f.isNewRoute && f.asInstanceOf[A01].outcome.contains('1')) || (f.isNewRoute && Set('E').contains(f.asInstanceOf[A01].readType.getOrElse('-')))),
    (f: Flow) => f.isInstanceOf[A40R] && Set(1, 2).contains(f.asInstanceOf[A40R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[A40] && ((!f.isNewRoute && f.asInstanceOf[A40].outcome.contains('1')) || (f.isNewRoute && Set('E').contains(f.asInstanceOf[A40].readType.getOrElse('-')))),
    (f: Flow) => f.isInstanceOf[SM1R] && Set(1, 2).contains(f.asInstanceOf[SM1R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[Sm1] && ((!f.isNewRoute && f.asInstanceOf[Sm1].outcome.contains('1')) && Set('E').contains(f.asInstanceOf[Sm1].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[AD2R] && Set(1, 2).contains(f.asInstanceOf[AD2R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[AD2] && Set('E').contains(f.asInstanceOf[AD2].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[AD3R] && Set(1, 2).contains(f.asInstanceOf[AD3R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[AD3] && Set('E').contains(f.asInstanceOf[AD3].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[AD4R] && Set(1, 2).contains(f.asInstanceOf[AD4R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[AD4] && Set('E').contains(f.asInstanceOf[AD4].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[AD5R] && Set(1, 2).contains(f.asInstanceOf[AD5R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[AD5] && Set('E').contains(f.asInstanceOf[AD5].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[A02R] && Set(1, 2).contains(f.asInstanceOf[A02R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[A02] && (!f.isNewRoute || (f.isNewRoute && Set('E').contains(f.asInstanceOf[A02].readType.getOrElse('-')))),
    (f: Flow) => f.isInstanceOf[S02R] && Set(1, 2).contains(f.asInstanceOf[S02R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[S02] && Set('E').contains(f.asInstanceOf[S02].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[S40R] && Set(1, 2).contains(f.asInstanceOf[S40R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[S40] && Set('E').contains(f.asInstanceOf[S40].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[R01r] && Set(1, 2).contains(f.asInstanceOf[R01r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[R01] && ((!f.isNewRoute && f.asInstanceOf[R01].outcome.contains('1')) || (f.isNewRoute && Set('E').contains(f.asInstanceOf[R01].readType.getOrElse('-')))),
    (f: Flow) => f.isInstanceOf[R40r] && Set(1, 2).contains(f.asInstanceOf[R40r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[R40] && Set('E').contains(f.asInstanceOf[R40].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[M01r] && Set(1, 2).contains(f.asInstanceOf[M01r].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[M01] && Set('E').contains(f.asInstanceOf[M01].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[V01R] && Set(1, 2).contains(f.asInstanceOf[V01R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[V01] && Set('E').contains(f.asInstanceOf[V01].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[V02R] && Set(1, 2).contains(f.asInstanceOf[V02R].motivation.getOrElse(-1)),
    (f: Flow) => f.isInstanceOf[V02] && Set('E').contains(f.asInstanceOf[V02].readType.getOrElse('-')),
    (f: Flow) => f.isInstanceOf[Tml] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[Tml].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[Tmv] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[Tmv].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[Swg1] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[Swg1].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[FUI] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[FUI].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[FDD] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[FDD].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[A01] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[A01].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[A40] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[A40].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[Sm1] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[Sm1].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[AD2] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[AD2].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[AD3] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[AD3].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[AD4] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[AD4].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[AD5] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[AD5].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[A02] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[A02].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[S02] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[S02].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[S40] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[S40].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[R01] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[R01].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[R40] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[R40].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[M01] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[M01].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[V01] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[V01].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[V02] && (f.isNewRoute && Set('A').contains(f.asInstanceOf[V02].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[A01] && (f.isNewRoute && Set('S').contains(f.asInstanceOf[A01].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[A40] && (f.isNewRoute && Set('S').contains(f.asInstanceOf[A40].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[Sm1] && (f.isNewRoute && Set('S').contains(f.asInstanceOf[Sm1].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[AD2] && (f.isNewRoute && Set('S').contains(f.asInstanceOf[AD2].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[AD3] && (f.isNewRoute && Set('S').contains(f.asInstanceOf[AD3].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[AD4] && (f.isNewRoute && Set('S').contains(f.asInstanceOf[AD4].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[AD5] && (f.isNewRoute && Set('S').contains(f.asInstanceOf[AD5].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[A02] && (f.isNewRoute && Set('S').contains(f.asInstanceOf[A02].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[S02] && (f.isNewRoute && Set('S').contains(f.asInstanceOf[S02].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[S40] && (f.isNewRoute && Set('S').contains(f.asInstanceOf[S40].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[R01] && (f.isNewRoute && Set('S').contains(f.asInstanceOf[R01].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[R40] && (f.isNewRoute && Set('S').contains(f.asInstanceOf[R40].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[M01] && (f.isNewRoute && Set('S').contains(f.asInstanceOf[M01].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[V01] && (f.isNewRoute && Set('S').contains(f.asInstanceOf[V01].readType.getOrElse('-'))),
    (f: Flow) => f.isInstanceOf[V02] && (f.isNewRoute && Set('S').contains(f.asInstanceOf[V02].readType.getOrElse('-')))
  ).zipWithIndex

}
