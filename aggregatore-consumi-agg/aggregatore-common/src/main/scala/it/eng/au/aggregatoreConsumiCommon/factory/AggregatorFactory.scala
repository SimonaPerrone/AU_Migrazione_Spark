package it.eng.au.aggregatoreConsumiCommon.factory

import it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregator._
import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioPdrG.{IdDettaglioG, UdbDettaglioG, UddDettaglioG}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.{IdDettaglioUnico, UdbDettaglioUnico, UddDettaglioUnico}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.dtg.Dtg
import it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded.{IdEsclusi, RdbEsclusi, UdbEsclusi, UddEsclusi}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.deltaNegativo.{IdDeltaNegativo, UdbDeltaNegativo, UddDeltaNegativo}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.giroContatore.{IdGiroContatore, UdbGiroContatore, UddGiroContatore}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerenti._
import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.pdr._
import it.eng.au.aggregatoreConsumiCommon.controller.traits.RunnableAggregatorTrait
import it.eng.au.aggregatoreConsumiCommon.factory.AggregatorLaunchModeEnum._
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

/** Si occupa dell'estrazione delle pubblicazioni (Aggregato, Esclusi, Dettaglio, ...) da eseguire. */
object AggregatorFactory {
  @transient lazy val log: Logger = Logger.getLogger(this.getClass)

  /**
   * <p>A partire da [[key]], estrae la coppia (pubblicazione, ruolo) da eseguire.</p>
   * E.g. {{{key="AGGREGATO->AGG1,ESCLUSI->AGG3"}}} diventa {{{(AGGREGATO, UDD), (ESCLUSI, IT)}}}
   * @param key stringa contenente le modalità
   * @return le modalità (pubblicazione, ruolo) da eseguire
   */
  private def propToSetOfCouples(key: String): List[(AggregatorLaunchModeEnum.Value, AggregatorLaunchModeEnum.Value)] = {
    Environment.getProperty(key)
      .split(",") //get all input couples
      .map(_.split("->").map(_.trim.toUpperCase)) //get all mapping
      .map(a => (a(0), a(1))) // transform array[array[String]] into an array of couples array[(string,string)]
      .map({ case (left, right) => (toValue(left), toValue(right)) }) //transform strings to AggregatorLaunchModeEnum.Value
      .distinct
      .toList //get a set of couples to avoid executing the same aggregation twice
  }

  /**
   * Dall'output di [[propToSetOfCouples]] ottiene la lista di [[RunnableAggregatorTrait]] da eseguire. In caso di match mancante, restituisce un'eccezione.
   * @return la lista di [[RunnableAggregatorTrait]] da eseguire
   */
  def getAggregators: List[RunnableAggregatorTrait] = {
    val executionModes: List[(AggregatorLaunchModeEnum.Value, AggregatorLaunchModeEnum.Value)] =
      Try(propToSetOfCouples("output.file.couples")) match {
        case Success(list) => list
        case Failure(exception) =>
          log.error("Error parsing properties. Syntax for output.file.couples is: left_value->right_value (like file.couples.type=aggregato->AGG1,aggregato->AGG5,esclusi->AGG2).")
          throw exception
      }

    executionModes.map({ case (left, right) =>
      (left, right) match {
        case (AGGREGATO, UDD) => UddAggregator
        case (AGGREGATO, RDB) => RdbAggregator
        case (AGGREGATO, IT) => ItAggregator
        case (AGGREGATO, ID) => IdAggregator
        case (AGGREGATO, UDB) => UdbAggregator
        case (DETTAGLIOUNICO, UDD) => UddDettaglioUnico
        case (DETTAGLIOUNICO, ID) => IdDettaglioUnico
        case (DETTAGLIOUNICO, UDB) => UdbDettaglioUnico
        case (ESCLUSI, UDD) => UddEsclusi
        case (ESCLUSI, RDB) => RdbEsclusi
        case (ESCLUSI, ID) => IdEsclusi
        case (ESCLUSI, UDB) => UdbEsclusi
        case (DETTAGLIO, UDD) => UddDettaglioG
        case (DETTAGLIO, ID) => IdDettaglioG
        case (DETTAGLIO, UDB) => UdbDettaglioG
        case (DTG, UDB) => Dtg
        case (INCOERENTI, UDD) => UddIncoerenti
        case (INCOERENTI, RDB) => RdbIncoerenti
        case (INCOERENTI, IT) => ITIncoerenti
        case (INCOERENTI, ID) => IDIncoerenti
        case (INCOERENTI, UDB) => UdbIncoerenti
        case (INCOERENTIDETTAGLIO, UDD) => UddPdrIncoerentiDettaglio
        case (INCOERENTIDETTAGLIO, RDB) => RdbPdrIncoerentiDettaglio
        case (INCOERENTIDETTAGLIO, IT) => ItPdrIncoerentiDettaglio
        case (INCOERENTIDETTAGLIO, ID) => IdPdrIncoerentiDettaglio
        case (INCOERENTIDETTAGLIO, UDB) => UdbPdrIncoerentiDettaglio
        case (DELTANEGATIVO, ID) => IdDeltaNegativo
        case (DELTANEGATIVO, UDB) => UdbDeltaNegativo
        case (DELTANEGATIVO, UDD) => UddDeltaNegativo
        case (GIROCONTATORE, ID) => IdGiroContatore
        case (GIROCONTATORE, UDB) => UdbGiroContatore
        case (GIROCONTATORE, UDD) => UddGiroContatore
        case _ => throw new Exception(s"Unsupported Couple $left->$right")
      }
    })
  }
}
