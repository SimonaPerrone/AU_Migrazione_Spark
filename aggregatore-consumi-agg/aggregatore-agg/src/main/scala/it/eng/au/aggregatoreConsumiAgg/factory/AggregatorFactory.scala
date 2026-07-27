package it.eng.au.aggregatoreConsumiAgg.factory

import it.eng.au.aggregatoreConsumiAgg.factory.AggregatorLaunchModeEnum._
import it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregator._
import it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregatorTripla.RdbAggregatorTripla
import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioPdrG.{IdDettaglioG, UdbDettaglioG, UddDettaglioG}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.{IdDettaglioUnico, UdbDettaglioUnico, UddDettaglioUnico}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.dtg.Dtg
import it.eng.au.aggregatoreConsumiCommon.controller.impl.excluded._
import it.eng.au.aggregatoreConsumiCommon.controller.impl.deltaNegativo.{IdDeltaNegativo, UdbDeltaNegativo, UddDeltaNegativo}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.giroContatore.{IdGiroContatore, UdbGiroContatore, UddGiroContatore}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerenti._
import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio._
import it.eng.au.aggregatoreConsumiCommon.controller.traits.RunnableAggregatorTrait
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

object AggregatorFactory {
  @transient lazy val log: Logger = Logger.getLogger(this.getClass)

  // If any of these are selected, we attach coefficient from rcugas to dailyConsumption
  val listOfAggregatorsWithIncoerentiGDM = List(
    UddAggregator, RdbAggregator, ItAggregator, IdAggregator, UdbAggregator,
    UddDettaglioUnico, IdDettaglioUnico, UdbDettaglioUnico,
    UddDettaglioG, IdDettaglioG, UdbDettaglioG,
    Dtg,
    UddIncoerenti, RdbIncoerenti, ITIncoerenti, IDIncoerenti, UdbIncoerenti,
    UddIncoerentiDettaglio, RdbIncoerentiDettaglio, ItIncoerentiDettaglio, IdIncoerentiDettaglio, UdbIncoerentiDettaglio
  )

  private def propToSetOfCouples(key: String): List[(AggregatorLaunchModeEnum.Value, AggregatorLaunchModeEnum.Value)] = {
    Environment.getProperty(key)
      .split(",") //get all input couples
      .map(_.split("->").map(_.trim.toUpperCase)) //get all mapping
      .map(a => (a(0), a(1))) // transform array[array[String]] into an array of couples array[(string,string)]
      .map({ case (left, right) => (toValue(left), toValue(right)) }) //transform strings to AggregatorLaunchModeEnum.Value
      .distinct
      .toList //get a set of couples to avoid executing the same aggregation twice
  }

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
        case (INCOERENTIDETTAGLIO, UDD) => UddIncoerentiDettaglio
        case (INCOERENTIDETTAGLIO, RDB) => RdbIncoerentiDettaglio
        case (INCOERENTIDETTAGLIO, IT) => ItIncoerentiDettaglio
        case (INCOERENTIDETTAGLIO, ID) => IdIncoerentiDettaglio
        case (INCOERENTIDETTAGLIO, UDB) => UdbIncoerentiDettaglio
        case (DELTANEGATIVO, ID) => IdDeltaNegativo
        case (DELTANEGATIVO, UDB) => UdbDeltaNegativo
        case (DELTANEGATIVO, UDD) => UddDeltaNegativo
        case (GIROCONTATORE, ID) => IdGiroContatore
        case (GIROCONTATORE, UDB) => UdbGiroContatore
        case (GIROCONTATORE, UDD) => UddGiroContatore
        case (AGGREGATO_TRIPLA, RDB) => RdbAggregatorTripla
        case _ => throw new Exception(s"Unsupported Couple $left->$right")
      }
    })
  }
}
