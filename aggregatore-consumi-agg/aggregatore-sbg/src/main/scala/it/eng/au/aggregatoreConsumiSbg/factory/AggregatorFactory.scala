package it.eng.au.aggregatoreConsumiSbg.factory

import it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregator.{IdAggregator, ItAggregator, UddAggregator}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.incoerentiDettaglio.{IdIncoerentiDettaglio, ItIncoerentiDettaglio, UdbIncoerentiDettaglio, UddIncoerentiDettaglio}
import it.eng.au.aggregatoreConsumiCommon.controller.traits.RunnableAggregatorTrait
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.controller.impl.aggregator.{IdAggregatorSbg, ItAggregatorSbg, RdbAggregatorSbg, UdbAggregatorSbg, UddAggregatorSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioPdrG.{IdDettaglioGSbg, UdbDettaglioGSbg, UddDettaglioGSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.dettaglioUnico.{IdDettaglioUnicoSbg, UdbDettaglioUnicoSbg, UddDettaglioUnicoSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.dtg.DtgSbg
import it.eng.au.aggregatoreConsumiSbg.controller.impl.excluded.{IdEsclusiSbg, RdbEsclusiSbg, UdbEsclusiSbg, UddEsclusiSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.deltaNegativo.{IdDeltaNegativoSbg, UdbDeltaNegativoSbg, UddDeltaNegativoSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.giroContatore.{IdGiroContatoreSbg, UdbGiroContatoreSbg, UddGiroContatoreSbg}
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerenti._
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiDettaglio._
import it.eng.au.aggregatoreConsumiSbg.factory.AggregatorLaunchModeEnum._
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

object AggregatorFactory {
  @transient lazy val log: Logger = Logger.getLogger(this.getClass)

  // If any of these are selected, we attach coefficient from rcugas to dailyConsumption
  val listOfAggregatorsWithIncoerentiGDM = List(
    UddAggregator, RdbAggregatorSbg, ItAggregator, IdAggregator, UdbAggregatorSbg,
    UddDettaglioUnicoSbg, IdDettaglioUnicoSbg, UdbDettaglioUnicoSbg,
    UddDettaglioGSbg, IdDettaglioGSbg, UdbDettaglioGSbg,
    DtgSbg,
    UddIncoerentiSbg, RdbIncoerentiSbg, ITIncoerentiSbg, IDIncoerentiSbg, UdbIncoerentiSbg,
    UddIncoerentiDettaglio, RdbIncoerentiDettaglioSbg, ItIncoerentiDettaglio, IdIncoerentiDettaglio, UdbIncoerentiDettaglio
  )

  private def propToSetOfCouples(key: String, outputFileCouples: Option[String]): List[(AggregatorLaunchModeEnum.Value, AggregatorLaunchModeEnum.Value)] = {
    outputFileCouples.getOrElse(Environment.getProperty(key))
      .split(",") //get all input couples
      .map(_.split("->").map(_.trim.toUpperCase)) //get all mapping
      .map(a => (a(0), a(1))) // transform array[array[String]] into an array of couples array[(string,string)]
      .map({ case (left, right) => (toValue(left), toValue(right)) }) //transform strings to AggregatorLaunchModeEnum.Value
      .distinct
      .toList //get a set of couples to avoid executing the same aggregation twice
  }

  def getAggregators(outputFileCouples: Option[String]): List[RunnableAggregatorTrait] = {
    val executionModes: List[(AggregatorLaunchModeEnum.Value, AggregatorLaunchModeEnum.Value)] =
      Try(propToSetOfCouples("output.file.couples", outputFileCouples)) match {
        case Success(list) => list
        case Failure(exception) =>
          log.error("Error parsing properties. Syntax for output.file.couples is: left_value->right_value (like file.couples.type=aggregato->AGG1,aggregato->AGG5,esclusi->AGG2).")
          throw exception
      }

    executionModes.map({ case (left, right) =>
      (left, right) match {
        case (AGGREGATO, UDD) => UddAggregatorSbg
        case (AGGREGATO, RDB) => RdbAggregatorSbg
        case (AGGREGATO, IT) => ItAggregatorSbg
        case (AGGREGATO, ID) => IdAggregatorSbg
        case (AGGREGATO, UDB) => UdbAggregatorSbg
        case (DETTAGLIOUNICO, UDD) => UddDettaglioUnicoSbg
        case (DETTAGLIOUNICO, ID) => IdDettaglioUnicoSbg
        case (DETTAGLIOUNICO, UDB) => UdbDettaglioUnicoSbg
        case (ESCLUSI, UDD) => UddEsclusiSbg
        case (ESCLUSI, RDB) => RdbEsclusiSbg
        case (ESCLUSI, ID) => IdEsclusiSbg
        case (ESCLUSI, UDB) => UdbEsclusiSbg
        case (DETTAGLIO, UDD) => UddDettaglioGSbg
        case (DETTAGLIO, ID) => IdDettaglioGSbg
        case (DETTAGLIO, UDB) => UdbDettaglioGSbg
        case (DTG, UDB) => DtgSbg
        case (INCOERENTI, UDD) => UddIncoerentiSbg
        case (INCOERENTI, RDB) => RdbIncoerentiSbg
        case (INCOERENTI, IT) => ITIncoerentiSbg
        case (INCOERENTI, ID) => IDIncoerentiSbg
        case (INCOERENTI, UDB) => UdbIncoerentiSbg
        case (INCOERENTIDETTAGLIO, UDD) => UddIncoerentiDettaglioSbg
        case (INCOERENTIDETTAGLIO, RDB) => RdbIncoerentiDettaglioSbg
        case (INCOERENTIDETTAGLIO, IT) => ItIncoerentiDettaglioSbg
        case (INCOERENTIDETTAGLIO, ID) => IdIncoerentiDettaglioSbg
        case (INCOERENTIDETTAGLIO, UDB) => UdbIncoerentiDettaglioSbg
        case (DELTANEGATIVO, ID) => IdDeltaNegativoSbg
        case (DELTANEGATIVO, UDB) => UdbDeltaNegativoSbg
        case (DELTANEGATIVO, UDD) => UddDeltaNegativoSbg
        case (GIROCONTATORE, ID) => IdGiroContatoreSbg
        case (GIROCONTATORE, UDB) => UdbGiroContatoreSbg
        case (GIROCONTATORE, UDD) => UddGiroContatoreSbg
        case _ => throw new Exception(s"Unsupported Couple $left->$right")
      }
    })
  }
}