package it.eng.au.queryReport.factory

import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.queryReport.factory.QueryFactoryLunchModeEnum._
import it.eng.au.queryReport.query._
import it.eng.au.queryReport.query.dettaglioIncoerenti.QueryDettaglioIncoerenti
import it.eng.au.queryReport.query.dettaglioUnico.QueryDettaglioUnico
import it.eng.au.queryReport.query.deltaNegativo.QueryDettaglioDeltaNegativo
import it.eng.au.queryReport.query.giroContatore.QueryDettaglioGiroContatore
import it.eng.au.queryReport.query.esclusi.QueryDettaglioEsclusi
import it.eng.au.queryReport.query.traits.QueryTrait
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

object QueryFactory {
  @transient lazy val log: Logger = Logger.getLogger(this.getClass)

  // If any of these are selected, we attach coefficient from rcugas to dailyConsumption
  val listOfQueriesWithIncoerentiGDM = List(QueryAggregato, QueryDettaglioUnico, QueryIncoerenti, QueryDettaglioIncoerenti, QueryDettaglioDeltaNegativo, QueryDettaglioGiroContatore)

  private def propToSetOfCouples(key: String): List[QueryFactoryLunchModeEnum.Value] = {
    Environment.getProperty(key)
      .toUpperCase
      .split(",")
      .map(value => value.trim)
      .map(toValue)
      .distinct
      .toList
  }

  def getQueries(args: Array[String]): List[QueryTrait] = {
    val executionModes: List[QueryFactoryLunchModeEnum.Value] =
      if (Environment.getProperty("properties.mode") == "true") {
        Try(propToSetOfCouples("query.to.run")) match {
          case Success(list) => list
          case Failure(exception) =>
            log.error("Error parsing properties. Admissible values: aggregato,esclusi,incoerenti,dettaglioIncoerenti,dettaglioUnico,sospesi,deltaNegativo,giroContatore.")
            throw exception
        }
      }
      else {
        ArgsFactory.parse(args)
          .queries
          .map(toValue)
          .distinct
      }


    executionModes
      .map({
        case DETTAGLIOUNICO => QueryDettaglioUnico
        case SOSPESI => QuerySospesi
        case AGGREGATO => QueryAggregato
        case ESCLUSI => QueryDettaglioEsclusi
        case INCOERENTI => QueryIncoerenti
        case DETTAGLIOINCOERENTI => QueryDettaglioIncoerenti
        case DELTANEGATIVO => QueryDettaglioDeltaNegativo
        case GIROCONTATORE => QueryDettaglioGiroContatore
      })
  }
}
