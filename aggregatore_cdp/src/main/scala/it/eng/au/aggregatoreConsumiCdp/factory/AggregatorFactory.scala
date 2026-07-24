package it.eng.au.aggregatoreConsumiCdp.factory

import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.RunnableAggregator
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.aggds.{AggdsDistr, AggdsUdb, AggdsUdd}
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.aggric.dettaglioFlussi.{DettaglioFlussiRicDistr, DettaglioFlussiRicUdb, DettaglioFlussiRicUdd}
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.aggric.{AggricDistr, AggricUdb, AggricUdd}
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.dedotti.{DedottiDistr, DedottiUdb, DedottiUdd}
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.prefin.fin.dettaglioFlussi.{DettaglioFlussiFinDistr, DettaglioFlussiFinUdb, DettaglioFlussiFinUdd}
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.prefin.fin.{FinDistr, FinUdb, FinUdd}
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.prefin.pre.dettaglioFlussi.{DettaglioFlussiPreDistr, DettaglioFlussiPreUdb, DettaglioFlussiPreUdd}
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.prefin.pre.{PreDistr, PreUdb, PreUdd}
import it.eng.au.aggregatoreConsumiCdp.factory.AggregatorLaunchModeEnum._
import it.eng.au.aggregatoreConsumiCdp.utility.Environment
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

object AggregatorFactory {
  @transient lazy val log: Logger = Logger.getLogger(this.getClass)

  private def propToSetOfCouples(key: String): List[(AggregatorLaunchModeEnum.Value, AggregatorLaunchModeEnum.Value)] = {
    Environment.getProperty(key)
      .split(",") //get all input couples
      .map(_.split("->").map(_.trim.toUpperCase)) //get all mapping
      .map(a => (a(0), a(1))) // transform array[array[String]] into an array of couples array[(string,string)]
      .map({ case (left, right) => (toValue(left), toValue(right)) }) //transform strings to AggregatorLaunchModeEnum.Value
      .distinct
      .toList //get a set of couples to avoid executing the same aggregation twice
  }

  /***
   * Read variable 'output.file.couples' from properties file and return list of aggregators to perform based on variable
   * value
   */
  def getAggregators: List[RunnableAggregator] = {
    val executionModes: List[(AggregatorLaunchModeEnum.Value, AggregatorLaunchModeEnum.Value)] =
      Try(propToSetOfCouples("output.file.couples")) match {
        case Success(list) => list
        case Failure(exception) =>
          log.error("Error parsing properties. Syntax for output.file.couples is: left_value->right_value (like file.couples.type=aggregato->AGG1,aggregato->AGG5,esclusi->AGG2).")
          throw exception
      }

    executionModes.map({ case (left, right) =>
      (left, right) match {
        case (PRE, UDD) => PreUdd
        case (PRE, DISTR) => PreDistr
        case (PRE, UDB) => PreUdb
        case (FIN, UDD) => FinUdd
        case (FIN, DISTR) => FinDistr
        case (FIN, UDB) => FinUdb
        case (AGGRIC, UDD) => AggricUdd
        case (AGGRIC, DISTR) => AggricDistr
        case (AGGRIC, UDB) => AggricUdb
        case (AGGDS, UDD) => AggdsUdd
        case (AGGDS, DISTR) => AggdsDistr
        case (AGGDS, UDB) => AggdsUdb
        case (DEDOTTI, UDD) => DedottiUdd
        case (DEDOTTI, DISTR) => DedottiDistr
        case (DEDOTTI, UDB) => DedottiUdb
        case (DETTAGLIOFLUSSIPRE, UDD) => DettaglioFlussiPreUdd
        case (DETTAGLIOFLUSSIPRE, DISTR) => DettaglioFlussiPreDistr
        case (DETTAGLIOFLUSSIPRE, UDB) => DettaglioFlussiPreUdb
        case (DETTAGLIOFLUSSIFIN, UDD) => DettaglioFlussiFinUdd
        case (DETTAGLIOFLUSSIFIN, DISTR) => DettaglioFlussiFinDistr
        case (DETTAGLIOFLUSSIFIN, UDB) => DettaglioFlussiFinUdb
        case (DETTAGLIOFLUSSIRIC, UDD) => DettaglioFlussiRicUdd
        case (DETTAGLIOFLUSSIRIC, DISTR) => DettaglioFlussiRicDistr
        case (DETTAGLIOFLUSSIRIC, UDB) => DettaglioFlussiRicUdb
        case _ => throw new Exception(s"Unsupported Couple $left->$right")
      }
    })
  }
}
