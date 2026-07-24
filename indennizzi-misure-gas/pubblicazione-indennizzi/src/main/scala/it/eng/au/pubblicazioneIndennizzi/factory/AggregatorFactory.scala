package it.eng.au.pubblicazioneIndennizzi.factory

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import it.eng.au.pubblicazioneIndennizzi.controller.impl.aggregator.{DettaglioPDRIZG1, DettaglioPDRIZG2, IZG1Aggregator, IZG2Aggregator}
import it.eng.au.pubblicazioneIndennizzi.controller.traits.RunnableAggregator
import it.eng.au.pubblicazioneIndennizzi.factory.AggregatorLaunchModeEnum._
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
        case (IZG, ID) => IZG1Aggregator
        case (IZG, UDD) => IZG2Aggregator
        case (DETTAGLIO, ID) => DettaglioPDRIZG1
        case (DETTAGLIO, UDD) => DettaglioPDRIZG2
        case _ => throw new Exception(s"Unsupported Couple $left->$right")
      }
    })
  }
}



