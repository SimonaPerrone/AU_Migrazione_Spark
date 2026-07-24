package it.eng.au.scambioDatiGasivori.factory

import it.eng.au.scambioDatiGasivori.controller._
import it.eng.au.scambioDatiGasivori.controller.traits.RunnableAggregatorTrait
import it.eng.au.scambioDatiGasivori.factory.AggregatorLaunchModeEnum._
import it.eng.au.scambioDatiGasivori.utility.Properties
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

object AggregatorFactory {
  @transient lazy val log: Logger = Logger.getLogger(this.getClass)

  private def propToSetOfModes(): List[AggregatorLaunchModeEnum.Value] = {
    Properties.getOutputFileModes
      .split(",") //get all input modes
      .map(_.trim.toUpperCase) //get all mapping
      .map(toValue) //transform strings to AggregatorLaunchModeEnum.Value
      .distinct
      .toList //get a set of modes to avoid executing the same aggregation twice
  }

  def getAggregators: List[RunnableAggregatorTrait] = {
    val executionModes: List[AggregatorLaunchModeEnum.Value] =
      Try(propToSetOfModes()) match {
        case Success(list) => list
        case Failure(exception) =>
          log.error("Error parsing properties. Syntax for output.file.modes is: mode1,mode2 (e.g. output.file.modes=CC,CSEA).")
          throw exception
      }

    executionModes.map({ case mode =>
      mode match {
        case CC => CcAggregator
        case UDB => UdbAggregator
        case UDD => UddAggregator
        case ID => IdAggregator
        case CSEA => CseaAggregator
        case AMM => AmmAggregator
        case _ => throw new Exception(s"Unsupported mode $mode")
      }
    })
  }
}
