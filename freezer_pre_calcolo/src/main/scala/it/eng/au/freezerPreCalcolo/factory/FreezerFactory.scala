package it.eng.au.freezerPreCalcolo.factory

import it.eng.au.freezerPreCalcolo.factory.FactoryLaunchModeEnum._
import it.eng.au.freezerPreCalcolo.freezer.flow.{RcuGasMassivoFlow, RunnableFreezer}
import org.apache.log4j.Logger

import it.eng.au.freezerPreCalcolo.utility.environment.Environment

import scala.util.{Failure, Success, Try}

/** Manages the parsing of properties in order to get execution modes. */
object FreezerFactory {
  @transient lazy val log: Logger = Logger.getLogger(this.getClass)

  private def propToSetOfCouples(key: String): List[FactoryLaunchModeEnum.Value] = {
    Environment.getProperty(key)
      .split(",") //get all input couples
      .map(_.trim.toUpperCase) //get all mapping
      .map(a => toValue(a)) //transform strings to FactoryLaunchModeEnum.Value
      .distinct
      .toList //get a set string to table run
  }

  /** Parses the properties looking for execution modes, and gives them back to the driver. */
  def getAggregators: List[RunnableFreezer] = {
    val executionModes: List[FactoryLaunchModeEnum.Value] =
      Try(propToSetOfCouples("input.table.freezer")) match {
        case Success(list) => list
        case Failure(exception) =>
          log.error("Error parsing properties. Syntax for input.table.freezer is: nametable.")
          throw exception
      }

    executionModes.map({
      case RCUGASMASSIVOTECH => new RcuGasMassivoFlow
      case table => throw new Exception(s"Unsupported table: $table")
    })
  }
}
