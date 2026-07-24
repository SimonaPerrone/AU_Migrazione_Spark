package it.au.misure.calcolo_capacita.component.utility.check

import it.au.misure.calcolo_capacita.component.implementation.Calculation
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant._
import it.au.misure.calcolo_capacita.component.utility.LoggerUtility
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ConvertStringIntoDate._
import it.au.misure.calcolo_capacita.component.utility.property.ApplicationProperty.format
import org.joda.time.LocalDate

import scala.util.{Failure, Success, Try}

/**
 *
 * @param dataCalc
 * @param x
 * @param y
 * @param automatic
 * @param verbose
 */
case class Args(var dataCalc: LocalDate,
                var x: Int,
                var y: Int,
                var automatic: Boolean,
                var verbose: Boolean,
                var executionId: String) {

  def getRange(): Tuple2[String, String] = {
    Calculation.calculateRange(dataCalc, y)
  }
}
//todo: questa componente andrebbe rivista e strutturarla meglio
object Args {

  def apply(args: Array[String]): Args = {
    if (args.length != 5) {
      LoggerUtility.printError("Bad param: i need data_calc,x,y,automatic and verbose", getClass.getName)
      System.exit(errorNumberParam)
      ???
    }
    var x = 0
    var y = 0
    var automatic = false
    var verbose = false

    var dataCalc = args(0).getLocalDate(format)
    val pX = parseInt(args(1))
    pX match {
      case Success(x_) => x = x_
      case Failure(x_) => LoggerUtility.printError(f"Bad Param x: $x_ is not integer", getClass.getName)
        System.exit(errorParamNotInt)
        ???
    }
    val pY = parseInt(args(2))
    pY match {
      case Success(y_) => y = y_
      case Failure(y_) => LoggerUtility.printError(f"Bad Param y: $y_ is not integer", getClass.getName)
        System.exit(errorParamNotInt)
        ???
    }
    val pAutomatic = parseBoolean(args(3))
    pAutomatic match {
      case Success(automatic_) => automatic = automatic_
      case Failure(automatic_) => LoggerUtility.printError(f"Bad Param automatic: $automatic_ is not boolean", getClass.getName)
        System.exit(errorParamNotBoolean)
        ???
    }
    val pVerbose = parseBoolean(args(4))
    pVerbose match {
      case Success(verbose_) => verbose = verbose_
      case Failure(verbose_) => LoggerUtility.printError(f"Bad Param verbose: $verbose_ is not boolean", getClass.getName)
        System.exit(errorParamNotBoolean)
        ???
    }
    checkX(x)
    LoggerUtility.printInfo(f"Param verbose: $verbose", getClass.getName)
    dataCalc = if (automatic) updateDataCalc() else dataCalc
    if (verbose) {
      LoggerUtility.printInfo(f"Param dataCalcString: ${args(0)}", getClass.getName)
      LoggerUtility.printInfo(f"Param y: ${args(2)}", getClass.getName)
      LoggerUtility.printInfo(f"Param x: ${args(1)}", getClass.getName)
      LoggerUtility.printInfo(f"Param automatic: ${args(3)}, changed in: ${dataCalc}", getClass.getName)
    }

    Args(dataCalc, x, y, automatic, verbose, executionId = "")
  }

  private def parseInt(x: String): Try[Int] = {
    Try(x.toInt)
  }

  private def parseBoolean(verbose: String): Try[Boolean] = {
    Try(verbose.toBoolean)
  }

  private def checkX(x: Int): Unit = {
    if (x <= 1) {
      LoggerUtility.printError(f"bad input param, x: {$x} at least > 1", getClass.getName)
      System.exit(errorXAtLeast2)
    }
  }

  private def updateDataCalc(): LocalDate = {
    LocalDate.now().toString(format).getLocalDate(format)
  }
}
