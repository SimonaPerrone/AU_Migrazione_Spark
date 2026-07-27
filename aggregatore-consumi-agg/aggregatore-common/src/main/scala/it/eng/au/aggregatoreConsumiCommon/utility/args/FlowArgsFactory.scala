package it.eng.au.aggregatoreConsumiCommon.utility.args

import org.apache.log4j.Logger
import org.joda.time.DateTime
import scopt.OptionParser

import scala.util.Try

object FlowArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def parse(args: Array[String]): FlowArgsConfig = {
    val parser = new OptionParser[FlowArgsConfig]("aggregatore") {
      head("aggregatore")

      opt[String]('p', "properties") action { (x, c) =>
        c.copy(pathToProperties = x)
      } text "Path to properties" required()

      opt[String]('o', "outputFileCouples") action { (x, c) =>
        c.copy(outputFileCouples = Some(x).map(_.toUpperCase))
      } text "Syntax for outputFileCouples is: left_value->right_value"

      help('h', "help") text "prints all options"
    }

    parser.parse(args, FlowArgsConfig()) match {
      case Some(c) => c
      case None =>
        logger.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }

}
