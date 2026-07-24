package it.eng.au.gse.calcoloMensile.args

import org.apache.log4j.Logger
import scopt.OptionParser

import java.time.format.DateTimeFormatter
import java.time.{Year, YearMonth}
import scala.util.Try

object ArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def parse(args: Array[String]): Args = {
    val parser = new OptionParser[Args]("[GSE] Energy Release") {
      head("gse-energy-release")

      opt[String]('p', "properties")
        .action({ (x, c) =>
        c.copy(propertiesPath = x)
      }).text("Properties path")
      .required()

      help('h', "help").text("prints all options")
    }

    parser.parse(args, Args()) match {
      case Some(c) => c
      case None =>
        logger.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }
}
