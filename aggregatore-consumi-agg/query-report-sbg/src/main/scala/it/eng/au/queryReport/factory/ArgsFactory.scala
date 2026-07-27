package it.eng.au.queryReport.factory

import org.apache.log4j.Logger
import scopt.OptionParser

object ArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def parse(args: Array[String]): ArgsMetadata = {
    val parser = new OptionParser[ArgsMetadata]("query-report-sbg") {
      head("query-report-sbg")

      opt[String]('q', "queries") action { (x, c) =>
        c.copy(queries = x.toUpperCase.split(",").toList)
      } text "Query to run. Ex query1,query2,..." required()


      help('h', "help") text "prints all options"
    }

    parser.parse(args, ArgsMetadata()) match {
      case Some(c) => c
      case None =>
        logger.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }
}
