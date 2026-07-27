package it.sferanet.au.utilities.args

import org.apache.log4j.Logger
import org.joda.time.DateTime
import scopt.OptionParser

object FlowArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def parse(args: Array[String]): FlowArgsConfig = {
    val parser = new OptionParser[FlowArgsConfig]("CAGas") {
      head("CAGas")

      opt[String]('p', "properties") action { (x, c) =>
        c.copy(pathToProperties = x)
      } text "Path to properties" required()

      opt[String]('s', "session") action { (x, c) =>
        val sessionValue = x.toUpperCase()
        if(!List("CDP","CCG_FIN", "CCG_RIC").contains(sessionValue)) throw new IllegalArgumentException(s"Error values accepted are CDP/CCG_FIN/CCG_RIC")
        c.copy(session = sessionValue)
      } text "Process type to run: CDP/CCG_FIN/CCG_RIC"

      opt[String]('d', "date") action { (x, c) =>
        c.copy(dateToRun = Option(new DateTime(x)))
      } text "Date to run, format yyyy-MM-dd. For CDP session this is ignored"

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
