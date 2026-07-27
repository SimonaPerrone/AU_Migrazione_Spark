package it.eng.au.ccgPubblicazione.args

import it.eng.au.ccgPubblicazione.utility.Constants._
import org.apache.log4j.Logger
import org.joda.time.DateTime
import scopt.OptionParser

object FlowArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def parse(args: Array[String]): FlowArgsConfig = {
    val parser = new OptionParser[FlowArgsConfig]("aggiustamento-gas") {
      head("aggiustamento-gas")

      opt[String]('p', "properties") action { (x, c) =>
        c.copy(pathToProperties = x)
      } text "Path to properties" required()

      opt[String]('s', "session") action { (x, c) =>
        val upperValue = x.toUpperCase()
        if (!List(AGG, SBG, CDP_FIN, CDP_RIC).contains(upperValue)) throw new IllegalArgumentException(s"Error values accepted are AGG/SBG/CDP_FIN/CDP_RIC")
        c.copy(session = upperValue)
      } text "Process type to run: AGG/SBG/CDP_FIN/CDP_RIC" required()

      // TODO fare un test execution id vuoto
      opt[String]('e', "executiondate") action { (x, c) =>
        c.copy(dataRichiesta = x)
      } text "Executionid to publish" required()

      opt[String]('t', "type") action { (x, c) =>
        val upperValue = x.toUpperCase()
        if (!List(PDR, FILTRO, ALL).contains(upperValue)) throw new IllegalArgumentException(s"Error values accepted are PDR/FILTRO/ALL")
        c.copy(tipo = x)
      } text "Type to request: PDR/FILTRO/ALL"

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
