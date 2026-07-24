package it.eng.au.pubblicazioneIndennizzi.args

import org.apache.log4j.Logger
import scopt.OptionParser

object PubblicazioneArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def parse(args: Array[String]): PubblicazioneArgsConfig = {
    val parser = new OptionParser[PubblicazioneArgsConfig]("Pubblicazione Indennizzi") {
      head("Pubblicazione Indennizzi")

      opt[String]('p', "properties") action { (x, c) =>
        c.copy(propertiesPath = x)
      } text "Path to properties" required()

      opt[String]('e', "executionid") action { (x, c) =>
        c.copy(recoveryMode = true, inputTableExecutionId = x)
      } text "Run the process in recovery mode, using the executionid given as input."

      help('h', "help") text "prints all options"
    }

    parser.parse(args, PubblicazioneArgsConfig()) match {
      case Some(c) => c
      case None =>
        logger.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }
}