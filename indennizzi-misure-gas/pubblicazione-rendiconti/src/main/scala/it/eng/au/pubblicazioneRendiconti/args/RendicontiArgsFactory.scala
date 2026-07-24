package it.eng.au.pubblicazioneRendiconti.args

import org.apache.log4j.Logger
import scopt.OptionParser

object RendicontiArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def parse(args: Array[String]): RendicontiArgs = {
    val parser = new OptionParser[RendicontiArgs]("[CIG] Pubblicazione Rendiconti") {
      head("pubblicazione-rendiconti")

      opt[String]('p', "properties") action { (x, c) =>
        c.copy(propertiesPath = x)
      } text "Properties path" required()

      opt[String]('e', "executionid") action { (x, c) =>
        c.copy(recoveryMode = true, inputTableExecutionId = x)
      } text "Run the process in recovery mode, using the executionid given as input."

      help('h', "help") text "prints all options"
    }

    parser.parse(args, RendicontiArgs()) match {
      case Some(c) => c
      case None =>
        logger.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }

}
