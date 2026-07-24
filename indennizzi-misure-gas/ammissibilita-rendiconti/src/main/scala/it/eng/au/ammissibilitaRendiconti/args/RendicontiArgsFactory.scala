package it.eng.au.ammissibilitaRendiconti.args

import org.apache.log4j.Logger
import scopt.OptionParser

object RendicontiArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def parse(args: Array[String]): RendicontiArgs = {
    val parser = new OptionParser[RendicontiArgs]("[CIG] Ammissibilità Rendiconti") {
      head("ammissibilita-rendiconti")

      opt[String]('p', "properties") action { (x, c) =>
        c.copy(propertiesPath = x)
      } text "Properties path" required()

      opt[Unit]('r', "recovery") action { (x, c) =>
        c.copy(recoveryMode = true)
      } text "Run the process in recovery mode, using the file provided in recovery.csv.path config as input."

      opt[String]('e', "executionId") action { (x, c) =>
        c.copy(outputExecutionid = Some(x))
      }

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
