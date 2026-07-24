package it.eng.au.calcoloIndennizzi.utility.args

import org.apache.log4j.Logger
import scopt.OptionParser

object ArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def parse(args: Array[String]): Args = {
    val parser = new OptionParser[Args]("[CIG] Calcolo Indennizzi Misure GAS") {
      head("calcolo-indennizzi-misure-gas")

      opt[String]('p', "properties") action { (x, c) =>
        c.copy(propertiesPath = x)
      } text "Properties path" required()

      opt[Unit]('r', "recovery") action { (x, c) =>
        c.copy(recoveryMode = true)
      } text "Enable recovery mode. If enabled, insert the path of the recovery csv in recovery.csv.path property."

      opt[String]('m', "year-month") action { (x, c) =>
        c.copy(yearMonth = Some(x))
      }

      opt[String]('d', "threshold-day") action { (x, c) =>
        c.copy(thresholdDay = Some(x))
      }

      help('h', "help") text "prints all options"
    }

    parser.parse(args, Args()) match {
      case Some(c) => c
      case None =>
        logger.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }

}
