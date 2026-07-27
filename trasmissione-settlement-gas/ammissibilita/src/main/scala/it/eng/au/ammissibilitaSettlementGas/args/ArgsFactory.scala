package it.eng.au.ammissibilitaSettlementGas.args

import org.apache.log4j.Logger
import scopt.OptionParser

object ArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def parse(args: Array[String]): Args = {
    val parser = new OptionParser[Args]("[TSG] Ammissibilita") {
      head("ammissibilita")

      opt[String]('m', "annomese") action { (x, c) =>
        c.copy(isRecoveryMode = true, annoMese = Some(x))
      } text "Anno mese su cui valutare l'ammissibilita"

      opt[String]('p', "properties") action { (x, c) =>
        c.copy(propertiesPath = x)
      } text "Properties path" required()

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
