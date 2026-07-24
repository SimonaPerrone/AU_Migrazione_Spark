package it.eng.au.scambioDatiGasivori.args

import org.apache.log4j.Logger
import scopt.OptionParser

object ArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def parse(args: Array[String]): Args = {
    val parser = new OptionParser[Args]("[GAS] Scambio Dati Gasivori") {
      head("scambio-dati-gasivori")

      opt[String]('p', "properties") action { (x, c) =>
        c.copy(propertiesPath = x)
      } text "Properties path" required()

      opt[String]('m', "modes") action { (x, c) =>
        c.copy(modes = x)
      } text "Modes" required()

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
