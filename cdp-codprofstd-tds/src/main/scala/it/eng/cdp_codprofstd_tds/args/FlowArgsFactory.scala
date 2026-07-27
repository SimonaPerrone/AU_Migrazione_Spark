package it.eng.cdp_codprofstd_tds.args

import org.apache.log4j.{LogManager, Logger}
import scopt.OptionParser

object FlowArgsFactory {
  @transient val logger: Logger = LogManager.getLogger(this.getClass)

  def parse(args: Array[String]): FlowArgsConfig = {
    val parser = new OptionParser[FlowArgsConfig]("[CDP] Ricalcolo Codide Profilo Standard Da Nuova Tds") {
      head("[CDP] Ricalcolo Codide Profilo Standard Da Nuova Tds")

      opt[String]('p', "properties-path") action { (x, c) =>
        c.copy(propertiesPath = x)
      } text "Job configuration (.properties) file full path. Example: /home/user/app/params.properties."

      help('h', "help") text "prints all options"
    }
    parser.parse(args, FlowArgsConfig()) match {
      case Some(c) => c
      case None =>
        logger.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }

  def checkArgs(argsConfig: FlowArgsConfig): Unit = {
    if (argsConfig.propertiesPath.isEmpty)
      throw new IllegalArgumentException(s"Properties` file path must be a valid path.")
  }
}

