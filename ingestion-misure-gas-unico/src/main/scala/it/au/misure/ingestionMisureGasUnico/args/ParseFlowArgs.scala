package it.au.misure.ingestionMisureGasUnico.args

import org.apache.log4j.Logger
import scopt.OptionParser

object ParseFlowArgs {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def parse(args: Array[String]): FlowArgsConfig = {
    val parser = new OptionParser[FlowArgsConfig]("ingestion-misure-gas-unico Flow") {
      head("ingestion-misure-gas-unico Flow")

      opt[String]('f', "flow") action { (x, c) =>
        c.copy(flowName = x)
      } text "flowName" required()

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