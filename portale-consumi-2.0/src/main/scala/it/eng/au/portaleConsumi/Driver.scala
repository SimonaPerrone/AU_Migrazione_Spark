package it.eng.au.portaleConsumi

import it.eng.au.portaleConsumi.flow.{FornitureGasFlow, MisureGasFlow}
import it.eng.au.portaleConsumi.utility.args.{ArgsFactory, PortaleConsumiArgs}
import it.eng.au.portaleConsumi.utility.common.VersionLoggingUtility.printVersionInfo
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.log4j.Logger

import java.time.LocalDateTime

object Driver {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.warn("Inizio job Portale consumi")
    printVersionInfo()

    val currentDateTime = LocalDateTime.now()

    val flowArgsConfig = ArgsFactory.parse(args)
    logger.warn(s"Parametri: ${flowArgsConfig.toString}")

    Environment.getOrCreate(
      appName = s"PortaleConsumi_${flowArgsConfig.flow}_${flowArgsConfig.interval}_$currentDateTime",
      path = flowArgsConfig.pathToProperties)

    flowArgsConfig.flow match {
      case PortaleConsumiArgs.flowFornitureGas => new FornitureGasFlow().run(flowArgsConfig)
      case PortaleConsumiArgs.flowMisureGas => new MisureGasFlow().run(flowArgsConfig)
      case _ => logger.error(s"Nessun flow per il parametro ${flowArgsConfig.flow}")
    }

    logger.warn("Fine job Portale consumi")
  }

}
