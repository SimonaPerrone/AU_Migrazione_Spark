package it.au.misure.ee_switching.driver

import it.au.misure.ee_switching.args.FlowArgsFactory
import it.au.misure.ee_switching.factory.FlowFactory
import it.au.misure.ee_switching.utility.environment.Environment
import it.au.misure.ee_switching.utility.{SparkImplicit, VersionLoggingUtility}
import org.apache.log4j.Logger

object FlowDriver {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      VersionLoggingUtility.printVersionInfo()

      val flowArgsMeta = FlowArgsFactory.parse(args)
      val argsConfig = FlowArgsFactory.getInputArgs(flowArgsMeta)
      FlowArgsFactory.checkInputArgs(argsConfig)

      val flow = FlowFactory.getFlow(argsConfig.flowName) match {
        case Some(flow) => flow
        case None => throw new IllegalArgumentException(s"Flow ${argsConfig.flowName} not supported")
      }

      Environment.getOrCreate((s"Switching EE ${argsConfig.flowName}"))
      flow.run(argsConfig)
    } catch {
      case e: Exception =>
        logger.error(s"Error procedure ${args.mkString(" ")}", e)
        throw e
    }
  }
}
