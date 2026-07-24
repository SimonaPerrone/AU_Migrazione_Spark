package it.au.misure.ingestionMisureGasUnico.driver

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

import it.au.misure.ingestionMisureGasUnico.args.ParseFlowArgs
import it.au.misure.ingestionMisureGasUnico.factory.FlowFactory
import it.au.misure.ingestionMisureGasUnico.utility.Constants.CURRENT_TIMESTAMP_PATTERN
import it.au.misure.ingestionMisureGasUnico.utility.{PropertyUtility, VersionLoggingUtility}
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment

import org.apache.log4j.Logger

object FlowDriver {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      VersionLoggingUtility.printVersionInfo()

      val argsConfig = ParseFlowArgs.parse(args)

      val flow = FlowFactory.getFlow(argsConfig.flowName) match {
        case Some(flow) => flow
        case None => throw new IllegalArgumentException(s"flow ${argsConfig.flowName} not supported")
      }
      val unzipTimestamp = LocalDateTime.now(ZoneId.of(PropertyUtility.getTimeZone))
        .format(DateTimeFormatter.ofPattern(CURRENT_TIMESTAMP_PATTERN))

      //initializeSpark(s"Ingestion GAS Standard ${flow.flowName}")
      Environment.getOrCreate(s"Ingestion GAS Standard ${flow.flowName}")
      //val sc = Environment.getSpark.sparkContext
      //val sqlContext = Environment.getSpark.sqlContext
      flow.run(unzipTimestamp)//(sc, sqlContext)

    } catch {
      case e: Exception =>
        logger.error(s"Error procedure ${args.mkString(" ")}", e)
        throw e
    }
  }
}
