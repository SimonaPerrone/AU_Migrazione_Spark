package it.au.misure.ingestionMisureGasUnico.driver

import it.au.misure.ingestionMisureGasUnico.args.UnzipFlowArgs
import it.au.misure.ingestionMisureGasUnico.unzip.UnzipFlow
import it.au.misure.ingestionMisureGasUnico.utility.PropertyUtility.getCheckSqoop
import it.au.misure.ingestionMisureGasUnico.utility._
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import org.apache.log4j.Logger


object UnzipDriver {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      VersionLoggingUtility.printVersionInfo()

      val argsConfig = UnzipFlowArgs.parse(args)
      // TODO: execute check on input parameters: -D > -d && flow is one of the available options


      //initializeSpark(s"Ingestion GAS Unzip")
      Environment.getOrCreate(s"Ingestion GAS Unzip")
      //val sc = Environment.getSpark.sparkContext
      //val sqlContext = Environment.getSpark.sqlContext
      logger.warn("run check sqoop rcu table")
      if (!PropertyUtility.getCheckSqoop || (
        CheckSqoopUtility.checkSqoopDateIsToday(PropertyUtility.getRcuAziendaPTable) &&
          CheckSqoopUtility.checkSqoopDateIsToday(PropertyUtility.getRcugasPdrTable) &&
          CheckSqoopUtility.checkSqoopDateIsToday(PropertyUtility.getRcugasPdrStatoTable) &&
          CheckSqoopUtility.checkSqoopDateIsToday(PropertyUtility.getRcugasUDDPTable)
      )) {
        //logger.warn("check sqoop rcu table is successful")
        if (PropertyUtility.getCheckSqoop) {
          logger.warn("check sqoop rcu table is successful.")
        } else logger.warn("check sqoop functionality is disabled")

        UnzipFlow.run(argsConfig) //(sc, sqlContext)
      } else {
        logger.error("One of the rcu/rcugas tables has not been updated today")
        throw new Exception(s"One of the rcu/rcugas tables has not been updated today")
      }


    } catch {
      case e: Exception =>
        logger.error(s"Error procedure ${args.mkString(" ")}", e)
        throw e
    }
  }
}
