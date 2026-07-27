package it.eng.cdp_codprofstd_tds

import it.eng.cdp_codprofstd_tds.args.FlowArgsFactory
import it.eng.cdp_codprofstd_tds.controller.TdsCodProfStd
import it.eng.cdp_codprofstd_tds.utility.Environment
import org.apache.log4j.Logger


object Driver {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      logger.info("Start Ricalcolo Codide Profilo Standard Da Nuova Tds")
      val cliArgs = FlowArgsFactory.parse(args)
      Environment.getOrCreate("[CDP] Ricalcolo Codide Profilo Standard Da Nuova Tds", cliArgs.propertiesPath)
      logger.warn("Run Ricalcolo Codide Profilo Standard Da Nuova Tds")
      logger.warn("Properties:")
      logger.warn(s"${Environment.printProperties}")
      logger.warn(s"Execution ID: ${Environment.executionId}")
      logger.warn(s"Date: ${Environment.getPartitionDate}")
      logger.warn(s"applicationID=${Environment.getSpark.sparkContext.applicationId}")
      TdsCodProfStd.run()

    } catch {
      case e: Throwable =>
        logger.error(s"Error procedure ${args.mkString(" ")}", e)
        throw e
    }
  }
}
