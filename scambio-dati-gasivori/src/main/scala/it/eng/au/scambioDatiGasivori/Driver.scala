package it.eng.au.scambioDatiGasivori

import it.eng.au.scambioDatiGasivori.args.ArgsFactory
import it.eng.au.scambioDatiGasivori.factory.AggregatorFactory
import it.eng.au.scambioDatiGasivori.utility.Constants.{APPLICATION_NAME, LOG_NAME}
import it.eng.au.scambioDatiGasivori.utility.{Environment, LogUtility}
import org.apache.log4j.Logger

import java.time.format.DateTimeFormatter

object Driver {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val parsedArgs = ArgsFactory.parse(args)

      Environment.getOrCreate(APPLICATION_NAME, LOG_NAME, parsedArgs.propertiesPath)
      Environment.setProperty("daterun", Environment.startDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")))
      Environment.setProperty("output.file.modes", parsedArgs.modes)

      LogUtility.printInitialLog()

      val aggregators = AggregatorFactory.getAggregators

      aggregators.foreach(agg => {
        logger.warn(s"Running ${agg.getClass}")
        agg.run()
        logger.warn(s"Finished ${agg.getClass}")
      })

      LogUtility.printFinalLog()
    }
    catch {
      case e: Exception => logger.error(e.getStackTrace); throw e
      case e: Error => logger.error(e.getStackTrace); throw e
    }
  }
}
