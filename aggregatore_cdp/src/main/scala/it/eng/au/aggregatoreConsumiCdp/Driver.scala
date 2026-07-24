package it.eng.au.aggregatoreConsumiCdp

import it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa.FactoryCdpDatiPrelievoGas
import it.eng.au.aggregatoreConsumiCdp.factory.AggregatorFactory
import it.eng.au.aggregatoreConsumiCdp.utility.{Environment, LogUtility}
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel

object Driver {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val applicationName = "[CDP] Aggregatore Consumi CDP"
      Environment.getOrCreate(applicationName, args(0))

      LogUtility.printInitialLog(applicationName, "LOG")

      logger.warn(s"Reading properties: ${args(0)}")
      val aggregators = AggregatorFactory.getAggregators
      logger.warn(s"Properties read.")

      logger.warn(s"Reading CA table...")
      val caFinal = FactoryCdpDatiPrelievoGas.getCaFinal(aggregators)
      caFinal.persist(StorageLevel.MEMORY_AND_DISK_SER)
      logger.warn(s"CA table read.")

      aggregators.foreach(agg => {
        logger.warn(s"Running ${agg.getClass.getName}")
        agg.run(caFinal)
        logger.warn(s"Finished ${agg.getClass.getName}")
      })

      LogUtility.printFinalLog(applicationName, "LOG")
    } catch {
      case e: Exception => logger.error(e.getStackTrace); throw e
      case e: Error => logger.error(e.getStackTrace); throw e
    }
  }


}
