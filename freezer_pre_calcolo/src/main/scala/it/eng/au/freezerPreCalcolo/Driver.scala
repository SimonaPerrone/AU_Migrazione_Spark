package it.eng.au.freezerPreCalcolo

import it.eng.au.freezerPreCalcolo.factory.FreezerFactory
import it.eng.au.freezerPreCalcolo.utility.args.FlowArgsFactory
import it.eng.au.freezerPreCalcolo.utility.environment.{CcgFinEnvironment, CcgRicEnvironment, CdpEnvironment}
import org.apache.log4j.Logger

/** Main class invoked by Spark. It takes care of parsing the properties,
 * getting and running the execution modes. */
object Driver {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val flowArgsConfig = FlowArgsFactory.parse(args)

      if (flowArgsConfig.session.contains("CCG_FIN"))
        CcgFinEnvironment.setEnvironment(flowArgsConfig)
      else if (flowArgsConfig.session.contains("CCG_RIC"))
        CcgRicEnvironment.setEnvironment(flowArgsConfig)
      else
        CdpEnvironment.setEnvironment(flowArgsConfig)

      run()

      logger.warn("Freezing procedure ended.")

    } catch {
      case e: Throwable =>
        logger.error(s"Error procedure ${args.mkString(" ")}", e)
        throw e
    }
  }

  def run(): Unit = {
    val aggregators = FreezerFactory.getAggregators
    aggregators.foreach(agg => {
      logger.info(s"Running ${agg.getClass}")
      agg.runFreezer()
      logger.info(s"Finished ${agg.getClass}")
    })
  }
}
