package it.eng.au.pubblicazioneIndennizzi

import it.eng.au.indennizziMisureGasCommon.utility.log.LogUtility
import it.eng.au.pubblicazioneIndennizzi.args.PubblicazioneArgsFactory
import it.eng.au.pubblicazioneIndennizzi.factory.AggregatorFactory
import it.eng.au.pubblicazioneIndennizzi.utility.PubblicazioneIndennizziEnvironment
import org.apache.log4j.Logger

object Driver {
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val parsedArgs = PubblicazioneArgsFactory.parse(args)

      PubblicazioneIndennizziEnvironment.setEnvironment(parsedArgs)

      LogUtility.printInitialLog()

      val aggregators = AggregatorFactory.getAggregators

      aggregators.foreach(agg => {
        log.info(s"Running ${agg.getClass}")
        agg.run()
        log.info(s"Finished ${agg.getClass}")
      })

      LogUtility.printFinalLog()
    } catch {
      case e: Exception => log.error(e.getStackTrace); throw e
      case e: Error => log.error(e.getStackTrace); throw e
    }
  }
}
