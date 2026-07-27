package it.eng.au.portale_consumi_ee.flow.storico33M

import it.eng.au.portale_consumi_ee.common.flow.FlowUnitOutput
import it.eng.au.portale_consumi_ee.flow.storico3M.storico3M
import it.eng.au.portale_consumi_ee.utility.args.MisureEEArgsConfig
import org.apache.spark.sql.SparkSession

class storico33Mplus3M(implicit spark: SparkSession)  extends FlowUnitOutput{

  def run(misureEEArgs:MisureEEArgsConfig) = {
    logger.info(s"INIZIO FASE 33M")
    new storico33M().run(misureEEArgs)
    logger.info(s"FINE FASE 33M")

    logger.info(s"INIZIO FASE 3M")
    new storico3M().run(misureEEArgs)
    logger.info(s"FINE FASE 3M")
  }
}
