package it.eng.au.portale_consumi_ee.flow.FornitureInfo

import it.eng.au.portale_consumi_ee.common.flow.{FlowDsOutput, FlowUnitOutput}
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import org.apache.spark.sql.SparkSession

class FornitureInfo(implicit spark: SparkSession)  extends FlowUnitOutput{

  override def run() = {

    logger.info("prova")
    logger.info("Starting FornitureInfo run")

    val properties = Environment.printProperties
    logger.info(s"Spark Environments property: $properties")
    // Example Spark operation
    val data = spark.range(10)  // Creates a simple DataFrame with numbers from 0 to 9
    data.show()  // Displays the DataFrame in the console

  }

}
