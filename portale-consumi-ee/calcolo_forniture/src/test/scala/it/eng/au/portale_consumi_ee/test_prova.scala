package it.eng.au.portale_consumi_ee

import it.eng.au.portale_consumi_ee.Main.logger
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment

class test_prova {

  val spark = Environment.getSpark

  import spark.implicits._
  //test prova
  def testProva(): Unit = {
    val properties = Environment.printProperties
    logger.info(s"Spark Environments property: $properties")
    // Example Spark operation
    val data = spark.range(10) // Creates a simple DataFrame with numbers from 0 to 9
    data.show() // Displays the DataFrame in the console
  }
}
