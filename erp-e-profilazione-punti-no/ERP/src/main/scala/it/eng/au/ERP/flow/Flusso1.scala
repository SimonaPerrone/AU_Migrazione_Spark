package it.eng.au.ERP.flow

import it.eng.au.ERP.utility.args.ERPArgsConfig
import it.eng.au.ERP.utility.environment.Environment
import org.apache.spark.sql.SparkSession

case class Flusso1(implicit spark: SparkSession)extends Flow{

  override def  run(timestamp: Long,podExluded:List[String],args: ERPArgsConfig): Unit = {

    import spark.implicits._

    logger.info("prova")
    logger.info("Starting FornitureInfo run")
    // Example Spark operation
    val properties = Environment.printProperties
    logger.info(s"Spark Environments property: $properties")


    // Create a simple DataFrame
    val df = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Carol")
    ).toDF("id", "name")

    // Show it
    df.show()

    // Count the rows
    println(s"Total records: ${df.count()}")

  }

}
