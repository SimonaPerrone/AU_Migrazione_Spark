package it.eng.au.portale_consumi_ee.flow.Prova

import it.eng.au.portale_consumi_ee.common.flow.{FlowDsOutput, FlowUnitOutput}
import it.eng.au.portale_consumi_ee.common.utility.functions.argumentsUtilities
import it.eng.au.portale_consumi_ee.dao.misure.MisureStoricNoraDao
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.schema.misure.MisureStoricNoraSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class Prova(implicit spark: SparkSession)  extends FlowUnitOutput{

  val getMisureStoricNoraDao = new MisureStoricNoraDao

  val windowTimeValue = EnvironmentMisure.getProperty("spark.app.mongodb.delay")
  val timeZone = EnvironmentMisure.getProperty("spark.app.time_zone")
  val annomesegiornoWindow3M =  argumentsUtilities.annomesegiornoDefiniton(windowTimeValue,timeZone)

  override def run() = {

    println(s"3M limit is : ${annomesegiornoWindow3M}")

    logger.info(s"Inizio fase storica per dati non orari")
    println(s"the annomese limit is: ${annomesegiornoWindow3M}")

    def getAnnoMeseWindowMisureStoricNoraDao =
      getMisureStoricNoraDao.read()
        //        .filter(col(MisureStoricNoraSchema.annomese) >= annomesegiornoWindow3M)
        .repartition(col(MisureStoricNoraSchema.annomese))

    println(s"the tablese showd is: ${getMisureStoricNoraDao.tableName}")
    getAnnoMeseWindowMisureStoricNoraDao.printSchema()
    val count = getAnnoMeseWindowMisureStoricNoraDao.count().toString
    println(s"the tablese count  is: ${count}")
    println("okey 5o")
//    getAnnoMeseWindowMisureStoricNoraDao.show()
     println("okey 5")
//    logger.info("prova")
//    logger.info("Starting FornitureInfo run")
//    // Example Spark operation
//    val properties = EnvironmentMisure.printProperties
//    logger.info(s"Spark Environments property: $properties")
//    val data = spark.range(10)  // Creates a simple DataFrame with numbers from 0 to 9
//    data.show()  // Displays the DataFrame in the console

  }

}
