package it.au.misure.ingestionMisureGasUnico.flow

import com.typesafe.config.ConfigFactory
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

case class ExampleFlow() {
  val rootPath: String = ConfigFactory.load.getString("rootPath")

  def run()/*(implicit sc: SparkContext, sQLContext: SQLContext)*/: Unit = {
    val df = Environment.getSpark.sqlContext.read.format("xml")
      .options(Map(
        "path" -> s"$rootPath/test/xml",
        "rowTag" -> "FlussoMisure",
        "mode" -> "FAILFAST",
        "rowValidationXSDPath" -> s"$rootPath/test/xsd/FlussiDatiMisuraPrelievoGAS-Flusso1-Periodici.xsd"
      )).load()

    df.printSchema()

    df.select("DatiPdr.DatiTecnPdr.Trattamento")
      .show(false)



  }
}