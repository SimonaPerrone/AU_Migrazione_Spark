package it.eng.au.portale_consumi_ee

import it.eng.au.portale_consumi_ee.common.Driver
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.flow.Prova.Prova
import it.eng.au.portale_consumi_ee.flow.Storico33M.Storico33M
import it.eng.au.portale_consumi_ee.flow.Storico3M.Storico3M
import it.eng.au.portale_consumi_ee.utility.args.MisureEEArgs
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime

object Main extends Driver{

  // Implement the job logic in startJob
  override def run(args: Array[String]): Unit = {

    @transient  lazy val logger = Logger.getLogger(getClass.getName)

    val misureEEArgs =  MisureEEArgs.parse(args)
    val currentDateTime = LocalDateTime.now().toString

    EnvironmentMisure.getOrCreate(appName = s"portale_consumi_ee_misure_storico_fase_${misureEEArgs.flow}_$currentDateTime",
      path = misureEEArgs.pathToProperties,
      storic = misureEEArgs.storico)

    implicit val spark: SparkSession =  EnvironmentMisure.getSpark

     misureEEArgs.storico match {
      //calcolo storico 3M
      case false => new Storico3M().run()
      //calcolo storico 36M
      case true => new Storico33M().run()

      case _ => logger.error(s"Nessun flow per i parametri ${misureEEArgs.flow} e ${misureEEArgs.storico}")
    }

  }

}
