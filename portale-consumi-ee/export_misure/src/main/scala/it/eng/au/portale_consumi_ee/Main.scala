package it.eng.au.portale_consumi_ee

import it.eng.au.portale_consumi_ee.common.Driver
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.flow.Prova.Prova
import it.eng.au.portale_consumi_ee.flow.stage.stagePhase33MInizitialization
import it.eng.au.portale_consumi_ee.utility.args.{MisureEEArgs, MisureEEArgsConfig}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import it.eng.au.portale_consumi_ee.flow.storico33M.storico33M
import it.eng.au.portale_consumi_ee.flow.storico3M.{storico3M, storico3MInizialization}

import java.time.LocalDateTime

object Main extends Driver{

  @transient  lazy val logger: Logger = Logger.getLogger(getClass.getName)


  // Implement the job logic in startJob
  override def run(args: Array[String]): Unit = {

    val misureEEArgs =  MisureEEArgs.parse(args)
    val currentDateTime = LocalDateTime.now().toString

    EnvironmentMisure.getOrCreate(appName = s"portale_consumi_ee_export_mongo_fase_${misureEEArgs.flow}_$currentDateTime",
      path = misureEEArgs.pathToProperties,
      storic = misureEEArgs.storico)

    implicit val spark: SparkSession =  EnvironmentMisure.getSpark

    (misureEEArgs.flow, misureEEArgs.storico) match {
      //export 3M
      case (MisureEEArgsConfig.flowFull,false) => new storico3M().run(misureEEArgs)
      //export 33M
      case (MisureEEArgsConfig.flowFull,true) => new storico33M().run(misureEEArgs)
      //initialization 3M
      case (MisureEEArgsConfig.flowInitialization,false) => new storico3MInizialization().run(misureEEArgs)
      //initialization 33M
      case (MisureEEArgsConfig.flowInitialization,true) => stagePhase33MInizitialization().run(misureEEArgs)
      case _ => logger.error(s"Nessun flow per i parametri ${misureEEArgs.flow} e ${misureEEArgs.storico}")
    }

  }

}
