package it.eng.au.portale_consumi_ee.flow.stage

import it.eng.au.portale_consumi_ee.common.utility.functions.argumentsUtilities
import it.eng.au.portale_consumi_ee.dao.hive.misure._
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.{etlStage3M2PreRunModel, etlStage3M2ProposedModel}
import it.eng.au.portale_consumi_ee.schema.misure.{etlStage3M2PreRunSchema, etlStage3M2ProposedSchema, registroLoadSchema}
import it.eng.au.portale_consumi_ee.trasformations.{registroLoadTrasformation, stagePhaseTrasformationDifferentAprroach}
import it.eng.au.portale_consumi_ee.utility.args.MisureEEArgsConfig
import it.eng.au.portale_consumi_ee.utility.functions.argumentsUtilitiesExport
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

case class stagePhase33M(implicit spark: SparkSession) {

  @transient  lazy val logger = Logger.getLogger(getClass.getName)
  import spark.implicits._

  //input data
  val getAutolettureDao = new autolettureDao
  val getMisureMensiliCDao = new misureMensiliCDao
  val getMisureNonOrarieCDao = new misureNonOrarieCDao
  val getMisureOrarieCDao = new misureOrarieCDao
  val getVoltureDao = new voltureDao
  val getRegistroLoadDao = new registroLoadDao

  //output data
//  val getEtlStage3M2Dao = new etlStage3M2Dao
  //alternative output
  val etlStage33M2RevisitedDao = new etlStage33M2RevisitedOnHiveDao

   def  run(misureEEArgs:MisureEEArgsConfig,listAnnoMese: List[Int]): (Dataset[etlStage3M2ProposedModel],Dataset[etlStage3M2ProposedModel]) = {

     val storico = misureEEArgs.storico
     val windowTimeValue = EnvironmentMisure.getProperty("spark.app.mongodb.delay")
     val timeZone = EnvironmentMisure.getProperty("spark.app.time_zone")

     val windowTimeValueDefinition = argumentsUtilitiesExport.annomeseDefiniton(windowTimeValue,timeZone)


     //read input data
     def autolettureDS = getAutolettureDao.readWindowStoric(storico,listAnnoMese)
     def misureMensiliCDS = getMisureMensiliCDao.readWindowStoric(storico,listAnnoMese)
     def misureNonOrarieCDS = getMisureNonOrarieCDao.readWindowStoric(storico,listAnnoMese)
     def misureOrarieCDS = getMisureOrarieCDao.readWindowStoric(storico,listAnnoMese)
     def voltureDS = getVoltureDao.readWindowStoric(storico,listAnnoMese)
     def etlStage3M2DSPreRun = etlStage33M2RevisitedDao.read().selectExpr(etlStage3M2PreRunSchema.getValues:_*).as[etlStage3M2PreRunModel]
     def etlStage3M2DSCompare = etlStage33M2RevisitedDao.readWindowStoric(storico,listAnnoMese)
     def registroLoadDS = getRegistroLoadDao.read()

//     val statoPrecedentRun = stagePhaseTrasformationDifferentAprroach.controlloEtlPrecedente(etlStage3M2DSPreRun,registroLoadDS)

//     if (statoPrecedentRun) {
       logger.info(s"INIZIO FASE DI STAGE OK")
       logger.info(s"inizio modifica struttura dati misura.autoletture per export mongoDB ")
       def autolettuRevisitedDs = stagePhaseTrasformationDifferentAprroach.autolettureGeneration(autolettureDS)
       logger.info(s"fine modifica struttura dati misura.autoletture per export mongoDB ")

       logger.info(s"inizio modifica struttura dati misura.misure_mensili_c per export mongoDB ")
       def misureMensiliCRevisitedDS = stagePhaseTrasformationDifferentAprroach.misureMensiliGeneration(misureMensiliCDS)
       logger.info(s"fine modifica struttura dati misura.misure_mensili_c per export mongoDB ")

       logger.info(s"inizio modifica struttura dati misura.misure_non_orarie_c per export mongoDB ")
       def misureNonOrarieCRevisitedDS = stagePhaseTrasformationDifferentAprroach.misureNonOrarieGeneration(misureNonOrarieCDS)
       logger.info(s"fine modifica struttura dati misura.misure_non_orarie_c per export mongoDB ")

       logger.info(s"inizio modifica struttura dati misura.misure_orarie_gg per export mongoDB ")
       def misureOrarieCRevisitedDS = stagePhaseTrasformationDifferentAprroach.misureOrarieGeneration(misureOrarieCDS)
       logger.info(s"fine modifica struttura dati misura.misure_orarie_gg per export mongoDB ")

       logger.info(s"inizio modifica struttura dati misura.volture per export mongoDB ")
       def voltureRevisitedDS = stagePhaseTrasformationDifferentAprroach.voltureGeneration(voltureDS)
       logger.info(s"fine modifica struttura dati misura.volture per export mongoDB ")

              logger.info(s"calcolo nuova tabella di stage")
              val etlstageNew = stagePhaseTrasformationDifferentAprroach.calcolo_stage(autolettuRevisitedDs
                ,misureMensiliCRevisitedDS
                ,misureNonOrarieCRevisitedDS
                ,misureOrarieCRevisitedDS
                ,voltureRevisitedDS)

              logger.info(s"inizio confronto con tabella stage precedente")
              val (stageDatUpdated,finalJoinHashDateLastModifiedUpdatedWrite) = stagePhaseTrasformationDifferentAprroach.data_compare33M(
                etlstageNew,
                etlStage3M2DSCompare)

//       logger.info(s"scrittura finale su ${etlStage33M2RevisitedDao.tableName}")
//       etlStage33M2RevisitedDao.write(
//         finalJoinHashDateLastModifiedUpdatedWrite.selectExpr(etlStage3M2ProposedSchema.getValues:_*).as[etlStage3M2ProposedModel],true
//       )

     logger.warn("Starting update table register load")

     //remove old data
     //remove old data

     argumentsUtilities.dropPartitionsByValues(getRegistroLoadDao.tableName,registroLoadSchema.competenza_consumi,
       listAnnoMese,"33M", spark, logger)

     val dfRegistroLoad = registroLoadTrasformation.updateRegistroLoad(
       stageDatUpdated.selectExpr(etlStage3M2ProposedSchema.getValues:_*).as[etlStage3M2ProposedModel]
       ,storico)
     getRegistroLoadDao.writeRegistroLoad(dfRegistroLoad,true)
     logger.warn("End update table register load")

       logger.info(s"FINE FASE DI STAGE OK")
     (stageDatUpdated,finalJoinHashDateLastModifiedUpdatedWrite)
//     }else {
//       logger.info(s"ETL PRECEDENTE NON TERMINATO, FASE DI STAGE NON ESEGUITA")
//       return (statoPrecedentRun, Seq(etlStage3M2ProposedModel()).toDS())
//     }

  }

  }
