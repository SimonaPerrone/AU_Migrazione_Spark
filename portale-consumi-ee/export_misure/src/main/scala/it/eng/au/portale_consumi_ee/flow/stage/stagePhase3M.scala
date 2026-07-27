package it.eng.au.portale_consumi_ee.flow.stage

import it.eng.au.portale_consumi_ee.common.utility.functions.argumentsUtilities
import org.apache.spark.sql.Dataset
import it.eng.au.portale_consumi_ee.dao.hive.misure.{autolettureDao, etlStage3M2Dao, etlStage3M2RevisitedOnHiveDao, misureMensiliCDao, misureNonOrarieCDao, misureOrarieCDao, registroLoadDao, voltureDao}
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.{etlStage3M2Model, etlStage3M2PreRunModel, etlStage3M2ProposedModel}
import it.eng.au.portale_consumi_ee.schema.misure.{etlStage3M2PreRunSchema, etlStage3M2ProposedSchema, registroLoadSchema}
import it.eng.au.portale_consumi_ee.trasformations.{registroLoadTrasformation, stagePhaseTrasformation, stagePhaseTrasformationDifferentAprroach}
import it.eng.au.portale_consumi_ee.utility.args.MisureEEArgsConfig
import it.eng.au.portale_consumi_ee.utility.functions.argumentsUtilitiesExport
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

case class stagePhase3M(implicit spark: SparkSession) {

  @transient  lazy val logger = Logger.getLogger(getClass.getName)
  import spark.implicits._

  //Dao
  val getAutolettureDao = new autolettureDao
  val getMisureMensiliCDao = new misureMensiliCDao
  val getMisureNonOrarieCDao = new misureNonOrarieCDao
  val getMisureOrarieCDao = new misureOrarieCDao
  val getVoltureDao = new voltureDao
  val getRegistroLoadDao = new registroLoadDao
  val etlStage3M2RevisitedDao = new etlStage3M2RevisitedOnHiveDao


   def  run(misureEEArgs:MisureEEArgsConfig): (Dataset[etlStage3M2ProposedModel],Dataset[etlStage3M2ProposedModel],Dataset[etlStage3M2ProposedModel]) = {

     val storico = misureEEArgs.storico
     val windowTimeValue = EnvironmentMisure.getProperty("spark.app.mongodb.delay")
     val timeZone = EnvironmentMisure.getProperty("spark.app.time_zone")

     val windowTimeValueDefinition = argumentsUtilitiesExport.annomeseDefiniton(windowTimeValue,timeZone)


     //read input data
     def autolettureDS = getAutolettureDao.read(storico,windowTimeValueDefinition)
     def misureMensiliCDS = getMisureMensiliCDao.read(storico,windowTimeValueDefinition)
     def misureNonOrarieCDS = getMisureNonOrarieCDao.read(storico,windowTimeValueDefinition)
     def misureOrarieCDS = getMisureOrarieCDao.read(storico,windowTimeValueDefinition)
     def voltureDS = getVoltureDao.read(storico,windowTimeValueDefinition)
     def etlStage3M2DSCompare = etlStage3M2RevisitedDao.read(storico,windowTimeValueDefinition)
     def registroLoadDS = getRegistroLoadDao.read()

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
              val (stageDataNew,stageDataUpdated,finalJoinHashDateLastModifiedUpdatedWrite) = stagePhaseTrasformationDifferentAprroach.data_compare(
                etlstageNew,
                etlStage3M2DSCompare
              )

     stageDataNew.cache()

     val stageDataUpdatedOuput = stageDataUpdated.persist(StorageLevel.MEMORY_AND_DISK)

     logger.warn("Starting update table register load")

     //remove old data
     val ListPartitionToRemove = argumentsUtilities.getYearMonthRange(windowTimeValueDefinition)
     argumentsUtilities.dropPartitionsByValues(getRegistroLoadDao.tableName,registroLoadSchema.competenza_consumi,
       ListPartitionToRemove,"3M", spark, logger)

    val registroLoadToWrite = stageDataNew.unionByName(stageDataUpdated)
    val dfRegistroLoad = registroLoadTrasformation.updateRegistroLoad(registroLoadToWrite,storico)
     getRegistroLoadDao.writeRegistroLoad(dfRegistroLoad,true)
     logger.warn("End update table register load")

       logger.info(s"FINE FASE DI STAGE OK")

       (stageDataNew,stageDataUpdatedOuput,finalJoinHashDateLastModifiedUpdatedWrite)
  }

  }
