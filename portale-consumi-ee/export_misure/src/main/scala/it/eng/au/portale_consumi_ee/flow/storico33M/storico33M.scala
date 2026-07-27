package it.eng.au.portale_consumi_ee.flow.storico33M

import it.eng.au.portale_consumi_ee.common.flow.FlowUnitOutput
import it.eng.au.portale_consumi_ee.common.utility.functions.argumentsUtilities
import it.eng.au.portale_consumi_ee.dao.hive.misure.{etlStage33M2RevisitedOnHiveDao, registroLoadDao}
import it.eng.au.portale_consumi_ee.dao.mongo.MisureElettriche33MDao
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.flow.`export`.{exportPhase33M, exportPhase3M}
import it.eng.au.portale_consumi_ee.flow.stage.{stagePhase33M, stagePhase3M}
import it.eng.au.portale_consumi_ee.model.misure.etlStage3M2ProposedModel
import it.eng.au.portale_consumi_ee.schema.misure.{etlStage3M2ProposedSchema, registroLoadSchema}
import it.eng.au.portale_consumi_ee.trasformations.registroLoadTrasformation
import it.eng.au.portale_consumi_ee.utility.args.MisureEEArgsConfig
import it.eng.au.portale_consumi_ee.utility.functions.argumentsUtilitiesExport
import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class storico33M(implicit spark: SparkSession)  extends FlowUnitOutput{

  //define stage flow
  def setStage = new stagePhase33M()
  //define export flow
  def setExportMongo = new exportPhase33M()

  //output hive
  val etlStage33M2RevisitedDao = new etlStage33M2RevisitedOnHiveDao
  val getRegistroLoadDao = new registroLoadDao

  //output mongo
  val getMisureElettriche33MDao = new MisureElettriche33MDao



  def run(misureEEArgs:MisureEEArgsConfig) = {

     val windowTimeValue = EnvironmentMisure.getProperty("spark.app.mongodb.delay")
     val timeZone = EnvironmentMisure.getProperty("spark.app.time_zone")

     val windowTimeValueDefinition = argumentsUtilitiesExport.annomeseDefiniton(windowTimeValue,timeZone)

     val windowsStoric = argumentsUtilitiesExport.generatePastMonths(windowTimeValueDefinition,windowTimeValue)

     for (listAnnomese <- windowsStoric) {

       val listAnnomeseString = listAnnomese.mkString(", ")
       logger.info(s"Inizio fase di export 33M per annomese: ${listAnnomeseString}")

     logger.warn("Starting stage phase")
       //statoPrecedentRun la voglio fuori dal ciclo
       //stageDatNew,stageDatUpdated la vorrei unica ds da scrivere
     val (stageDatUpdated,finalJoinHashDateLastModifiedUpdatedWrite) = setStage.run(misureEEArgs,listAnnomese)
     logger.warn("End stage phase")

     logger.warn("Starting export phase")

       setExportMongo.run(stageDatUpdated)

       logger.info(s"Fine fase di export 33M per annomese: ${listAnnomeseString}")
       logger.info(s"scrittura finale su ${etlStage33M2RevisitedDao.tableName}")
       etlStage33M2RevisitedDao.write(
         finalJoinHashDateLastModifiedUpdatedWrite,true
       )
   }
     //start remove old data
    //hive
    // etl_stage_33m_2
     val OldPartitionData = argumentsUtilitiesExport.get37thMonthAgo()

    logger.info(s"Remove from table ${etlStage33M2RevisitedDao.tableName} patition greater than  ${windowTimeValueDefinition.toString}")
    //spark.sql(s"ALTER TABLE ${etlStage33M2RevisitedDao.tableName}  DROP IF EXISTS PARTITION (${etlStage3M2ProposedSchema.competenza_consumi}>${windowTimeValueDefinition})")
    argumentsUtilities.dropPartitionsAfterAnnomeseRiferimento( etlStage33M2RevisitedDao.tableName,etlStage3M2ProposedSchema.competenza_consumi,windowTimeValueDefinition, spark, logger)
    logger.info(s"Remove from table ${etlStage33M2RevisitedDao.tableName} patition greater than  ${windowTimeValueDefinition.toString} done")


    logger.info(s"Remove from table ${etlStage33M2RevisitedDao.tableName} patition less or equal  ${OldPartitionData.toString}")
     //spark.sql(s"ALTER TABLE ${etlStage33M2RevisitedDao.tableName}  DROP IF EXISTS PARTITION (${etlStage3M2ProposedSchema.competenza_consumi}<=${OldPartitionData})")
    argumentsUtilities.dropPartitionsBeforeOrEqualAnnomeseRiferimento( etlStage33M2RevisitedDao.tableName,etlStage3M2ProposedSchema.competenza_consumi,OldPartitionData, spark, logger)
    logger.info(s"Remove from table ${etlStage33M2RevisitedDao.tableName} patition less or equal  ${OldPartitionData.toString} done")

    // registro_load
    val (idRun, partitionToRemove) = registroLoadTrasformation.deleteDataRegistroLoad(misureEEArgs.storico)
    logger.info(s"Remove from table ${getRegistroLoadDao.tableName} patition: competenza_consumi ${OldPartitionData.toString} and id_run ${idRun}")
    spark.sql(s" ALTER TABLE  ${getRegistroLoadDao.tableName}   DROP IF EXISTS PARTITION  (${registroLoadSchema.competenza_consumi}=${partitionToRemove}, ${registroLoadSchema.id_run}='${idRun}')")
    logger.info(s"Remove from table ${getRegistroLoadDao.tableName} patition: competenza_consumi ${OldPartitionData.toString} and id_run ${idRun} done")

    //mongo

    //end remove old date data
    logger.info(s"Remove from collection ${getMisureElettriche33MDao.readConfig.databaseName}.${getMisureElettriche33MDao.readConfig.collectionName} documents with competenza_consumi greater than  ${windowTimeValueDefinition.toString}")
    //getMisureElettriche33MDao.removeOldPartition(OldPartitionData)
    getMisureElettriche33MDao.removeOldPartitionsGreaterThan(windowTimeValueDefinition)
    logger.info(s"Remove from collection ${getMisureElettriche33MDao.readConfig.databaseName}.${getMisureElettriche33MDao.readConfig.collectionName} documents with competenza_consumi greater than  ${windowTimeValueDefinition.toString} done")


    //end remove old date data
    logger.info(s"Remove from collection ${getMisureElettriche33MDao.readConfig.databaseName}.${getMisureElettriche33MDao.readConfig.collectionName} documents with competenza_consumi less or equal to  ${OldPartitionData.toString}")
    //getMisureElettriche33MDao.removeOldPartition(OldPartitionData)
    getMisureElettriche33MDao.removeOldPartitionsUpTo(OldPartitionData)
    logger.info(s"Remove from collection ${getMisureElettriche33MDao.readConfig.databaseName}.${getMisureElettriche33MDao.readConfig.collectionName} documents with competenza_consumi less or equal to  ${OldPartitionData.toString} done")

   }
}
