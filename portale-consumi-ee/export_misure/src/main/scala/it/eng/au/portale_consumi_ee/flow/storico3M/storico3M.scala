package it.eng.au.portale_consumi_ee.flow.storico3M

import it.eng.au.portale_consumi_ee.common.flow.FlowUnitOutput
import it.eng.au.portale_consumi_ee.common.utility.functions.argumentsUtilities
import it.eng.au.portale_consumi_ee.dao.hive.misure.{etlStage3M2RevisitedOnHiveDao, registroLoadDao}
import it.eng.au.portale_consumi_ee.dao.mongo.MisureElettriche3MDao
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.flow.`export`.exportPhase3M
import it.eng.au.portale_consumi_ee.flow.stage.stagePhase3M
import it.eng.au.portale_consumi_ee.schema.misure.{etlStage3M2ProposedSchema, registroLoadSchema}
import it.eng.au.portale_consumi_ee.trasformations.registroLoadTrasformation
import it.eng.au.portale_consumi_ee.utility.args.MisureEEArgsConfig
import it.eng.au.portale_consumi_ee.utility.functions.argumentsUtilitiesExport
import org.apache.spark.sql.{Dataset, SparkSession}

class storico3M(implicit spark: SparkSession)  extends FlowUnitOutput{

  //define stage flow
  def setStage = new stagePhase3M()
  //define export flow
  def setExportMongo = new exportPhase3M()
  val etlStage3M2RevisitedDao = new etlStage3M2RevisitedOnHiveDao
  val getRegistroLoadDao = new registroLoadDao

  //output mongo
  val getMisureElettriche3MDao = new MisureElettriche3MDao

  def run(misureEEArgs:MisureEEArgsConfig) = {

     logger.warn("Starting stage phase")
     val (stageDatNew,stageDatUpdated,finalJoinHashDateLastModifiedUpdatedWrite) = setStage.run(misureEEArgs)
     logger.warn("End stage phase")

     logger.warn("Starting export phase")
       setExportMongo.run(stageDatNew, stageDatUpdated)

     logger.warn("End export phase")
    logger.info(s"scrittura finale su ${etlStage3M2RevisitedDao.tableName}")
    etlStage3M2RevisitedDao.write(
      finalJoinHashDateLastModifiedUpdatedWrite,true
    )
    //start remove old data
    //hive
    // etl_stage_33m_2
    val windowTimeValue = EnvironmentMisure.getProperty("spark.app.mongodb.delay")
    val timeZone = EnvironmentMisure.getProperty("spark.app.time_zone")
    val monthLimitFlow3M = argumentsUtilitiesExport.annomeseDefiniton(windowTimeValue,timeZone)
    logger.info(s"Remove from table ${etlStage3M2RevisitedDao.tableName} patition less or equal  ${monthLimitFlow3M.toString}")
    //spark.sql(s"ALTER TABLE ${etlStage3M2RevisitedDao.tableName}  DROP IF EXISTS PARTITION (${etlStage3M2ProposedSchema.competenza_consumi}<=${monthLimitFlow3M})")
    argumentsUtilities.dropPartitionsBeforeOrEqualAnnomeseRiferimento( etlStage3M2RevisitedDao.tableName,etlStage3M2ProposedSchema.competenza_consumi,monthLimitFlow3M, spark, logger)
    logger.info(s"Remove from table ${etlStage3M2RevisitedDao.tableName} patition less or equal  ${monthLimitFlow3M.toString} done")
    // registro_load
    val (idRun, partitionToRemove) = registroLoadTrasformation.deleteDataRegistroLoad(misureEEArgs.storico)
    logger.info(s"Remove from table ${getRegistroLoadDao.tableName} patition: competenza_consumi ${monthLimitFlow3M.toString} and id_run ${idRun}")
    spark.sql(s" ALTER TABLE  ${getRegistroLoadDao.tableName}   DROP IF EXISTS PARTITION  (${registroLoadSchema.competenza_consumi}=${partitionToRemove}, ${registroLoadSchema.id_run}='${idRun}')")
    logger.info(s"Remove from table ${getRegistroLoadDao.tableName} patition: competenza_consumi ${monthLimitFlow3M.toString} and id_run ${idRun} done")

    //mongo
    //end remove old date data
    logger.info(s"Remove from collection ${getMisureElettriche3MDao.readConfig.databaseName}.${getMisureElettriche3MDao.readConfig.collectionName} documents with competenza_consumi less or equal to  ${monthLimitFlow3M.toString}")
    //getMisureElettriche3MDao.removeOldPartition(monthLimitFlow3M)
    getMisureElettriche3MDao.removeOldPartitionsUpTo(monthLimitFlow3M)

    logger.info(s"Remove from collection ${getMisureElettriche3MDao.readConfig.databaseName}.${getMisureElettriche3MDao.readConfig.collectionName} documents with competenza_consumi less or equal to  ${monthLimitFlow3M.toString} done")


  }

}
