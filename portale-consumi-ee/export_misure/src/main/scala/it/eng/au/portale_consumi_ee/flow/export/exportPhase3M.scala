package it.eng.au.portale_consumi_ee.flow.`export`

import it.eng.au.portale_consumi_ee.dao.hive.misure._
import it.eng.au.portale_consumi_ee.dao.mongo.MisureElettriche3MDao
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.etlStage3M2ProposedModel
import it.eng.au.portale_consumi_ee.trasformations.{ExportOpertion, stagePhaseTrasformationDifferentAprroach}
import it.eng.au.portale_consumi_ee.utility.args.MisureEEArgsConfig
import it.eng.au.portale_consumi_ee.utility.functions.argumentsUtilitiesExport
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class exportPhase3M(implicit spark: SparkSession) {

  @transient  lazy val logger = Logger.getLogger(getClass.getName)
  import spark.implicits._

  //output data
  val geMisureElettriche3MDao = new MisureElettriche3MDao

  val mongoUri = EnvironmentMisure.getProperty("mongodb.db.uri")
  require(mongoUri != null, "MongoDB URI is null. Check that 'mongodb.db.uri' is correctly loaded.")


  def  run(datasetNew:Dataset[etlStage3M2ProposedModel], datasetUpdate:Dataset[etlStage3M2ProposedModel]) = {

     //insert new data
     // set ddataset to Dataset[MisureElettricheModel]
     val datasetNewCorrectSetUp = ExportOpertion.writeNewData(datasetNew)


     // yyyyMM  in integer format (year and month when the code is running)
     val currentDate = LocalDate.now()
     val currentAnnoMese = currentDate.format(DateTimeFormatter.ofPattern("yyyyMM")).toInt

     //geMisureElettriche3MDao.moveDataToBackup(currentAnnoMese)
     val valideRemove = geMisureElettriche3MDao.removeOldPartition(currentAnnoMese)
     if (valideRemove) {
       geMisureElettriche3MDao.write(datasetNewCorrectSetUp,mongoUri, false)
       //update data
       // set ddataset to Dataset[MisureElettricheModel]
       val datasetUpdateSetUp = ExportOpertion.writeNewData(datasetUpdate)
       geMisureElettriche3MDao.write(datasetUpdateSetUp,mongoUri, true)

       //remove unused data
//       val windowTimeValue = EnvironmentMisure.getProperty("spark.app.mongodb.delay")
//       val timeZone = EnvironmentMisure.getProperty("spark.app.time_zone")
//
//
//       val windowTimeValueDefinition = argumentsUtilitiesExport.annomeseDefiniton(windowTimeValue,timeZone)
//       logger.warn(s"remove data from Mongo Collection: ${geMisureElettriche3MDao.readConfig.databaseName}.${geMisureElettriche3MDao.readConfig.collectionName} with comperenza_consumi less or equal to: ${windowTimeValueDefinition}")
//       geMisureElettriche3MDao.removeOldPartitionsUpTo(windowTimeValueDefinition)
//       logger.warn("remove operation terminated")
     }else {
       logger.warn("Update MongoDB failed")
     }



   }

  }
