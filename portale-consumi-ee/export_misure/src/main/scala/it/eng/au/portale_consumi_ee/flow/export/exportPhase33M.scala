package it.eng.au.portale_consumi_ee.flow.`export`

import it.eng.au.portale_consumi_ee.dao.mongo.{MisureElettriche33MDao, MisureElettriche3MDao}
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.etlStage3M2ProposedModel
import it.eng.au.portale_consumi_ee.trasformations.ExportOpertion
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class exportPhase33M(implicit spark: SparkSession) {

  @transient  lazy val logger = Logger.getLogger(getClass.getName)

  //output data
  val geMisureElettriche33MDao = new MisureElettriche33MDao

  val mongoUri = EnvironmentMisure.getProperty("mongodb.db.uri")
  require(mongoUri != null, "MongoDB URI is null. Check that 'mongodb.db.uri' is correctly loaded.")

  def  run( datasetUpdate:Dataset[etlStage3M2ProposedModel]) = {

     //insert new data
     // set ddataset to Dataset[MisureElettricheModel]
     val dataseUpdateCorrectSetUp = ExportOpertion.writeNewData(datasetUpdate)
       geMisureElettriche33MDao
         .write(dataseUpdateCorrectSetUp,mongoUri, true)

   }

  }
