package it.eng.au.portale_consumi_ee.flow.storico3M

import it.eng.au.portale_consumi_ee.common.flow.FlowUnitOutput
import it.eng.au.portale_consumi_ee.dao.mongo.MisureElettriche3MDao
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.flow.`export`.exportPhase3M
import it.eng.au.portale_consumi_ee.flow.stage.{stagePhase3M, stagePhase3MInizitialization}
import it.eng.au.portale_consumi_ee.trasformations.ExportOpertion
import it.eng.au.portale_consumi_ee.utility.args.MisureEEArgsConfig
import org.apache.spark.sql.SparkSession

class storico3MInizialization(implicit spark: SparkSession)  extends FlowUnitOutput{

  //define export Dao
  val geMisureElettriche3MDao = new MisureElettriche3MDao


  val mongoUri = EnvironmentMisure.getProperty("mongodb.db.uri")
  require(mongoUri != null, "MongoDB URI is null. Check that 'mongodb.db.uri' is correctly loaded.")



  def run(misureEEArgs:MisureEEArgsConfig) = {

     logger.warn("Starting stage phase initialization")
     val stageDatNew = stagePhase3MInizitialization().run(misureEEArgs)
     logger.warn("End stage phase initzialization")

     logger.warn("Starting export phase")

     //insert new data
     // set ddataset to Dataset[MisureElettricheModel]
     val datasetNewCorrectSetUp = ExportOpertion.writeNewData(stageDatNew)
     geMisureElettriche3MDao.write(datasetNewCorrectSetUp,mongoUri)

     logger.warn("End export phase")
   }

}
