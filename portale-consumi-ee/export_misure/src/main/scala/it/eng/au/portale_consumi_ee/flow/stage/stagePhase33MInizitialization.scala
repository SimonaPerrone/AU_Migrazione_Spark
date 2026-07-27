package it.eng.au.portale_consumi_ee.flow.stage

import it.eng.au.portale_consumi_ee.dao.hive.misure._
import it.eng.au.portale_consumi_ee.dao.mongo.MisureElettriche33MDao
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.{etlStage3M2ProposedModel, registroLoadModel}
import it.eng.au.portale_consumi_ee.schema.misure.etlStage3M2ProposedSchema
import it.eng.au.portale_consumi_ee.trasformations.{ExportOpertion, registroLoadTrasformation, stagePhaseTrasformationDifferentAprroach}
import it.eng.au.portale_consumi_ee.utility.args.MisureEEArgsConfig
import it.eng.au.portale_consumi_ee.utility.functions.argumentsUtilitiesExport
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.LongType

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class stagePhase33MInizitialization(implicit spark: SparkSession) {

  @transient  lazy val logger = Logger.getLogger(getClass.getName)
  import spark.implicits._

  //input data
  val getAutolettureDao = new autolettureDao
  val getMisureMensiliCDao = new misureMensiliCDao
  val getMisureNonOrarieCDao = new misureNonOrarieCDao
  val getMisureOrarieCDao = new misureOrarieCDao
  val getVoltureDao = new voltureDao
  val getRegistroLoadDao = new registroLoadDao

  //output data hive
  val etlStage33M2RevisitedDao = new etlStage33M2RevisitedOnHiveDao

  //output data mongo
  val geMisureElettriche33MDao = new MisureElettriche33MDao

  val mongoUri = EnvironmentMisure.getProperty("mongodb.db.uri")
  require(mongoUri != null, "MongoDB URI is null. Check that 'mongodb.db.uri' is correctly loaded.")

  def  run(misureEEArgs:MisureEEArgsConfig) = {

     val storico = misureEEArgs.storico
     val windowTimeValue = EnvironmentMisure.getProperty("spark.app.mongodb.delay")
     val timeZone = EnvironmentMisure.getProperty("spark.app.time_zone")

     val windowTimeValueDefinition = argumentsUtilitiesExport.annomeseDefiniton(windowTimeValue,timeZone)

     val currentAnnoMeseGiornoLong = argumentsUtilitiesExport.convertUtcToLong(timeZone)

     val windowsStoric = argumentsUtilitiesExport.generatePastMonths(windowTimeValueDefinition,windowTimeValue)

     logger.info(s"INIZIO FASE DI INIZIALIZZAZIONE TABELLA DI STAGE 33M")
     for (listAnnomese <- windowsStoric) {
       val listAnnomeseString = listAnnomese.mkString(", ")
       println(s"Inizio fase inizializzazione tabella di stage per annomese: ${listAnnomeseString}")
       logger.info(s"Inizio fase inizializzazione tabella di stage per annomese: ${listAnnomeseString}")
       //read input data
       def autolettureDS = getAutolettureDao.readWindowStoric(storico,listAnnomese)
       def misureMensiliCDS = getMisureMensiliCDao.readWindowStoric(storico,listAnnomese)
       def misureNonOrarieCDS = getMisureNonOrarieCDao.readWindowStoric(storico,listAnnomese)
       def misureOrarieCDS = getMisureOrarieCDao.readWindowStoric(storico,listAnnomese)
       def voltureDS = getVoltureDao.readWindowStoric(storico,listAnnomese)

       logger.info(s"inizio modifica struttura dati misura.autoletture per export mongoDB su annomese: ${listAnnomese}")
       def autolettuRevisitedDs = stagePhaseTrasformationDifferentAprroach.autolettureGeneration(autolettureDS)
       logger.info(s"fine modifica struttura dati misura.autoletture per export mongoDB su annomese: ${listAnnomese} ")

       logger.info(s"inizio modifica struttura dati misura.misure_mensili_c per export mongoDB su annomese: ${listAnnomese} ")
       def misureMensiliCRevisitedDS = stagePhaseTrasformationDifferentAprroach.misureMensiliGeneration(misureMensiliCDS)
       logger.info(s"fine modifica struttura dati misura.misure_mensili_c per export mongoDB su annomese: ${listAnnomese} ")

       logger.info(s"inizio modifica struttura dati misura.misure_non_orarie_c per export mongoDB su annomese: ${listAnnomese} ")
       def misureNonOrarieCRevisitedDS = stagePhaseTrasformationDifferentAprroach.misureNonOrarieGeneration(misureNonOrarieCDS)
       logger.info(s"fine modifica struttura dati misura.misure_non_orarie_c per export mongoDB su annomese: ${listAnnomese} ")

       logger.info(s"inizio modifica struttura dati misura.misure_orarie_gg per export mongoDB su annomese: ${listAnnomese} ")
       def misureOrarieCRevisitedDS = stagePhaseTrasformationDifferentAprroach.misureOrarieGeneration(misureOrarieCDS)
       logger.info(s"fine modifica struttura dati misura.misure_orarie_gg per export mongoDB su annomese: ${listAnnomese} ")

       logger.info(s"inizio modifica struttura dati misura.volture per export mongoDB su annomese: ${listAnnomese} ")
       def voltureRevisitedDS = stagePhaseTrasformationDifferentAprroach.voltureGeneration(voltureDS)
       logger.info(s"fine modifica struttura dati misura.volture per export mongoDB su annomese: ${listAnnomese} ")

       logger.info(s"calcolo nuova tabella di stage per annomese: ${listAnnomese}")
       val etlstageNew = stagePhaseTrasformationDifferentAprroach.calcolo_stage(autolettuRevisitedDs
         ,misureMensiliCRevisitedDS
         ,misureNonOrarieCRevisitedDS
         ,misureOrarieCRevisitedDS
         ,voltureRevisitedDS)
         .withColumn(etlStage3M2ProposedSchema.last_update,lit(currentAnnoMeseGiornoLong).cast(LongType))


       logger.info(s"scrittura finale su ${etlStage33M2RevisitedDao.tableName}  per annomese: ${listAnnomese}")
       etlStage33M2RevisitedDao.write(
         etlstageNew.selectExpr(etlStage3M2ProposedSchema.getValues:_*).as[etlStage3M2ProposedModel],true
       )
       logger.info(s"Fine fase inizializzazione tabella di stage per annomese: ${listAnnomese}")
       val etlstageNewToMongo = etlstageNew.selectExpr(etlStage3M2ProposedSchema.getValues:_*).as[etlStage3M2ProposedModel]

       logger.warn("Starting update table register load")
       val dfRegistroLoad = registroLoadTrasformation.updateRegistroLoad(
         etlstageNewToMongo
         ,storico)
       getRegistroLoadDao.writeRegistroLoad(dfRegistroLoad)
       logger.warn("End update table register load")

       logger.warn(s"Starting export phase for data with competenza_consumi: ${listAnnomese}")
       val datasetNewCorrectSetUp = ExportOpertion.writeNewData(etlstageNewToMongo)
       geMisureElettriche33MDao.write(datasetNewCorrectSetUp,mongoUri)
       logger.warn(s"Starting export phase for data with competenza_consumi: ${listAnnomese}")

     }

     logger.info(s"FINE FASE DI INIZIALIZZAZIONE TABELLA DI STAGE 33M")

   }
  }
