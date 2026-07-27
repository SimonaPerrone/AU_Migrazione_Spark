package it.eng.au.portale_consumi_ee.flow.stage

import it.eng.au.portale_consumi_ee.dao.hive.misure._
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.etlStage3M2ProposedModel
import it.eng.au.portale_consumi_ee.schema.misure.etlStage3M2ProposedSchema
import it.eng.au.portale_consumi_ee.trasformations.{registroLoadTrasformation, stagePhaseTrasformationDifferentAprroach}
import it.eng.au.portale_consumi_ee.utility.args.MisureEEArgsConfig
import it.eng.au.portale_consumi_ee.utility.functions.argumentsUtilitiesExport
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class stagePhase3MInizitialization(implicit spark: SparkSession) {

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
  val etlStage3M2RevisitedDao = new etlStage3M2RevisitedOnHiveDao

  def  run(misureEEArgs:MisureEEArgsConfig):Dataset[etlStage3M2ProposedModel] = {

     val storico = misureEEArgs.storico
     val windowTimeValue = EnvironmentMisure.getProperty("spark.app.mongodb.delay")
     val timeZone = EnvironmentMisure.getProperty("spark.app.time_zone")

     val windowTimeValueDefinition = argumentsUtilitiesExport.annomeseDefiniton(windowTimeValue,timeZone)

     val currentAnnoMeseGiornoLong = argumentsUtilitiesExport.convertUtcToLong(timeZone)
     
     //read input data
     def autolettureDS = getAutolettureDao.read(storico,windowTimeValueDefinition)
     def misureMensiliCDS = getMisureMensiliCDao.read(storico,windowTimeValueDefinition)
     def misureNonOrarieCDS = getMisureNonOrarieCDao.read(storico,windowTimeValueDefinition)
     def misureOrarieCDS = getMisureOrarieCDao.read(storico,windowTimeValueDefinition)
     def voltureDS = getVoltureDao.read(storico,windowTimeValueDefinition)

       logger.info(s"INIZIO FASE DI INIZIALIZZAZIONE TABELLA DI STAGE OK")
       logger.info(s"inizio modifica struttura dati misura.autoletture per inizializzazione tabella stage ")
       def autolettuRevisitedDs = stagePhaseTrasformationDifferentAprroach.autolettureGeneration(autolettureDS)
       logger.info(s"fine modifica struttura dati misura.autoletture per inizializzazione tabella stage ")

       logger.info(s"inizio modifica struttura dati misura.misure_mensili_c per inizializzazione tabella stage ")
       def misureMensiliCRevisitedDS = stagePhaseTrasformationDifferentAprroach.misureMensiliGeneration(misureMensiliCDS)
       logger.info(s"fine modifica struttura dati misura.misure_mensili_c per inizializzazione tabella stage ")

       logger.info(s"inizio modifica struttura dati misura.misure_non_orarie_c per inizializzazione tabella stage ")
       def misureNonOrarieCRevisitedDS = stagePhaseTrasformationDifferentAprroach.misureNonOrarieGeneration(misureNonOrarieCDS)
       logger.info(s"fine modifica struttura dati misura.misure_non_orarie_c per inizializzazione tabella stage ")

       logger.info(s"inizio modifica struttura dati misura.misure_orarie_gg per inizializzazione tabella stage ")
       def misureOrarieCRevisitedDS = stagePhaseTrasformationDifferentAprroach.misureOrarieGeneration(misureOrarieCDS)
       logger.info(s"fine modifica struttura dati misura.misure_orarie_gg per inizializzazione tabella stage ")

       logger.info(s"inizio modifica struttura dati misura.volture per inizializzazione tabella stage ")
       def voltureRevisitedDS = stagePhaseTrasformationDifferentAprroach.voltureGeneration(voltureDS)
       logger.info(s"fine modifica struttura dati misura.volture per inizializzazione tabella stage ")

              logger.info(s"calcolo nuova tabella di stage")
              val etlstageNew = stagePhaseTrasformationDifferentAprroach.calcolo_stage(autolettuRevisitedDs
                ,misureMensiliCRevisitedDS
                ,misureNonOrarieCRevisitedDS
                ,misureOrarieCRevisitedDS
                ,voltureRevisitedDS)
                .withColumn(etlStage3M2ProposedSchema.last_update,lit(currentAnnoMeseGiornoLong).cast(LongType))

     etlstageNew.cache()

     logger.info(s"scrittura finale su ${etlStage3M2RevisitedDao.tableName}")
       etlStage3M2RevisitedDao.write(
         etlstageNew.selectExpr(etlStage3M2ProposedSchema.getValues:_*).as[etlStage3M2ProposedModel],true
       )

     logger.warn("Starting update table register load")
     val dfRegistroLoad = registroLoadTrasformation.updateRegistroLoad(etlstageNew.selectExpr(etlStage3M2ProposedSchema.getValues:_*).as[etlStage3M2ProposedModel]
       ,storico)
     getRegistroLoadDao.write(dfRegistroLoad)
     logger.warn("End update table register load")

     logger.info(s"FINE FASE DI INIZIALIZZAZIONE TABELLA DI STAGE OK")

     etlstageNew.selectExpr(etlStage3M2ProposedSchema.getValues:_*).as[etlStage3M2ProposedModel]
  }

  }
