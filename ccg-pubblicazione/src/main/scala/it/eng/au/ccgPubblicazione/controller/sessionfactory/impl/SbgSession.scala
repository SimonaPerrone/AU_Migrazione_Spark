package it.eng.au.ccgPubblicazione.controller.sessionfactory.impl

import it.eng.au.ccgPubblicazione.args.FlowArgsConfig
import it.eng.au.ccgPubblicazione.controller.runnablefile.impl.sbg.aggregato.{SbgGestoreAggregato, SbgIdAggregato, SbgUdbAggregato, SbgUddAggregato}
import it.eng.au.ccgPubblicazione.controller.runnablefile.impl.sbg.esclusi.{SbgGestoreEsclusi, SbgIdEsclusi, SbgUdbEsclusi, SbgUddEsclusi}
import it.eng.au.ccgPubblicazione.controller.runnablefile.impl.sbg.incoerentiab.{SbgGestoreIncoerentiAB, SbgIdIncoerentiAB, SbgUdbIncoerentiAB, SbgUddIncoerentiAB}
import it.eng.au.ccgPubblicazione.controller.runnablefile.impl.sbg.incoerentic.{SbgGestoreIncoerentiC, SbgIdIncoerentiC, SbgUdbIncoerentiC, SbgUddIncoerentiC}
import it.eng.au.ccgPubblicazione.controller.runnablefile.traits.RunnableAggregator
import it.eng.au.ccgPubblicazione.controller.sessionfactory.traits.AggSbgFlow
import it.eng.au.ccgPubblicazione.dao.sbg.{DailyConsumptionSbgDao, DailyConsumptionSbgEsclusiDao, DailyConsumptionSbgIncoerentiDao, ValidationSbgDao}
import it.eng.au.ccgPubblicazione.schema.aggsbg.{AggConsumptionRequestRunnableSchema, DailyConsumptionAggSchema, DailyConsumptionSbgEsclusiSchema, DailyConsumptionSbgIncoerentiSchema, ValidatedFlowsAggSchema}
import it.eng.au.ccgPubblicazione.utility.Constants.{SBG, SBG_LOG}
import it.eng.au.ccgPubblicazione.utility.{Environment, VersionLoggingUtility}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}

import java.sql.Timestamp
import java.time.LocalDateTime

object SbgSession extends AggSbgFlow {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  override val session: String = SBG
  override val sessionLog: String = SBG_LOG

  def setEnvironment(flowArgsConfig: FlowArgsConfig): Unit = {
    logger.warn(s"$sessionLog Start Pubblicazione CCG - Sessione Bilanciamento Gas")
    VersionLoggingUtility.printVersionInfo()

    Environment.getOrCreate("Pubblicazione Contatore Consumi Gas - Sessione Bilanciamento Gas", flowArgsConfig.pathToProperties)
    Environment.setProperty("daterun", Timestamp.valueOf(LocalDateTime.now()).toString)

    logger.warn(s"$sessionLog Run Pubblicazione CCG - Sessione Bilanciamento Gas")

    logger.warn(s"$sessionLog Properties:")
//    logger.warn(s"$sessionLog ${Environment.printProperties}")
    //    logger.warn(s"$sessionLog Execution ID: ${Environment.executionId}")
    //    logger.warn(s"$sessionLog Date: ${Environment.getPartitionDate}")
    logger.warn(s"$sessionLog Date to run: ${Environment.getProperty("daterun")}")
    logger.warn(s"$sessionLog applicationID=${Environment.spark.sparkContext.applicationId}")

  }

  override def readConsumptionWithLastPartition: (DataFrame, String) = {
    val partition = DailyConsumptionSbgDao.readLastPartition._2
    val dailyConsumption = DailyConsumptionSbgDao.readLastPartition._1
    val dailyConsumptionIncoerenti = DailyConsumptionSbgIncoerentiDao.readLastPartition._1.selectExpr(DailyConsumptionSbgIncoerentiSchema.getValues:_*)
    val dailyConsumptionEsclusi = DailyConsumptionSbgEsclusiDao.readLastPartition._1.selectExpr(DailyConsumptionSbgEsclusiSchema.getValues:_*)

    val colsForJoinDailyC = Seq("pdr", "date", "session", "executionid")
    val esclusiFlag = "esclusiFlag"

    val dailyConsumptionSterilized = dailyConsumption
      .join(dailyConsumptionIncoerenti
        , colsForJoinDailyC, "left")
      .join(dailyConsumptionEsclusi.withColumn(esclusiFlag, lit(true))
        , colsForJoinDailyC, "left")
      .withColumn(DailyConsumptionAggSchema.valueNotSterilized, col(DailyConsumptionAggSchema.value))
      .withColumn(DailyConsumptionAggSchema.value, when(col(DailyConsumptionSbgIncoerentiSchema.isDayAnomalous), col(DailyConsumptionAggSchema.valuef3)).otherwise(col(DailyConsumptionAggSchema.value)))
      .withColumn(DailyConsumptionAggSchema.value, when(col(esclusiFlag), col(DailyConsumptionAggSchema.valuef3)).otherwise(col(DailyConsumptionAggSchema.value)))
      .selectExpr(DailyConsumptionAggSchema.getValues.+:(esclusiFlag).+:(DailyConsumptionSbgIncoerentiSchema.isPdrAnomalousGDM.toString).+:(DailyConsumptionSbgIncoerentiSchema.isDayAnomalous.toString): _*)

    (dailyConsumptionSterilized, partition)
  }

  override def readValidation(partition: String): DataFrame = {
    ValidationSbgDao.readPartition(partition)
  }

  override val idPdrElencoFlussi: RunnableAggregator = SbgIdAggregato
  override val uddPdrElencoFlussi: RunnableAggregator = SbgUddAggregato
  override val udbPdrElencoFlussi: RunnableAggregator = SbgUdbAggregato
  override val gestorePdrElencoFlussi: RunnableAggregator = SbgGestoreAggregato

  override val idPdrElencoFlussiIncoerentiAB: RunnableAggregator = SbgIdIncoerentiAB
  override val uddPdrElencoFlussiIncoerentiAB: RunnableAggregator = SbgUddIncoerentiAB
  override val udbPdrElencoFlussiIncoerentiAB: RunnableAggregator = SbgUdbIncoerentiAB
  override val gestorePdrElencoFlussiIncoerentiAB: RunnableAggregator = SbgGestoreIncoerentiAB

  override val idPdrElencoFlussiIncoerentiC: RunnableAggregator = SbgIdIncoerentiC
  override val uddPdrElencoFlussiIncoerentiC: RunnableAggregator = SbgUddIncoerentiC
  override val udbPdrElencoFlussiIncoerentiC: RunnableAggregator = SbgUdbIncoerentiC
  override val gestorePdrElencoFlussiIncoerentiC: RunnableAggregator = SbgGestoreIncoerentiC

  override val idPdrElencoFlussiEsclusi: RunnableAggregator = SbgIdEsclusi
  override val uddPdrElencoFlussiEsclusi: RunnableAggregator = SbgUddEsclusi
  override val udbPdrElencoFlussiEsclusi: RunnableAggregator = SbgUdbEsclusi
  override val gestorePdrElencoFlussiEsclusi: RunnableAggregator = SbgGestoreEsclusi

  override val pivaUddFieldConsumption: String = DailyConsumptionAggSchema.pivaUdd.toString
  override val pivaUdbFieldConsumption: String = DailyConsumptionAggSchema.pivaUdb.toString
  override val pivaIdFieldConsumption: String = DailyConsumptionAggSchema.pivaDistr.toString
  override val pivaGestoreFieldConsumption: String = AggConsumptionRequestRunnableSchema.pivaGestore.toString
  override val pdrFieldConsumption: String = DailyConsumptionAggSchema.pdr.toString
  override val pdrFieldValidation: String = ValidatedFlowsAggSchema.pdr.toString
  override val idRichiestaFields: String = AggConsumptionRequestRunnableSchema.idRichiesta.toString
//  override val dataRichiestaFields: String = AggConsumptionRequestRunnableSchema.dataRichiesta.toString
  override val filtroFieldCodProfConsumption: String = DailyConsumptionAggSchema.codProfStd.toString

  override val fieldsConsumptionRequestRunnable: List[String] = AggConsumptionRequestRunnableSchema.getValues
  override val filtroFiledCodRemiConsumption: String = DailyConsumptionAggSchema.codRemi.toString
  override val filtroFiledTrattamentoConsumption: String = DailyConsumptionAggSchema.treatment.toString
}