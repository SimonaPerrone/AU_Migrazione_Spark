package it.eng.au.ccgPubblicazione.controller.sessionfactory.impl

import it.eng.au.ccgPubblicazione.args.FlowArgsConfig
import it.eng.au.ccgPubblicazione.controller.runnablefile.impl.agg.aggregato.{AggGestoreAggregato, AggIdAggregato, AggUdbAggregato, AggUddAggregato}
import it.eng.au.ccgPubblicazione.controller.runnablefile.impl.agg.esclusi.{AggGestoreEsclusi, AggIdEsclusi, AggUdbEsclusi, AggUddEsclusi}
import it.eng.au.ccgPubblicazione.controller.runnablefile.impl.agg.incoerentiab.{AggGestoreIncoerentiAB, AggIdIncoerentiAB, AggUdbIncoerentiAB, AggUddIncoerentiAB}
import it.eng.au.ccgPubblicazione.controller.runnablefile.impl.agg.incoerentic.{AggGestoreIncoerentiC, AggIdIncoerentiC, AggUdbIncoerentiC, AggUddIncoerentiC}
import it.eng.au.ccgPubblicazione.controller.runnablefile.traits.RunnableAggregator
import it.eng.au.ccgPubblicazione.controller.sessionfactory.traits.AggSbgFlow
import it.eng.au.ccgPubblicazione.dao.agg.{DailyConsumptionAggDao, DailyConsumptionAggEsclusiDao, DailyConsumptionAggIncoerentiDao, ValidationAggDao}
import it.eng.au.ccgPubblicazione.schema.aggsbg.{AggConsumptionRequestRunnableSchema, DailyConsumptionAggEsclusiSchema, DailyConsumptionAggIncoerentiSchema, DailyConsumptionAggSchema, ValidatedFlowsAggSchema}
import it.eng.au.ccgPubblicazione.utility.Constants.{AGG, AGG_LOG}
import it.eng.au.ccgPubblicazione.utility.{Environment, VersionLoggingUtility}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, concat, lit, substring, when}
import org.apache.spark.sql.{Column, DataFrame}

import java.sql.Timestamp
import java.time.LocalDateTime

object AggSession extends AggSbgFlow {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  override val session: String = AGG
  override val sessionLog: String = AGG_LOG

  def setEnvironment(flowArgsConfig: FlowArgsConfig): Unit = {
    logger.warn(s"$sessionLog Start Pubblicazione CCG - Aggiustamento Gas")
    VersionLoggingUtility.printVersionInfo()

    Environment.getOrCreate("Pubblicazione Contatore Consumi Gas - Aggiustamento Gas", flowArgsConfig.pathToProperties)
    Environment.setProperty("daterun", Timestamp.valueOf(LocalDateTime.now()).toString)

    logger.warn(s"$sessionLog Run Pubblicazione CCG - Aggiustamento Gas")

    logger.warn(s"$sessionLog Properties:")
//    logger.warn(s"$sessionLog ${Environment.printProperties}")
    //    logger.warn(s"$sessionLog Execution ID: ${Environment.executionId}")
    //    logger.warn(s"$sessionLog Date: ${Environment.getPartitionDate}")
    logger.warn(s"$sessionLog Date to run: ${Environment.getProperty("daterun")}")
    logger.warn(s"$sessionLog applicationID=${Environment.spark.sparkContext.applicationId}")

  }

  override def readConsumptionWithLastPartition: (DataFrame, String) = {
    val partition = DailyConsumptionAggDao.readLastPartition._2
    val dailyConsumption = DailyConsumptionAggDao.readLastPartition._1
    val dailyConsumptionIncoerenti = DailyConsumptionAggIncoerentiDao.readLastPartition._1.selectExpr(DailyConsumptionAggIncoerentiSchema.getValues:_*)
    val dailyConsumptionEsclusi = DailyConsumptionAggEsclusiDao.readLastPartition._1.selectExpr(DailyConsumptionAggEsclusiSchema.getValues:_*)

    val colsForJoinDailyC = Seq("pdr", "date", "session", "executionid")
    val esclusiFlag = "esclusiFlag"

    val dailyConsumptionSterilized = dailyConsumption
      .join(dailyConsumptionIncoerenti
        , colsForJoinDailyC, "left")
      .join(dailyConsumptionEsclusi.withColumn(esclusiFlag, lit(true))
        , colsForJoinDailyC, "left")
      .withColumn(DailyConsumptionAggSchema.valueNotSterilized, col(DailyConsumptionAggSchema.value))
      .withColumn(DailyConsumptionAggSchema.value, when(col(DailyConsumptionAggIncoerentiSchema.isDayAnomalous), col(DailyConsumptionAggSchema.valuef3)).otherwise(col(DailyConsumptionAggSchema.value)))
      .withColumn(DailyConsumptionAggSchema.value, when(col(esclusiFlag), col(DailyConsumptionAggSchema.valuef3)).otherwise(col(DailyConsumptionAggSchema.value)))
      .selectExpr(DailyConsumptionAggSchema.getValues.+:(esclusiFlag).+:(DailyConsumptionAggIncoerentiSchema.isPdrAnomalousGDM.toString).+:(DailyConsumptionAggIncoerentiSchema.isDayAnomalous.toString): _*)

    (dailyConsumptionSterilized, partition)
  }

  override def readValidation(partition: String): DataFrame = {
    ValidationAggDao.readPartition(partition)
  }

  override val idPdrElencoFlussi: RunnableAggregator = AggIdAggregato
  override val uddPdrElencoFlussi: RunnableAggregator = AggUddAggregato
  override val udbPdrElencoFlussi: RunnableAggregator = AggUdbAggregato
  override val gestorePdrElencoFlussi: RunnableAggregator = AggGestoreAggregato

  override val idPdrElencoFlussiIncoerentiAB: RunnableAggregator = AggIdIncoerentiAB
  override val uddPdrElencoFlussiIncoerentiAB: RunnableAggregator = AggUddIncoerentiAB
  override val udbPdrElencoFlussiIncoerentiAB: RunnableAggregator = AggUdbIncoerentiAB
  override val gestorePdrElencoFlussiIncoerentiAB: RunnableAggregator = AggGestoreIncoerentiAB

  override val idPdrElencoFlussiIncoerentiC: RunnableAggregator = AggIdIncoerentiC
  override val uddPdrElencoFlussiIncoerentiC: RunnableAggregator = AggUddIncoerentiC
  override val udbPdrElencoFlussiIncoerentiC: RunnableAggregator = AggUdbIncoerentiC
  override val gestorePdrElencoFlussiIncoerentiC: RunnableAggregator = AggGestoreIncoerentiC

  override val idPdrElencoFlussiEsclusi: RunnableAggregator = AggIdEsclusi
  override val uddPdrElencoFlussiEsclusi: RunnableAggregator = AggUddEsclusi
  override val udbPdrElencoFlussiEsclusi: RunnableAggregator = AggUdbEsclusi
  override val gestorePdrElencoFlussiEsclusi: RunnableAggregator = AggGestoreEsclusi

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

  val annoMeseFieldConsumption: String = DailyConsumptionAggSchema.annoMese.toString

  override def filterAnnoMese(annoField: String, meseField: String): Column = (col(annoField).isNull || substring(col(annoMeseFieldConsumption), 0, 4) === col(annoField)) &&
    (col(meseField).isNull || col(annoMeseFieldConsumption) === concat(col(annoField), col(meseField)))
}
