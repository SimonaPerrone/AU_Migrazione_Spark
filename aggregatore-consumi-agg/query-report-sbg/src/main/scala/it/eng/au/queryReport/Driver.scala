package it.eng.au.queryReport

import it.eng.au.aggregatoreConsumiCommon.controller.CoefficientController
import it.eng.au.aggregatoreConsumiCommon.controller.impl.dtg.Dtg.findAnomalousDays
import it.eng.au.aggregatoreConsumiCommon.dao.agg.{DailyConsumptionDao, DailyConsumptionEsclusiDao, DailyConsumptionIncoerentiDao}
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DailyConsumptionInputProcessSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.{Environment, LogUtility}
import it.eng.au.queryReport.factory.QueryFactory
import it.eng.au.queryReport.factory.QueryFactory.listOfQueriesWithIncoerentiGDM
import it.eng.au.queryReport.utility.Constants
import it.eng.au.queryReport.utility.Constants.TIMESTAMP_FORMAT_WITHOUT_MIL
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, lit, when}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime

object Driver {
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    try {
      val applicationName = "[SBG] Query Report"
      // init spark context and load job settings
      Environment.getOrCreate(applicationName, args(0))
      val now = new SimpleDateFormat(TIMESTAMP_FORMAT_WITHOUT_MIL).format(Timestamp.valueOf(LocalDateTime.now()))
      Environment.setProperty("daterun", now)

      LogUtility.printInitialLog(applicationName, Constants.LOG)

      // Estrae la lista delle query da eseguire
      val factory = QueryFactory.getQueries(args)

      // Legge e prepara la tabella dei consumi
      val dailyConsumptionDao = new DailyConsumptionDao()
      val dailyConsumptionDF = dailyConsumptionDao
        .prepare(Environment.getDailyConsumptionExecutionid)

      val dailyConsumptionIncoerentiDao = new DailyConsumptionIncoerentiDao
      val dailyConsumptionIncoerentiDF = dailyConsumptionIncoerentiDao
        .readPartition(Environment.getDailyConsumptionExecutionid)

      val dailyConsumptionEsclusiDao = new DailyConsumptionEsclusiDao
      val dailyConsumptionEsclusiDF = dailyConsumptionEsclusiDao
        .readPartition(Environment.getDailyConsumptionExecutionid)

      val colsForJoinDailyC = Seq("pdr", "date", "session", "executionid")
      val joinType = "left"

      val dailyConsumptionJoinDF =
        dailyConsumptionDF
          .join(dailyConsumptionIncoerentiDF, colsForJoinDailyC, joinType)
          .join(dailyConsumptionEsclusiDF, colsForJoinDailyC, joinType)
          .withColumn(DailyConsumptionInputProcessSchema.value, when(col(DailyConsumptionInputProcessSchema.esclusiFlag), lit(col(DailyConsumptionInputProcessSchema.sterilizedValueE))).otherwise(col(DailyConsumptionInputProcessSchema.value)))
          .withColumn(DailyConsumptionInputProcessSchema.value, when(col(DailyConsumptionInputProcessSchema.incoerentiFlag), lit(col(DailyConsumptionInputProcessSchema.sterilizedValueI))).otherwise(col(DailyConsumptionInputProcessSchema.value)))
        .selectExpr(DailyConsumptionInputProcessSchema.getValues:_*)

      // Se sono inclusi gli incoerenti GDM, allora aggiorna il coefficiente di correzione da rcugas
      val dailyConsumption = if (factory.exists(listOfQueriesWithIncoerentiGDM.contains(_)))
        CoefficientController.attachCoefficient(dailyConsumptionJoinDF)
      else dailyConsumptionJoinDF

      if (factory.size > 1) dailyConsumption.cache

      factory.foreach(query => {
        log.warn(s"Running query ${query.queryName}")
        query.runQuery(dailyConsumption)
        log.warn(s"Finished query ${query.queryName}")
      })

      LogUtility.printFinalLog(applicationName, Constants.LOG)
    } catch {
      case e: Exception => log.error(e.getStackTrace); throw e
      case e: Error => log.error(e.getStackTrace); throw e
    }
  }
}