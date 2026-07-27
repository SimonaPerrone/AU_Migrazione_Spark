package it.eng.au.aggregatoreConsumiAgg

import it.eng.au.aggregatoreConsumiAgg.factory.AggregatorFactory
import it.eng.au.aggregatoreConsumiAgg.factory.AggregatorFactory.listOfAggregatorsWithIncoerentiGDM
import it.eng.au.aggregatoreConsumiAgg.utility.Constants
import it.eng.au.aggregatoreConsumiCommon.controller.CoefficientController
import it.eng.au.aggregatoreConsumiCommon.dao.agg.{DailyConsumptionDao, DailyConsumptionEsclusiDao, DailyConsumptionIncoerentiDao}
import it.eng.au.aggregatoreConsumiCommon.dao.rcugas.{RcugasPdrDao, RcugasVarMisuratoreDao}
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DailyConsumptionInputProcessSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.{Environment, LogUtility}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, lit, when}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime

object Driver {
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val applicationName = "Aggregatore Consumi Aggiustamento"
      // init spark context and load job settings
      Environment.getOrCreate(applicationName, args(0))

      val now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Timestamp.valueOf(LocalDateTime.now()))
      Environment.setProperty("daterun", now)
      Environment.setProperty("date.run", now)
      Environment.setProperty("publication.type", "AGG")

      LogUtility.printInitialLog(applicationName, Constants.LOG)

      // Estrae la lista di pubblicazioni (aggregators) da eseguire
      val aggregators = AggregatorFactory.getAggregators

      // Legge e prepara le tabelle dei consumi
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
      val joinType: String = "left"
      val treatmentPub: String = "treatmentPub"

      val dailyConsumptionJoinDF =
        dailyConsumptionDF
          .join(dailyConsumptionIncoerentiDF, colsForJoinDailyC, joinType)
          .join(dailyConsumptionEsclusiDF, colsForJoinDailyC, joinType)
          .withColumn(DailyConsumptionInputProcessSchema.value, when(col(DailyConsumptionInputProcessSchema.esclusiFlag), lit(col(DailyConsumptionInputProcessSchema.sterilizedValueE))).otherwise(col(DailyConsumptionAggSchema.value)))
          .withColumn(DailyConsumptionInputProcessSchema.value, when(col(DailyConsumptionInputProcessSchema.incoerentiFlag), lit(col(DailyConsumptionInputProcessSchema.sterilizedValueI))).otherwise(col(DailyConsumptionAggSchema.value)))
          //sostituzione colonna treatment con colonna treatmentPub, al fine della gestione dei trattamenti="N"
          .withColumn(DailyConsumptionInputProcessSchema.treatment, when(col(DailyConsumptionInputProcessSchema.treatment)==="N",lit(col(treatmentPub))).otherwise(lit(col(DailyConsumptionAggSchema.treatment))))
          .selectExpr(DailyConsumptionInputProcessSchema.getValues: _*)


      // Se sono inclusi gli incoerenti GDM, allora aggiorna il coefficiente di correzione da rcugas
      val dailyConsumption = if (aggregators.exists(listOfAggregatorsWithIncoerentiGDM.contains(_)))
        CoefficientController.attachCoefficient(dailyConsumptionJoinDF)
      else dailyConsumptionJoinDF

      if (aggregators.size > 1) dailyConsumption.cache

      // Esecuzione della procedura per ogni pubblicazione
      aggregators.foreach(agg => {
        log.warn(s"Running ${agg.getClass}")
        agg.run(dailyConsumption)
        log.warn(s"Finished ${agg.getClass}")
      })

      Environment.spark.sql(s"MSCK REPAIR TABLE ${Environment.getInfoLogTableName}")

      LogUtility.printFinalLog(applicationName, Constants.LOG)
    } catch {
      case e: Exception => log.error(e.getStackTrace); throw e
      case e: Error => log.error(e.getStackTrace); throw e
    }
  }
}
