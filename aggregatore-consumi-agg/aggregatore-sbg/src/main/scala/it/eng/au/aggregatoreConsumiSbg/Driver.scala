package it.eng.au.aggregatoreConsumiSbg

import it.eng.au.aggregatoreConsumiCommon.controller.CoefficientController
import it.eng.au.aggregatoreConsumiCommon.dao.agg.{DailyConsumptionDao, DailyConsumptionEsclusiDao, DailyConsumptionIncoerentiDao}
import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionInputProcessSchema
import it.eng.au.aggregatoreConsumiCommon.utility.args.FlowArgsFactory
import it.eng.au.aggregatoreConsumiCommon.utility.{Environment, LogUtility}
import it.eng.au.aggregatoreConsumiSbg.factory.AggregatorFactory
import it.eng.au.aggregatoreConsumiSbg.factory.AggregatorFactory.listOfAggregatorsWithIncoerentiGDM
import it.eng.au.aggregatoreConsumiSbg.utility.Constants
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, lit, when}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime

object Driver {
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val applicationName = "[SBG] Aggregatore Consumi Sessione Bilanciamento Gas"

      val parseArgs = FlowArgsFactory.parse(args)

      // init spark context and load job settings
      Environment.getOrCreate(applicationName, parseArgs.pathToProperties)
      log.warn(s"type to run ${parseArgs.outputFileCouples.getOrElse(Environment.getOutputFileCouples)}")

      val now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Timestamp.valueOf(LocalDateTime.now()))
      Environment.setProperty("daterun", now)
      Environment.setProperty("date.run", now)
      Environment.setProperty("publication.type", "SBG")

      LogUtility.printInitialLog(applicationName, Constants.LOG)

      val aggregators = AggregatorFactory.getAggregators(parseArgs.outputFileCouples)

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

      val dailyConsumption = if (aggregators.exists(listOfAggregatorsWithIncoerentiGDM.contains(_)))
        CoefficientController.attachCoefficient(dailyConsumptionJoinDF)
      else dailyConsumptionJoinDF

      if (aggregators.size > 1) dailyConsumption.cache

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