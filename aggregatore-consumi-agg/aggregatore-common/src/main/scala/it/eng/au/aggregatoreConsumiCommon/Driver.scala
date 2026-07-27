package it.eng.au.aggregatoreConsumiCommon

import it.eng.au.aggregatoreConsumiCommon.dao.agg.DailyConsumptionDao
import it.eng.au.aggregatoreConsumiCommon.factory.AggregatorFactory
import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.{Environment, LogUtility}
import org.apache.log4j.Logger

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime

@deprecated("This object is deprecated, since every module has its own driver")
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

      LogUtility.printInitialLog(applicationName, "LOG")

      val dailyConsumptionDao = new DailyConsumptionDao()
      val dailyConsumptionDF = dailyConsumptionDao
        .prepare(Environment.getDailyConsumptionExecutionid)
        .cache

      val aggregators = AggregatorFactory.getAggregators

      aggregators.foreach(agg => {
        log.info(s"Running ${agg.getClass}")
        agg.run(dailyConsumptionDF)
        log.info(s"Finished ${agg.getClass}")
      })

      Environment.spark.sql(s"MSCK REPAIR TABLE ${Environment.getInfoLogTableName}")

      LogUtility.printFinalLog(applicationName, "LOG")
    } catch {
      case e: Exception => log.error(e.getStackTrace); throw e
      case e: Error => log.error(e.getStackTrace); throw e
    }
  }
}