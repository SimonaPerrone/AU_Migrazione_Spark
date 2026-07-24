package it.eng.au.pubblicazionePcg

import it.eng.au.pubblicazionePcg.args.ParseCLIArgs
import it.eng.au.pubblicazionePcg.controller.PubblicazionePCG
import it.eng.au.pubblicazionePcg.dao.sbg.DailyConsumptionDAO
import it.eng.au.pubblicazionePcg.utility.Constants.TIMESTAMP_FORMAT
import it.eng.au.pubblicazionePcg.utility.DateTimeUtility.convertLocalDateTimeToStringWithFormat
import it.eng.au.pubblicazionePcg.utility.SparkImplicit
import org.apache.log4j.Logger

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, YearMonth}

object Driver extends SparkImplicit {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val cliArgs = ParseCLIArgs.parse(args)

      initialize("[CLG] Pubblicazione PCG", cliArgs)

      val currentTimestamp = LocalDateTime.now()
      val dateRun = convertLocalDateTimeToStringWithFormat(currentTimestamp, TIMESTAMP_FORMAT)
      jobProperties.setProperty("date.run", dateRun)

      val monthDifference = jobProperties.getProperty("offset.mese").toInt
      val timestampFormatter = DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT)
      val yearMonthFormatter = DateTimeFormatter.ofPattern("yyyyMM")
      jobProperties.setProperty("year.month", YearMonth.parse(dateRun, timestampFormatter).minusMonths(monthDifference).format(yearMonthFormatter))

      logger.info(s"Reading dailyConsumption and creating CSVs...")
      val dailyConsumptionDAO = new DailyConsumptionDAO()
      val sbgMisureDF = dailyConsumptionDAO.readAnnoMesePartition
      PubblicazionePCG.run(sbgMisureDF)
      logger.info(s"Process finished.")

    } catch {
      case e: Exception => logger.error(e.getStackTraceString); throw e
      case e: Error => logger.error(e.getStackTraceString); throw e
    }
  }
}
