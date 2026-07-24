package it.eng.au.gse.calcoloAnnuale.utility.environment

import it.eng.au.gse.calcoloAnnuale.args.Args
import it.eng.au.gse.common.utility.environment.Environment
import org.apache.log4j.Logger

import java.time.{Year, YearMonth}
import java.time.format.DateTimeFormatter

object EnvironmentInit {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def setEnvironment(appName: String, loggerName: String, args: Args): Unit = {
    Environment.getOrCreate(appName, loggerName, args.propertiesPath)

    val currentYear = Year.now()

    Environment.setProperty("year", currentYear.format(DateTimeFormatter.ofPattern("yyyy")))
  }
}
