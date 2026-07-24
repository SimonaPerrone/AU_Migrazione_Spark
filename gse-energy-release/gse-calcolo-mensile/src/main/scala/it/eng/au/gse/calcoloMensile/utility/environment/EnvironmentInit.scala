package it.eng.au.gse.calcoloMensile.utility.environment

import it.eng.au.gse.calcoloMensile.args.Args
import it.eng.au.gse.common.utility.environment.Environment
import org.apache.log4j.Logger

import java.time.YearMonth
import java.time.format.DateTimeFormatter

object EnvironmentInit {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def setEnvironment(appName: String, loggerName: String, args: Args): Unit = {
    Environment.getOrCreate(appName, loggerName, args.propertiesPath)

    val yearMonth = YearMonth.now().minusMonths(2).format(DateTimeFormatter.ofPattern("MM/yyyy"))
    Environment.setProperty("year.month", yearMonth)
  }
}
