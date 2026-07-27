package it.eng.au.queryReport.utility

import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.log4j.{FileAppender, Level, Logger, PatternLayout}

import java.sql.Timestamp

object Logging {
  def createBaseFileAppender(): FileAppender = {
    val fa = new FileAppender()
    fa.setName("queryLogger")
    fa.setFile("/home/sii_misure_gas/SBG/deploy/deploy_reportistica/log/query-sbg-" + Timestamp.valueOf(Environment.getDateRun).getTime.toString + ".log")
    fa.setLayout(new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n"))
    fa.setThreshold(Level.INFO) //non funziona, si prende il livello che setti nel jog4j.properties
    fa.setAppend(true)
    fa.activateOptions()
    //Logger.getRootLogger.getLoggerRepository.resetConfiguration() //rimuove tutti gli altri Appenders (ad esempio STDOUT)
    Logger.getRootLogger.addAppender(fa)

    fa
  }

  def createCustomFileAppender(queryName: String): Unit = {
    Logger.getRootLogger.removeAppender("queryLogger")
    val fa = new FileAppender()
    fa.setName("queryLogger")
    fa.setFile("/home/eng_test/sbg/query/log/query-" + queryName + "-sbg-" + Timestamp.valueOf(Environment.getDateRun).getTime.toString + ".log")
    fa.setLayout(new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n"))
    fa.setThreshold(Level.INFO) //non funziona, si prende il livello che setti nel jog4j.properties
    fa.setAppend(true)
    fa.activateOptions()
    //Logger.getRootLogger.getLoggerRepository.resetConfiguration() //rimuove tutti gli altri Appenders (ad esempio STDOUT)
    Logger.getRootLogger.addAppender(fa)
  }
}
