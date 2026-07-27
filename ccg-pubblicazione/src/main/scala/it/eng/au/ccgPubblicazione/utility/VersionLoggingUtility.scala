package it.eng.au.ccgPubblicazione.utility

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

object VersionLoggingUtility {

  val conf: Config = ConfigFactory.load("version")

  @transient val logger: Logger = Logger.getLogger(this.getClass)

  def printVersionInfo(): Unit = {
    logger.warn(s"Jar artifactId: ${conf.getString("artifactId")} ")
    logger.warn(s"Jar groupId: ${conf.getString("groupId")} ")
    logger.warn(s"Jar version: ${conf.getString("version")} ")
    logger.warn(s"Last build: ${conf.getString("build.date")} ")
    logger.warn(s"Last commit: id -> ${conf.getString("last.commit.id")}, time -> ${conf.getString("last.commit.time")}")
  }

}
