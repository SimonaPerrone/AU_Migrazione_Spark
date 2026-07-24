package it.au.misure.ingestionMisureGasUnico.utility

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

object VersionLoggingUtility {

  val conf: Config = ConfigFactory.load("version")

  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def printVersionInfo():Unit = {
    logger.info(s"Jar artifactId: ${conf.getString("artifactId")} ")
    logger.info(s"Jar groupId: ${conf.getString("groupId")} ")
    logger.info(s"Jar version: ${conf.getString("version")} ")
    logger.info(s"Last build: ${conf.getString("build.date")} ")
    logger.info(s"Last commit: id -> ${conf.getString("last.commit.id")}, time -> ${conf.getString("last.commit.time")}")
  }

}
