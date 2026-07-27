package it.eng.au.portale_consumi_ee.common

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

trait Driver {

  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  // Entry point for running the job
   def run(args: Array[String]): Unit

  protected def main(args: Array[String]): Unit = {
    logger.info("Start Job")
    run(args)
    logger.info("End Job")
      }
  }
