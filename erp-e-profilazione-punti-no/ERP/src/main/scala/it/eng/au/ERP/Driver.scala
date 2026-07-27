package it.eng.au.ERP

import org.apache.log4j.Logger

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
