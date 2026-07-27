package it.eng.au.portale_consumi_ee.common.calcolo_forniture.flow

import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

class prova2 {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  val spark: SparkSession = Environment.getSpark
}
