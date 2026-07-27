package it.eng.au.portale_consumi_ee.common.flow

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

abstract class FlowTupleDsOutput[T, U](implicit spark: SparkSession) {
  @transient  lazy val logger = Logger.getLogger(getClass.getName)

  def run() :  (Dataset[T], Dataset[U])

}
