package it.eng.au.ERP.flow

import it.eng.au.ERP.utility.args.ERPArgsConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

abstract class Flow(implicit spark: SparkSession) {
  @transient  lazy val logger = Logger.getLogger(getClass.getName)

  def run(timestamp: Long,podExluded:List[String],args: ERPArgsConfig) : Unit = {}

}
