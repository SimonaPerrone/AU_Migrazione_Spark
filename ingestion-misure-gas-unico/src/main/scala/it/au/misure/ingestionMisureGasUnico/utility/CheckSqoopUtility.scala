package it.au.misure.ingestionMisureGasUnico.utility

import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, from_unixtime}

import java.time.LocalDate

object CheckSqoopUtility {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def checkSqoopDateIsToday(table: String): Boolean = {
    val lastModifiedTime = Environment.getSpark.sql(s"show TBLPROPERTIES $table ('transient_lastDdlTime')").toDF("last_modified_time")
      .withColumn("last_modified_time", from_unixtime(col("last_modified_time"), "yyyy-MM-dd"))
      .collect()(0)(0)
      .toString

    val datetime = LocalDate.parse(lastModifiedTime)

    val condition = datetime.isEqual(LocalDate.now())
    logger.warn(s"check sqoop $table is $condition, last update is ${datetime.toString}")

    condition
  }
}
