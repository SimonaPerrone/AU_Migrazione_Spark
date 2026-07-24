package it.eng.au.pubblicazionePcg.dao.sbg

import it.eng.au.pubblicazionePcg.dao.DAO
import it.eng.au.pubblicazionePcg.schema.DailyConsumptionSchema
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}

import java.util.Properties

class DailyConsumptionDAO(implicit prop: Properties) extends DAO {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  override val tableName: String = prop.getProperty("dailyConsumptionSbg.tableName")
  override val columns: List[String] = DailyConsumptionSchema.getValues
  override val partitionColumn: String = DailyConsumptionSchema.annomese
  override val partitionValue: String = prop.getProperty("year.month")
  override val hdfsOutput: String = ""

  def readAnnoMesePartition(implicit sqlContext: SQLContext): DataFrame = {
    logger.warn(s"Reading partition $partitionColumn=$partitionValue of $tableName...")

    val listPartitions = sqlContext
      .sql(s"SHOW PARTITIONS $tableName")
      .collect
      .toList
      .map(_.getString(0).toLowerCase)

    val executionId = listPartitions
      .filter(_.contains(s"$partitionColumn=$partitionValue"))
      .map(
        _.split("/")
          .filter(_.contains(DailyConsumptionSchema.executionid))
          .head
          .split("=")
          .last
      )
      .sorted
      .reverse
      .head

    logger.warn(s"Reading executionid $executionId and annomese $partitionValue...")

    sqlContext.read.table(tableName)
      .where(col(partitionColumn) === partitionValue)
      .where(col(DailyConsumptionSchema.executionid) === executionId)
      .selectExpr(columns: _*)
  }
}
