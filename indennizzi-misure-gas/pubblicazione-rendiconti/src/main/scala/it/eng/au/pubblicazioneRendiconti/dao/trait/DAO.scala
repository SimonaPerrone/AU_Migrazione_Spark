package it.eng.au.pubblicazioneRendiconti.dao.`trait`

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import it.eng.au.pubblicazioneRendiconti.Driver.getClass
import it.eng.au.pubblicazioneRendiconti.utility.constants.Constants.CIG_LOG
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

trait DAO {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  val tableName: String
  val columns: List[String]
  val partitionColumn: String

  def readTable: DataFrame = {
    Environment.sqlContext.table(tableName).selectExpr(columns: _*)
  }

  def readTableByPartiton(partition: String): (DataFrame, String) = {
    logger.warn(s"$CIG_LOG Reading partition $partitionColumn=$partition of table $tableName.")
    val df = readTable.where(col(partitionColumn) === partition)
      .selectExpr(columns: _*)

    (df, partition)
  }

  def readLastPartition(): (DataFrame, String) = {
    val tablePartitions = Environment.spark
      .sql(s"SHOW PARTITIONS $tableName")
      .collect
      .toList
      .map(_.getString(0))

    val lastPartition = tablePartitions
      .map(_
        .split("/")
        .filter(_.contains(partitionColumn))
        .head
        .split("=").last
      )
      .sorted
      .reverse
      .head

    readTableByPartiton(lastPartition)
  }
}
