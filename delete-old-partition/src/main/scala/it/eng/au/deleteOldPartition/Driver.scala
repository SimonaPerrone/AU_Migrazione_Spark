package it.eng.au.deleteOldPartition

import it.eng.au.deleteOldPartition.utility.constants.FieldConstants.{CCG, LOG}
import it.eng.au.deleteOldPartition.utility.environment.Environment
import it.eng.au.deleteOldPartition.utility.version.VersionLoggingUtility
import org.apache.hadoop.fs.Path
import org.apache.log4j.{FileAppender, Logger, SimpleLayout}
import org.apache.spark.sql.functions.col

object Driver {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      logger.warn(s"$LOG Start Delete Old Partition")
      VersionLoggingUtility.printVersionInfo()

      Environment.getOrCreate("Delete Old Partition", args(0))

      logger.warn(s"$LOG Run Delete Old Partition")
      logger.warn(s"$LOG Properties:")
      logger.warn(s"$LOG ${Environment.printProperties}")
      logger.warn(s"$LOG Execution ID: ${Environment.executionId}")
      logger.warn(s"$LOG Date: ${Environment.getPartitionDate}")
      logger.warn(s"$LOG applicationID=${Environment.getSpark.sparkContext.applicationId}")
      run()
    } catch {
      case e: Throwable =>
        logger.error(s"Error procedure ${args.mkString(" ")}", e)
        throw e
    }
  }

  def run(): Unit = {
    val spark = Environment.getSpark

    val listTable = Environment.getTables.split(",")
    logger.warn(s"$LOG list Initial delete old partition ${listTable.mkString(",")}")

    listTable.foreach(table => {
      logger.warn(s"$LOG Initial delete old partition $table")

      val listPartitions = getListPartitions(table)
      logger.warn(s"$LOG Partitions exist ${listPartitions.mkString(",")}")

      val pathTableLocation = getPathInputFile(table)
      logger.warn(s"$LOG Path input file $pathTableLocation")

      val partitionToDelete = getPartitionToDelete(listPartitions)
      logger.warn(s"$LOG Partition to delete: ${partitionToDelete.mkString(",")}")

      val fs = Environment.getFs

      partitionToDelete.foreach(partition => {
        val sql = s"ALTER TABLE $table DROP IF EXISTS PARTITION (${partition.replace("/", ",").replace(CCG, s"\'$CCG\'")})"
        logger.warn(s"$LOG Spark Sql Query: $sql")

        spark.sql(sql)

        val path = s"$pathTableLocation/$partition"
        logger.warn(s"$LOG Path to delete $path")

        val output = new Path(path)

        val deleteIsSuccess = if (fs.exists(output)) {
          fs.delete(output, true)
        } else {
          logger.warn(s"$LOG Delete partition ${output.getName} isn't success")
          false
        }

        logger.warn(s"$LOG Delete is success: $deleteIsSuccess")
      })

      logger.warn(s"$LOG End delete old partition of the table ${table}")
    })

    logger.warn(s"$LOG End delete old partition")
  }

  def createLogForTable(table: String, process: String): FileAppender = {
    val nameTable = table.split("\\.").mkString("-")
    val layout = new SimpleLayout()
    val appender = new FileAppender(layout, s"${Environment.getPathToLog}/${nameTable}_${process}_${Environment.getPartitionDate.toString("yyyy-MM-dd_HH:mm:ss")}.log", true)
    appender
  }

  /**
   * @param table
   * @return list of partition
   *
   *         Es:
   *         show partitions output: List([sessione=CCG/executionid=00000000000001], [sessione=CCG/executionid=1616663722376])
   */
  def getListPartitions(table: String): List[String] = {
    Environment.getSpark
      .sql(s"SHOW PARTITIONS $table")
      .collect
      .toList
      .map(_.getString(0))

  }

  /**
   *
   * @param listPartitions
   * @return list columns
   *         es input: List([sessione=CCG/executionid=00000000000001], [sessione=CCG/executionid=1616663722376])
   *         output: List([sessione, executionid])
   */
  def getPartitionToDelete(listPartitions: List[String]): List[String] = {
    val filterCCGPartitions = listPartitions
      .filter(_.toUpperCase.contains(CCG))

    val numPartition = filterCCGPartitions.size
    val numPartitionToRemain = Environment.getNumPartitionToRemain.toInt
    val numPartitionToDelete = numPartition - numPartitionToRemain

    logger.warn(s"$LOG Number partition to delete: $numPartitionToDelete")

    if (numPartitionToDelete > 0)
      filterCCGPartitions
        .map(partition => (partition, partition.split("/").last.split("=").last))
        .sortBy(_._2)
        .map(_._1)
        .take(numPartitionToDelete)
    else
      List()
  }


  def getPathInputFile(table: String): String = {
    Environment.getSpark.sql(s"desc formatted $table").toDF
      .filter(col("col_name") === "Location")
      .collect()(0)(1)
      .toString
  }

}