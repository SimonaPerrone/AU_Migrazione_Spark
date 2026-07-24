package it.eng.au.partitionOptimization

import it.eng.au.partitionOptimization.FunctionUtility._
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

object Driver {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  val logName = "Partition Optimization:"

  def main(args: Array[String]): Unit = {
    try {
      logger.warn(s"$logName Start Partition Optimization")
      VersionLoggingUtility.printVersionInfo()

      Environment.getOrCreate("Partition Optimization", args(0))

      logger.warn(s"$logName Run Partition Optimization")
      logger.warn(s"$logName Properties:")
      logger.warn(s"$logName ${Environment.printProperties}")
      logger.warn(s"$logName Execution ID: ${Environment.executionId}")
      logger.warn(s"$logName Date: ${Environment.getPartitionDate}")
      logger.warn(s"$logName applicationID=${Environment.getSpark.sparkContext.applicationId}")
      run()
    } catch {
      case e: Throwable =>
        logger.error(s"Error procedure ${args.mkString(" ")}", e)
        throw e
    }
  }


  def run(): Unit = {
    val listTable = Environment.getTables.split(",")
    logger.warn(s"$logName list Initial optimize table ${listTable.mkString(",")}")

    listTable.foreach(table => {
      logger.warn(s"$logName Initial optimize table $table")

      val appender = createLogForTable(table, "optimization")
      logger.addAppender(appender)

      logger.warn(s"$logName Start optimize table $table")

      val listPartitions = getListPartitions(table)
      logger.warn(s"$logName Partition to do ${listPartitions.mkString(",")}")

      val partitionColumn = getPartitionColumns(listPartitions)
      logger.warn(s"$logName Partition Column ${partitionColumn.mkString(",")}")

      val pathTableLocation = getPathInputFile(table)
      logger.warn(s"$logName Path input file $pathTableLocation")

      val pathBackupFile = s"${pathTableLocation}_backup"
      logger.warn(s"$logName Path backup file $pathBackupFile")

      val schema = Environment.getSpark.read.table(table).schema

      logger.warn(s"$logName Start Backup table")
      moveTable(pathTableLocation, pathBackupFile)
      logger.warn(s"$logName End Backup table")

      listPartitions.foreach(partition => {
        logger.warn(s"$logName Start optimization of the partition $partition")
        optimizePartition(partition, pathBackupFile, pathTableLocation, partitionColumn, schema)
        logger.warn(s"$logName End optimization of the partition $partition")
      })

      if (Environment.checkIntegrityTable) {
        logger.warn(s"$logName Start check integrity table $table")
        val dfOld = Environment.getSpark.read.option("mergeSchema", "true").parquet(pathBackupFile).cache
        val dfNew = Environment.getSpark.read.option("mergeSchema", "true").parquet(pathTableLocation).cache
        val equality = checkEquality(dfOld, dfNew)
        logger.warn(s"$logName Table is integrity $equality")
        logger.warn(s"$logName End check integrity table $table")
      }

      logger.warn(s"$logName End optimize table $table")
      logger.removeAppender(appender)
    })
  }

  def optimizePartition(partition: String
                        , pathInputFile: String
                        , pathOutputFile: String
                        , partitionColumn: List[String]
                        , schema: StructType
                       ): Unit = {
    val df = Environment.getSpark.read.option("mergeSchema", "true").parquet(s"$pathInputFile/$partition")
      .cache

    df.foreach(_ => ())
    val catalyst_plan = df.queryExecution.logical
    val df_size_in_bytes = Environment.getSpark.sessionState.executePlan(catalyst_plan).optimizedPlan.stats.sizeInBytes

    val numOptimalPartition = (df_size_in_bytes / Environment.getMaxOptimalSize).toInt + 1
    logger.warn(s"$logName Number optimal partition $numOptimalPartition")

    //    val numOriginalPartitions = Environment.getFs
    //      .listStatus(new Path(s"$pathInputFile/$partition"))
    //      .length
    //    logger.warn(s"$logName Number original partition $numOriginalPartitions")

    val sparkNumPartitions = df.rdd.getNumPartitions
    logger.warn(s"$logName Number spark partition $sparkNumPartitions")

    val doRepartition = numOptimalPartition != sparkNumPartitions

    writeDf(df, numOptimalPartition, partition, schema, pathOutputFile, partitionColumn, doRepartition)

    df.unpersist
  }

  //  def algorithmDoRepartition(numOptimalPartition: Int, sparkNumPartitions: Int): Boolean = {
  //    if (sparkNumPartitions <= 50 || numOptimalPartition <= 50) {
  //      val diff = Math.abs(numOptimalPartition - sparkNumPartitions)
  //      diff > 3
  //    } else {
  //      val diffPercent = Math.abs((numOptimalPartition - sparkNumPartitions).toDouble / sparkNumPartitions.toDouble * 100)
  //      diffPercent > 20
  //    }
  //  }

  def checkEquality(df1: DataFrame, df2: DataFrame): Boolean = {
    val df1Count = df1.count()
    logger.warn(s"$logName Number record old table $df1Count")
    val df2Count = df2.count()
    logger.warn(s"$logName Number record new table $df2Count")

    val dfUnionCount = df1.exceptAll(df2).count()
    logger.warn(s"$logName Number record mismatch $dfUnionCount")

    (df1Count == df2Count) && (dfUnionCount == 0)
  }
}
