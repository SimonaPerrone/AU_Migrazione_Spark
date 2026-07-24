package it.eng.au.partitionRollback

import it.eng.au.partitionOptimization.FunctionUtility.{createLogForTable, getPathInputFile, moveTable}
import it.eng.au.partitionOptimization._
import org.apache.log4j.Logger

object Driver {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  val logName = "Partition Optimization:"

  def main(args: Array[String]): Unit = {
    try {
      logger.warn(s"$logName Start Partition Rollback")
      VersionLoggingUtility.printVersionInfo()

      Environment.getOrCreate("Partition Rollback", args(0))

      logger.warn(s"$logName Run Partition Rollback")
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
    logger.warn(s"$logName list Initial delete backup table ${listTable.mkString(",")}")

    listTable.foreach(table => {
      logger.warn(s"$logName Initial Rollback table $table")

      val appender = createLogForTable(table, "rollback")
      logger.addAppender(appender)

      logger.warn(s"$logName Start Rollback table $table")

      val pathTableLocation = getPathInputFile(table)
      logger.warn(s"$logName Path input file $pathTableLocation")

      val pathBackupTable = s"${pathTableLocation}_backup"
      logger.warn(s"$logName Path backup $pathBackupTable")

      val isSuccess = moveTable(pathBackupTable, pathTableLocation)

      logger.warn(s"$logName Rollback is success: $isSuccess")

      logger.warn(s"$logName End Rollback table $table")
      logger.removeAppender(appender)
    })
  }
}
