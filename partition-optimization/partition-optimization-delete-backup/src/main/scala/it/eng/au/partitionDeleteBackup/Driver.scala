package it.eng.au.partitionDeleteBackup

import it.eng.au.partitionOptimization.FunctionUtility.{createLogForTable, getPathInputFile}
import it.eng.au.partitionOptimization._
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger

object Driver {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  val logName = "Partition Optimization:"

  def main(args: Array[String]): Unit = {
    try {
      logger.warn(s"$logName Start Partition Delete Backup")
      VersionLoggingUtility.printVersionInfo()

      Environment.getOrCreate("Partition Optimization Backup", args(0))

      logger.warn(s"$logName Run Partition Optimization Backup")
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
      logger.warn(s"$logName Initial delete backup table $table")

      val appender = createLogForTable(table, "delete_backup")
      logger.addAppender(appender)

      logger.warn(s"$logName Start delete backup table $table")

      val pathTableLocation = getPathInputFile(table)
      logger.warn(s"$logName Path input file $pathTableLocation")

      val pathBackupTable = s"${pathTableLocation}_backup"
      logger.warn(s"$logName Path backup $pathBackupTable")

      val output = new Path(pathBackupTable)

      val fs = Environment.getFs

      val deleteIsSuccess = if (fs.exists(output)) {
        fs.delete(output, true)
      } else {
        logger.warn(s"$logName Backup don't find")
        false
      }

      logger.warn(s"$logName Delete is success: $deleteIsSuccess")

      logger.warn(s"$logName End delete backup table $table")
      logger.removeAppender(appender)
    })
  }

}
