package it.eng.au.portale_consumi_ee.common.utility.functions

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import java.time.YearMonth
import java.time.format.DateTimeFormatter

import java.util.{Calendar, TimeZone}

object argumentsUtilities {

  def annomeseDefiniton (windowsTime:String,timeZone:String) : Int = {
    var mesi = 3
    try{
      mesi = windowsTime.toInt
    }catch {
      case e: Exception => { mesi = 3}
    }
    var delay = mesi+1
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    cal.add(Calendar.MONTH, -delay)
    val anno: String = Integer.toString(cal.get(Calendar.YEAR))
    val mese: String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
    val annomese = anno.toString + (("0" + mese.toString) takeRight 2)

    annomese.toInt
  }

  def annomesegiornoDefiniton (windowsTime:String,timeZone:String) : Int = {
    var mesi = 3
    try{
      mesi = windowsTime.toInt
    }catch {
      case e: Exception => { mesi = 3}
    }
    var delay = mesi+1
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    cal.add(Calendar.MONTH, -delay)
    val anno: String = Integer.toString(cal.get(Calendar.YEAR))
    val mese: String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
    val giorno: String = "01"
    val annomesegiorno = anno.toString + (("0" + mese.toString) takeRight 2) + giorno

    annomesegiorno.toInt
  }


  def annomese36MonthsAgo(): Int = {
    val now = java.time.YearMonth.now()
    val newDate = now.minusMonths(36)
    newDate.getYear * 100 + newDate.getMonthValue
  }

  def annomeseToRemoveDefiniton (windowsTime:String,timeZone:String) : Int = {
    var mesi = 3
    try{
      mesi = windowsTime.toInt
    }catch {
      case e: Exception => { mesi = 3}
    }
    var delay = mesi
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    cal.add(Calendar.MONTH, -delay)
    val anno: String = Integer.toString(cal.get(Calendar.YEAR))
    val mese: String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
    val annomese = anno.toString + (("0" + mese.toString) takeRight 2)

    annomese.toInt
  }

  def numberMonthAgoToDefine(windowTimeValue: String): Int = {
    var monthAgoLimit = 3
    try{
      monthAgoLimit = windowTimeValue.toInt
    }catch {
      case e: Exception => { monthAgoLimit = 3}
    }

    monthAgoLimit + 1
  }

  def removepartition(month37th: Integer,tableName:String ,spark: SparkSession,logger: Logger) : Unit = {

    // Fetch all matching partitions dynamically
    val partitions = spark.sql(s"SHOW PARTITIONS $tableName")
      .collect()
      .map(_.getString(0))
      .filter(_.contains(s"annomese_riferimento=$month37th"))

    if (partitions.isEmpty) {
      logger.info(s"No partitions found for annomese_riferimento=$month37th")
    } else {
      partitions.foreach { partition =>
        // Convert partition format for DROP statement
        val partitionSpec = partition.split("/")
          .map {
            part =>
          val Array(key, value) = part.split("=")
          s"$key='$value'"
        }.mkString(", ")

        val dropPartitionQuery = s"ALTER TABLE $tableName DROP IF EXISTS PARTITION ($partitionSpec)"
        logger.info(s"Executing: $dropPartitionQuery")
        spark.sql(dropPartitionQuery)
      }

    }
  }


  def dropPartitionsBeforeOrEqualAnnomeseRiferimento(
                                                      tableName: String,
                                                      columnName: String,
                                                      cutoffAnnomese: Int,
                                                      spark: SparkSession,
                                                      logger: Logger
                                                    ): Unit = {
    import spark.implicits._

    // Step 1: Fetch all partitions of the table
    val partitions = spark.sql(s"SHOW PARTITIONS $tableName")
      .map(_.getString(0))  // e.g., "annomese_riferimento=202401/cod_pod=AB/is_mis_oraria=Y"
      .collect()

    // Step 2: Filter partitions with annomese_riferimento <= cutoff
    val partitionsToDrop = partitions.filter { part =>
      val partsMap = part.split("/").map { kv =>
        val Array(k, v) = kv.split("=")
        (k, v)
      }.toMap

      partsMap(columnName).toInt <= cutoffAnnomese
    }

    // Step 3: Drop the matching partitions
    partitionsToDrop.foreach { part =>
      val partitionClause = part
        .split("/")
        .map(_.replace("=", "='") + "'")
        .mkString(", ")

      val dropStmt = s"ALTER TABLE $tableName DROP IF EXISTS PARTITION ($partitionClause)"
      logger.info(s"Dropping partition: $dropStmt") // Optional logging
      spark.sql(dropStmt)
    }

    logger.info(s"Dropped ${partitionsToDrop.length} partition(s) from $tableName.")
  }



  def dropPartitionsAfterAnnomeseRiferimento(
                                                      tableName: String,
                                                      nameColumn:String,
                                                      cutoffAnnomese: Int,
                                                      spark: SparkSession,
                                                      logger: Logger
                                                    ): Unit = {
    import spark.implicits._

    // Step 1: Fetch all partitions of the table
    val partitions = spark.sql(s"SHOW PARTITIONS $tableName")
      .map(_.getString(0))  // e.g., "annomese_riferimento=202401/cod_pod=AB/is_mis_oraria=Y"
      .collect()

    // Step 2: Filter partitions with annomese_riferimento <= cutoff
    val partitionsToDrop = partitions.filter { part =>
      val partsMap = part.split("/").map { kv =>
        val Array(k, v) = kv.split("=")
        (k, v)
      }.toMap

      partsMap(nameColumn).toInt > cutoffAnnomese
    }

    // Step 3: Drop the matching partitions
    partitionsToDrop.foreach { part =>
      val partitionClause = part
        .split("/")
        .map(_.replace("=", "='") + "'")
        .mkString(", ")

      val dropStmt = s"ALTER TABLE $tableName DROP IF EXISTS PARTITION ($partitionClause)"
      logger.info(s"Dropping partition: $dropStmt") // Optional logging
      spark.sql(dropStmt)
    }

    logger.info(s"Dropped ${partitionsToDrop.length} partition(s) from $tableName.")
  }

  def getYearMonthRange(from: Int): List[Int] = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMM")
    val start = YearMonth.parse(from.toString, formatter)
    val end = YearMonth.now()

    Iterator.iterate(end)(_.minusMonths(1))
      .takeWhile(!_.isBefore(start))
      .map(ym => ym.format(formatter).toInt)
      .toList
  }

  def dropPartitionsByValues(
                              tableName: String,
                              nameColumn: String,                  // e.g. "competenza_consumi"
                              valuesToDrop: List[Int],            // e.g. List(202401, 202402)
                              idRun: String,                      // Single id_run value, e.g. "run_001"
                              spark: SparkSession,
                              logger: Logger
                            ): Unit = {
    import spark.implicits._

    // Get list of all partitions
    val partitions = spark.sql(s"SHOW PARTITIONS $tableName")
      .map(_.getString(0)) // e.g. "competenza_consumi=202401/id_run=run_001"
      .collect()

    val valueSet = valuesToDrop.toSet

    // Filter for partitions that match the nameColumn and id_run value
    val partitionsToDrop = partitions.filter { part =>
      val partsMap = part.split("/").map { kv =>
        val Array(k, v) = kv.split("=")
        (k, v)
      }.toMap

      partsMap.get(nameColumn).exists(v => valueSet.contains(v.toInt)) &&
        partsMap.get("id_run").contains(idRun)
    }

    // Drop matching partitions
    partitionsToDrop.foreach { part =>
      val partitionClause = part
        .split("/")
        .map { kv =>
          val Array(k, v) = kv.split("=")
          s"$k='$v'"
        }.mkString(", ")

      val dropStmt = s"ALTER TABLE $tableName DROP IF EXISTS PARTITION ($partitionClause)"
      logger.info(s"Dropping partition: $dropStmt")
      spark.sql(dropStmt)
    }

    logger.info(s"Dropped ${partitionsToDrop.length} partition(s) from $tableName.")
  }


}
