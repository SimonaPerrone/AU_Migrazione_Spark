package it.eng.au.ERP.dao.hive

import it.eng.au.ERP.schema.SchemaEnum
import org.apache.log4j.Logger
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

// able to read more than 200 column
abstract class HDao
{
  val tableName: String
  val schema: SchemaEnum
  def columns: List[String] = schema.getValues

  /** Whether writes are permitted. */
  val writeEnabled: Boolean = true

  @transient private lazy val logger: Logger = Logger.getLogger(getClass.getName)

  private def isExecutionIdPartitioned: Boolean = {
    val cols = columns.map(_.toLowerCase)
    cols.contains("executionid") && cols.contains("anno") && cols.contains("mese")
  }

  private def isEmpty(df: DataFrame): Boolean = df.limit(1).isEmpty

  def read()(implicit spark: SparkSession): DataFrame = {
    spark.sqlContext.read
      .table(tableName)
      .selectExpr(columns: _*)
  }

  /**
    * Reads only data for the specified year-month. Leverages partition pruning when available.
    */
  def readForMonth(anno: Int, mese: Int)(implicit spark: SparkSession): DataFrame = {
    val yearStr = anno.toString
    val monthStr = f"$mese%02d"
    spark.table(tableName)
      .where(col("anno") === lit(yearStr))
      .where(col("mese") === lit(monthStr))
      .selectExpr(columns: _*)
  }

  /** Read for specific year-month and executionid. */
  def readForMonthAndExecutionId(anno: Int, mese: Int, executionId: Long)(implicit spark: SparkSession): DataFrame = {
    val yearStr = anno.toString
    val monthStr = f"$mese%02d"
    spark.table(tableName)
      .where(col("anno") === lit(yearStr))
      .where(col("mese") === lit(monthStr))
      .where(col("executionid").cast("long") === lit(executionId))
      .selectExpr(columns: _*)
  }

  /**
    * Finds the latest executionid for a given year-month using partition metadata when possible.
    * Falls back to data scan (restricted to the month) if the table is not partitioned.
    */
  def latestExecutionIdForMonth(anno: Int, mese: Int)(implicit spark: SparkSession): Option[Long] = {
    import spark.implicits._

    if (isExecutionIdPartitioned) {
      // Try using partition metadata for efficiency
      val tryParts = try {
        Some(spark.sql(s"SHOW PARTITIONS ${tableName}").as[String].collect().toSeq)
      } catch {
        case _: Throwable => None
      }

      tryParts.flatMap { parts =>
        val yearStr = anno.toString
        val monthStr = f"$mese%02d"
        def toLongOpt(s: String): Option[Long] = try Some(s.toLong) catch { case _: Throwable => None }
        val candidates = parts.flatMap { p =>
          val pairs = p.split('/').flatMap { kv =>
            kv.split('=') match {
              case Array(k, v) => Some(k -> v)
              case _ => None
            }
          }.toMap
          val a = pairs.get("anno")
          val m = pairs.get("mese")
          val e = pairs.get("executionid")
          (a, m, e) match {
            case (Some(ay), Some(mm), Some(ex)) if ay == yearStr && mm == monthStr => toLongOpt(ex)
            case _ => None
          }
        }
        if (candidates.nonEmpty) Some(candidates.max) else None
      }.orElse {
        // Fallback: scan month and compute max(executionid)
        val monthDf = readForMonth(anno, mese)
        if (monthDf.limit(1).isEmpty) None
        else Some(monthDf.agg(max(col("executionid")).cast("long")).first().getLong(0))
      }
    } else {
      // Not partitioned: compute from data (restricted to month)
      val monthDf = readForMonth(anno, mese)
      if (monthDf.limit(1).isEmpty) None
      else Some(monthDf.agg(max(col("executionid")).cast("long")).first().getLong(0))
    }
  }

  /**
    * Read only the latest partition (by executionid) for the given year-month.
    * Uses SHOW PARTITIONS when possible to avoid full scans.
    */
  def readLatestForMonth(anno: Int, mese: Int)(implicit spark: SparkSession): DataFrame = {
    latestExecutionIdForMonth(anno, mese) match {
      case Some(exec) => readForMonthAndExecutionId(anno, mese, exec)
      case None => spark.table(tableName).limit(0).selectExpr(columns: _*)
    }
  }

  /**
    * Write to Hive aligning with functional spec (Enermia):
    * - For ERP tables partitioned by (anno, mese, executionid) we APPEND runs (executionid chronology kept).
    * - Skip writes when the provided DataFrame is empty to avoid accidental data loss.
    * - For all other tables, honor the requested overwrite/append mode.
    */
  def write(data: DataFrame, overwrite: Boolean = false)(implicit spark: SparkSession): Unit = {
    if (!writeEnabled) {
      throw new UnsupportedOperationException(s"Writing to table '$tableName' is not allowed.")
    }

    val orderedData =
      try {
        data.selectExpr(columns: _*)
      } catch {
        case _: AnalysisException =>
          throw new IllegalArgumentException(
            s"Unable to align columns for table '$tableName'. Expected columns: ${columns.mkString(", ")}"
          )
      }

    if (isExecutionIdPartitioned) {
      // ERP semantics: keep history by executionid and select MAX(executionid) at read time.
      logger.info(s"Writing to '$tableName' in APPEND mode (executionid-partitioned semantics).")
      orderedData.write.mode("append").insertInto(tableName)
    } else {
      if (overwrite) {
        if (isEmpty(orderedData)) {
          logger.warn(s"Skip write to '$tableName': dataset is empty (mode requested: overwrite).")
          return
        }
        logger.info(s"Writing to '$tableName' in OVERWRITE mode.")
        orderedData.write.mode("overwrite").insertInto(tableName)
      } else {
        logger.info(s"Writing to '$tableName' in APPEND mode.")
        orderedData.write.mode("append").insertInto(tableName)
      }
    }
  }

  def registerTempTable(data: DataFrame, viewName: String): DataFrame = {
    data.createOrReplaceTempView(viewName)
    data
  }
}
