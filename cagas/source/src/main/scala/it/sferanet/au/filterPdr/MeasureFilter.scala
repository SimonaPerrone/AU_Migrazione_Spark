package it.sferanet.au.filterPdr

import it.sferanet.au.model.Flow
import it.sferanet.au.schema.PdrAndMeasureToExcludeSchema
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit, when}

import java.io.File
import scala.util.Try

object MeasureFilter {
  private val logger = org.apache.log4j.LogManager.getLogger(this.getClass)

  private lazy val pdrAndMeasureToExcludeDF = Environment.getSqlContext
    .read
    .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
    .format("csv")
    .schema(PdrAndMeasureToExcludeSchema.createSparkSchema())
    .load(Environment.getIgnorePdrMeasureCsvPath)
    //transform blank in null
    .withColumn(PdrAndMeasureToExcludeSchema.pdr, when(col(PdrAndMeasureToExcludeSchema.pdr) =!= lit(""), col(PdrAndMeasureToExcludeSchema.pdr)))
    .withColumn(PdrAndMeasureToExcludeSchema.measure_file, when(col(PdrAndMeasureToExcludeSchema.measure_file) =!= lit(""), col(PdrAndMeasureToExcludeSchema.measure_file)))
    .distinct()
    .cache()

  private lazy val onlyPdrRDD = pdrAndMeasureToExcludeDF
    .where(col(PdrAndMeasureToExcludeSchema.pdr).isNotNull)
    .where(col(PdrAndMeasureToExcludeSchema.measure_file).isNull)
    .select(PdrAndMeasureToExcludeSchema.pdr)
    .distinct()
    .rdd
    .map(_.getAs[String](PdrAndMeasureToExcludeSchema.pdr))

  private lazy val onlyMeasureFileRDD = pdrAndMeasureToExcludeDF
    .where(col(PdrAndMeasureToExcludeSchema.pdr).isNull)
    .where(col(PdrAndMeasureToExcludeSchema.measure_file).isNotNull)
    .select(PdrAndMeasureToExcludeSchema.measure_file)
    .distinct()
    .rdd
    .map(_.getAs[String](PdrAndMeasureToExcludeSchema.measure_file))

  private lazy val pdrMeasureFilesCouplesRDD = pdrAndMeasureToExcludeDF
    .where(col(PdrAndMeasureToExcludeSchema.pdr).isNotNull)
    .where(col(PdrAndMeasureToExcludeSchema.measure_file).isNotNull)
    .select(PdrAndMeasureToExcludeSchema.pdr, PdrAndMeasureToExcludeSchema.measure_file)
    .distinct()
    .rdd
    .map(row => (row.getAs[String](PdrAndMeasureToExcludeSchema.pdr), row.getAs[String](PdrAndMeasureToExcludeSchema.measure_file)))

  /**
   *
   * Filters the input dataframe according to the query: <br><br>
   *
   * <code>
   * Let pdr-file_input = spark.read( Environment.getIgnorePdrMeasureCsvPath )<br><br>
   * SELECT * FROM measures  F <br>
   * LEFT OUTER JOIN pdr-file_input I ON F.p =I.p AND F.f = I.f<br>
   * LEFT OUTER JOIN  (SELECT * FROM pdr-file_input i WHERE i.p IS NOT NULL AND i.f IS NULL  ) I2 ON F.p = I2.p<br>
   * LEFT OUTER JOIN (SELECT * FROM pdr-file_input i WHERE i.f IS NOT NULL AND i.p IS NULL  ) I3 F.f = I3.f<br>
   * WHERE !( (F.p = I.p AND F.f = I.f) OR (F.p = I2.p) OR (F.f = I3.f) )<br>
   * </code>
   *
   * @param measures the measure rdd from which we should remove measures
   * @return measures filtered
   */
  def excludeMeasures(measures: RDD[Flow]): RDD[Flow] = {
    if (!shouldFilter) {
      logger.info(s"Should Filter is false")
      return measures
    }
    val fileSize = Try(new File(Environment.getIgnorePdrMeasureCsvPath).length())
    val fileSizeInByte: Long = if (fileSize.isSuccess && fileSize.get != 0L) fileSize.get else Constants.MAX_BC_SIZE_MB * 1024 * 1024 + 1
    val fileSizeInMByte: Double = fileSizeInByte / (1024 * 1024)
    val bcThresholdProp = Environment.getIgnorePdrMeasuresBroadcastThreshold
    val bcThreshold: Double = if (Try(bcThresholdProp.toLong).isFailure) Constants.MAX_BC_SIZE_MB else bcThresholdProp.toLong

    logger.info(s"Should Filter is true")
    logger.info(s"Estimated size is $fileSizeInByte Byte ($fileSizeInMByte MByte), threshold is $bcThreshold MB")

    if (fileSizeInMByte < bcThreshold) {
      logger.info(s"Using broadcast filter")
      excludeMeasuresBC(measures)
    } else {
      logger.info(s"Using 3-Join filter")
      excludeMeasures3Join(measures)
    }
  }

  private def excludeMeasuresBC(measures: RDD[Flow]): RDD[Flow] = {

    val onlyPdrListBc = Environment.getSparkContext.broadcast(onlyPdrRDD.collect())
    val onlyMeasureFileListBc = Environment.getSparkContext.broadcast(onlyMeasureFileRDD.collect())
    val pdrMeasureFilesCouplesListBc = Environment.getSparkContext.broadcast(pdrMeasureFilesCouplesRDD.collect())

    measures.filter(flow =>
      !(onlyPdrListBc.value.contains(flow.pdr)
        || onlyMeasureFileListBc.value.contains(flow.local_file.getOrElse("None"))
        || pdrMeasureFilesCouplesListBc.value.contains((flow.pdr, flow.local_file.getOrElse("None"))))
    )
  }

  private def excludeMeasures3Join(measures: RDD[Flow]): RDD[Flow] = {
    val firstJoinRDD = pdrMeasureFilesCouplesRDD.map({ case (pdr, localFile) => ((pdr, localFile), ()) })
    val secondJoinRDD = onlyPdrRDD.map(pdr => (pdr, ()))
    val thirdJoinRDD = onlyMeasureFileRDD.map(localFile => (localFile, ()))

    measures
      //key the measure by join key (pdr, local_file)
      .keyBy(f => (f.pdr, f.local_file.getOrElse("")))
      //join with input file (pdr,local_file) couples to exclude
      .leftOuterJoin(firstJoinRDD)
      //Exclude all the rows that got a match in the join
      .filter({ case ((pdr, localFile), (flow, matchFlag)) =>
        matchFlag.isEmpty
      })
      //prepare for the next join where we need pdr as key
      .map({ case ((pdr, localFile), (flow, matchFlag)) => (pdr, flow) })
      .leftOuterJoin(secondJoinRDD)
      //Exclude all the rows that got a match in the join
      .filter({ case (pdr, (flow, matchFlag)) =>
        matchFlag.isEmpty
      })
      //prepare for the last join: exclude by local_file
      .map({ case (pdr, (flow, matchFlag)) => (flow.local_file.getOrElse(""), flow) })
      .leftOuterJoin(thirdJoinRDD)
      .filter({ case (localFile, (flow, matchFlag)) =>
        matchFlag.isEmpty
      })
      .map({ case (localFile, (flow, matchFlag)) => flow })
  }

  private def shouldFilter: Boolean = {
    val prop = Try(Environment.isIgnorePdrMeasuresFilterEnabled)
    if (prop.isSuccess && prop.get.equalsIgnoreCase("true")) true else false
  }
}
