package it.eng.au.aggiustamentoGas.filter.exclusion

import it.eng.au.aggiustamentoGas.model.agg.{DailyConsumption, ExternalDailyInfo}
import it.eng.au.aggiustamentoGas.schema.agg.ForceExclusionFilterSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit, not, trim}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.joda.time.format.DateTimeFormat

import java.io.File
import scala.util.Try

/**
 *
 * Utilizzato per contrassegnare un dato insieme di misure come "escluse forzate". In questo modo, durante la fase di aggregazione
 * (vedere progetto aggregatore-consumi-agg), verranno inserite nell'insieme delle misure escluse (chiamato ora "incoerenti exc").
 * @param isStrong true se si tratta del processo di esclusione definitiva. False nel caso di esclusione standard
 */
class ForceExclusionController(isStrong: Boolean) {
  private val logger = org.apache.log4j.LogManager.getLogger(this.getClass)

  lazy val exclusionFileDf: DataFrame = Environment.getSpark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .schema(ForceExclusionFilterSchema.createSparkSchema())
    .load(csvPathToLoad)
    .select(
      trim(col(ForceExclusionFilterSchema.pdr)).as(ForceExclusionFilterSchema.pdr),
//      trim(col(ForceExclusionFilterSchema.file)).as(ForceExclusionFilterSchema.file),
      trim(col(ForceExclusionFilterSchema.cod_remi)).as(ForceExclusionFilterSchema.cod_remi),
      trim(col(ForceExclusionFilterSchema.anno_mese)).as(ForceExclusionFilterSchema.anno_mese)
    )
    .distinct
    .cache

  def isEnabled: Boolean = if (isStrong) Environment.isStrongForceExclusionFilterEnabled.equalsIgnoreCase("true")
  else Environment.isForceExclusionFilterEnabled.equalsIgnoreCase("true")

  private val csvPath = if (isStrong) Environment.getStrongForceExclusionFilterFolder
  else Environment.getForceExclusionFilterPath

  private val csvPathToLoad = if (isStrong) csvPath + "/*.csv"
  else csvPath

  private val exclusionFileSize = if (isStrong) Try(Environment.getFs.getContentSummary(new Path(csvPath)).getLength)
  else Try(new File(csvPath).length())

  private val fileSizeInByte: Long = if (exclusionFileSize.isSuccess && exclusionFileSize.get != 0L) exclusionFileSize.get else ForceExclusionController.MAX_BC_SIZE_MB * 1024 * 1024 + 1
  private val fileSizeInMByte: Double = fileSizeInByte / (1024 * 1024)
  private val bcThresholdProp = Environment.getBroadcastThreshold
  private val bcThreshold: Double = if (Try(bcThresholdProp.toLong).isFailure) ForceExclusionController.MAX_BC_SIZE_MB else bcThresholdProp.toLong

  def canBroadcast: Boolean = fileSizeInMByte <= bcThreshold

  def backupExclusionFolder(): Unit = {
    if (isStrong && isEnabled) {
      // we backup the entries in the CSV files into a table
      val backupTable = Environment.getStrongExclusionFilterBackupTable

      exclusionFileDf
        .withColumn("exclusion_type", lit("esclusione forzata"))
        .withColumn("process_type", lit(Environment.getSession))
        .withColumn("execution_id", lit(Environment.executionId).cast(LongType))
        .write
        .mode(SaveMode.Append)
        .partitionBy("execution_id")
        .parquet(backupTable)
    }
  }

  /**
   * Method exposing the possibility to mark all the daily consumptions with a given pdr or a given codRemi to be marked
   * as excluded. This have to consequence to put the filed forceExclusion on the table daily_consumptions_agg true for
   * those record (it is then used from others spark procedures).
   *
   * @param dailyCons output of JoinAnagraficaConsumptionController.getJoinedAnagrafica
   * @return dailyCons with forced field to exclude
   */
  def forceExclusion(dailyCons: RDD[(DailyConsumption, ExternalDailyInfo)], rcuGasSqoopDate: String): RDD[(DailyConsumption, ExternalDailyInfo)] = {
    if (isEnabled) {
      logger.info(s"Estimated size is $fileSizeInByte Byte ($fileSizeInMByte MByte), threshold is $bcThreshold MB, can use broadcast: $canBroadcast")
      forceExclusionForAnnoMese(
        forceExclusionForCodRemi(
          forceExclusionForPdr(dailyCons.keyBy(_._1.pdr)
          ), rcuGasSqoopDate)
      ).values
    }
    else dailyCons
  }


  /**
   * Mark all the pdrs given in input as "to force Exclusion"
   *
   * @param dailyCons output of JoinAnagraficaConsumptionController.getJoinedAnagrafica
   * @return dailyCons where dailyConsumption.forceExclusion = true for all the pdr that match conditions
   */
  private def forceExclusionForPdr(dailyCons: RDD[(String, (DailyConsumption, ExternalDailyInfo))]): RDD[(String, (DailyConsumption, ExternalDailyInfo))] = {
    val pdrToExcludeRDD = exclusionFileDf
      .where(isNotNullNorEmpty(col(ForceExclusionFilterSchema.pdr)))
//      .where(isNullOrEmpty(col(ForceExclusionFilterSchema.file)))
      .where(isNullOrEmpty(col(ForceExclusionFilterSchema.cod_remi)))
      .where(isNullOrEmpty(col(ForceExclusionFilterSchema.anno_mese)))
      .select(col(ForceExclusionFilterSchema.pdr))
      .distinct
      .rdd
      .map(row => (row.getAs[String](0), true))

    if (canBroadcast) {
      val pdrToExcludeBC = Environment.getSpark.sparkContext.broadcast(pdrToExcludeRDD.collectAsMap())
      dailyCons.map({ case (pdr, (dailyConsumption, extInfo)) =>
        dailyConsumption.forceExclusion = dailyConsumption.forceExclusion || pdrToExcludeBC.value.contains(pdr)
        (pdr, (dailyConsumption, extInfo))
      })
    } else {
      dailyCons
        .leftOuterJoin(pdrToExcludeRDD)
        .map({ case (pdr, ((dailyConsumption, extInfo), hasMatched)) =>
          dailyConsumption.forceExclusion = dailyConsumption.forceExclusion || hasMatched.isDefined
          (pdr, (dailyConsumption, extInfo))
        })

    }
  }

  /**
   * Mark all the consumptions having t_cod_remi at the date rcugas.sqoop.date equals to the one given in input from an
   * input file as "to force Exclusion"
   *
   * @param dailyCons output of JoinAnagraficaConsumptionController.getJoinedAnagrafica
   * @return dailyCons where dailyConsumption.forceExclusion = true for all the record that match conditions
   */
  private def forceExclusionForCodRemi(dailyCons: RDD[(String, (DailyConsumption, ExternalDailyInfo))], rcuGasSqoopDate: String): RDD[(String, (DailyConsumption, ExternalDailyInfo))] = {
    val remiToExcludeRDD = exclusionFileDf
      .where(isNotNullNorEmpty(col(ForceExclusionFilterSchema.cod_remi)))
//      .where(isNullOrEmpty(col(ForceExclusionFilterSchema.file)))
      .where(isNullOrEmpty(col(ForceExclusionFilterSchema.pdr)))
      .where(isNullOrEmpty(col(ForceExclusionFilterSchema.anno_mese)))
      .select(col(ForceExclusionFilterSchema.cod_remi))
      .distinct
      .rdd
      .map(row => (row.getAs[String](0), true))

    val consWithKeyRemi = dailyCons.map({ case (pdr, (dailyConsumption, extInfos)) =>
      val sqoopDate = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(rcuGasSqoopDate)
        .withTimeAtStartOfDay()
      val remiAtSqoopDate = extInfos.findRcuGasConnessioniDistr2(sqoopDate).map(_.tRemi).getOrElse("")
      (remiAtSqoopDate, (dailyConsumption, extInfos))
    })

    if (canBroadcast) {
      val remiToExcludeBC = Environment.getSpark.sparkContext.broadcast(remiToExcludeRDD.collectAsMap())
      consWithKeyRemi.map({ case (remiAtSqoopDate, (dailyConsumption, extInfo)) =>
        dailyConsumption.forceExclusion = dailyConsumption.forceExclusion || remiToExcludeBC.value.contains(remiAtSqoopDate)
        (dailyConsumption.pdr, (dailyConsumption, extInfo))
      })
    } else {
      consWithKeyRemi.leftOuterJoin(remiToExcludeRDD)
        .map({ case (remi, ((dailyConsumption, extInfo), hasMatched)) =>
          dailyConsumption.forceExclusion = dailyConsumption.forceExclusion || hasMatched.isDefined
          (dailyConsumption.pdr, (dailyConsumption, extInfo))
        })
    }
  }

  /**
   * Mark as "to force Exclusion" all the consumptions having pdr and anno_mese equal to the input from the chosen file
   *
   * @param dailyCons output of JoinAnagraficaConsumptionController.getJoinedAnagrafica
   * @return dailyCons where dailyConsumption.forceExclusion = true for all the record that match conditions
   */
  private def forceExclusionForAnnoMese(dailyCons: RDD[(String, (DailyConsumption, ExternalDailyInfo))]): RDD[(String, (DailyConsumption, ExternalDailyInfo))] = {
    val pdrAndAnnoMeseToExcludeRDD = exclusionFileDf
      .where(isNotNullNorEmpty(col(ForceExclusionFilterSchema.pdr)))
      .where(isNotNullNorEmpty(col(ForceExclusionFilterSchema.anno_mese)))
//      .where(isNullOrEmpty(col(ForceExclusionFilterSchema.file)))
      .where(isNullOrEmpty(col(ForceExclusionFilterSchema.cod_remi)))
      .select(col(ForceExclusionFilterSchema.pdr), col(ForceExclusionFilterSchema.anno_mese))
      .distinct
      .rdd
      .map(row => ((row.getAs[String](0), row.getAs[String](1)), true))

    val dailyConsWithAnnoMese = dailyCons.map({ case (pdr, (dailyConsumption, extInfos)) =>
      ((pdr, dailyConsumption.annoMese.getOrElse("-1")), (dailyConsumption, extInfos))
    })

    if (canBroadcast) {
      val pdrAndAnnoMeseToExcludeBC = Environment.getSpark.sparkContext.broadcast(pdrAndAnnoMeseToExcludeRDD.collectAsMap())
      dailyConsWithAnnoMese.map({ case ((pdr, annoMese), (dailyConsumption, extInfo)) =>
        dailyConsumption.forceExclusion = dailyConsumption.forceExclusion || pdrAndAnnoMeseToExcludeBC.value.exists(map => map._1._1.contains(pdr) && map._1._2.contains(annoMese))
        (pdr, (dailyConsumption, extInfo))
      })
    } else {
      dailyConsWithAnnoMese
        .leftOuterJoin(pdrAndAnnoMeseToExcludeRDD)
        .map({ case ((pdr, annoMese), ((dailyConsumption, extInfo), hasMatched)) =>
          dailyConsumption.forceExclusion = dailyConsumption.forceExclusion || hasMatched.isDefined
          (pdr, (dailyConsumption, extInfo))
        })
    }
  }

  private def isNotNullNorEmpty(c: Column): Column = not(isNullOrEmpty(c))

  private def isNullOrEmpty(c: Column): Column = c.isNull or (trim(c) === lit(""))
}

object ForceExclusionController {
  private val MAX_BC_SIZE_MB = 30
}