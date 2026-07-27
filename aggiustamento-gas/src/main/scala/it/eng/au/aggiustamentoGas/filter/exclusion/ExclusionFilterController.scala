package it.eng.au.aggiustamentoGas.filter.exclusion

import it.eng.au.aggiustamentoGas.model.agg.{ExternalDailyInfo, FlowWithInfo}
import it.eng.au.aggiustamentoGas.model.measure.Flow
import it.eng.au.aggiustamentoGas.schema.agg.ExclusionFilterSchema
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
 * Utilizzato per escludere alcune misure dal processo di calcolo dei consumi
 * @param isStrong true se si tratta del processo di esclusione definitiva. False nel caso di esclusione standard
 */
class ExclusionFilterController(isStrong: Boolean) {
  private val logger = org.apache.log4j.LogManager.getLogger(this.getClass)

  lazy val exclusionFileDf: DataFrame = Environment.getSpark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .schema(ExclusionFilterSchema.createSparkSchema())
    .load(csvPathToLoad)
    .select(
      trim(col(ExclusionFilterSchema.pdr)).as(ExclusionFilterSchema.pdr),
      trim(col(ExclusionFilterSchema.file)).as(ExclusionFilterSchema.file),
      trim(col(ExclusionFilterSchema.cod_remi)).as(ExclusionFilterSchema.cod_remi)//,
//      trim(col(ExclusionFilterSchema.anno_mese)).as(ExclusionFilterSchema.anno_mese)
    )
    .distinct
    .cache

  def isEnabled: Boolean = if (isStrong) Environment.isStrongExclusionFilterEnabled.equalsIgnoreCase("true")
  else Environment.isExclusionFilterEnabled.equalsIgnoreCase("true")

  private val csvPath = if (isStrong) Environment.getStrongExclusionFilterFolder
  else Environment.getExclusionFilterPath

  private val csvPathToLoad = if (isStrong) csvPath + "/*.csv"
  else csvPath

  private val exclusionFileSize = if (isStrong) Try(Environment.getFs.getContentSummary(new Path(csvPath)).getLength)
  else Try(new File(csvPath).length())

  private val fileSizeInByte: Long = if (exclusionFileSize.isSuccess && exclusionFileSize.get != 0L) exclusionFileSize.get else ExclusionFilterController.MAX_BC_SIZE_MB * 1024 * 1024 + 1
  private val fileSizeInMByte: Double = fileSizeInByte / (1024 * 1024)
  private val bcThresholdProp = Environment.getBroadcastThreshold
  private val bcThreshold: Double = if (Try(bcThresholdProp.toLong).isFailure) ExclusionFilterController.MAX_BC_SIZE_MB else bcThresholdProp.toLong

  def canBroadcast: Boolean = fileSizeInMByte <= bcThreshold

  /**
   * Esclude le misure contenute nel file di esclusione [[exclusionFileDf]] dal calcolo dei consumi
   * @param measures misure dalle quali effettuare l'esclusione
   * @return [[measures]] senza le misure contenute nel file di esclusione
   */
  def excludeFlows(measures: RDD[Flow]): RDD[Flow] = {
    if (isEnabled) {
      /** Sottoinsieme delle misure da eslcudere, con file valorizzato */
      val fileToExcludeRDD: RDD[(String, Boolean)] = exclusionFileDf
        .where(isNotNullNorEmpty(col(ExclusionFilterSchema.file)))
        .where(isNullOrEmpty(col(ExclusionFilterSchema.pdr)))
        .where(isNullOrEmpty(col(ExclusionFilterSchema.cod_remi)))
//        .where(isNullOrEmpty(col(ExclusionFilterSchema.anno_mese)))
        .select(col(ExclusionFilterSchema.file))
        .distinct
        .rdd
        .map(row => (row.getAs[String](ExclusionFilterSchema.file), true))

      /** Sottoinsieme delle misure da escludere, con PdR e file valorizzati */
      val pdrWithFileToExcludeRDD: RDD[((String, String), Boolean)] = exclusionFileDf
        .where(isNotNullNorEmpty(col(ExclusionFilterSchema.pdr)))
        .where(isNotNullNorEmpty(col(ExclusionFilterSchema.file)))
        .where(isNullOrEmpty(col(ExclusionFilterSchema.cod_remi)))
//        .where(isNullOrEmpty(col(ExclusionFilterSchema.anno_mese)))
        .select(col(ExclusionFilterSchema.pdr), col(ExclusionFilterSchema.file))
        .distinct
        .rdd
        .map(row => ((row.getAs[String](ExclusionFilterSchema.pdr), row.getAs[String](ExclusionFilterSchema.file)), true))

      /** Sottoinsieme delle misure da escludere, con PdR valorizzato */
      val pdrToExcludeRDD: RDD[(String, Boolean)] = exclusionFileDf
        .where(isNotNullNorEmpty(col(ExclusionFilterSchema.pdr)))
        .where(isNullOrEmpty(col(ExclusionFilterSchema.file)))
        .where(isNullOrEmpty(col(ExclusionFilterSchema.cod_remi)))
//        .where(isNullOrEmpty(col(ExclusionFilterSchema.anno_mese)))
        .select(col(ExclusionFilterSchema.pdr))
        .distinct
        .rdd
        .map(row => (row.getAs[String](ExclusionFilterSchema.pdr), true))

      logger.info(s"Estimated size is $fileSizeInByte Byte ($fileSizeInMByte MByte), threshold is $bcThreshold MB, can use broadcast: $canBroadcast")
      if (canBroadcast) { //perform a BC join to boost performances
        val pdrWithFileToExcludeBC = Environment.getSpark.sparkContext.broadcast(pdrWithFileToExcludeRDD.collectAsMap())
        val fileToExcludeBC = Environment.getSpark.sparkContext.broadcast(fileToExcludeRDD.collectAsMap())
        val pdrToExcludeBC = Environment.getSpark.sparkContext.broadcast(pdrToExcludeRDD.collectAsMap())
        //exclude all those measure with pdr and local_file couples that match the content of the exclusion file
        measures.filter(m => !pdrWithFileToExcludeBC.value.contains((m.pdr, m.localFile.getOrElse("-1"))))
          .filter(m => !fileToExcludeBC.value.contains(m.localFile.getOrElse("-1")))
          .filter(m => !pdrToExcludeBC.value.contains(m.pdr))
      } else {
        measures.map(m => ((m.pdr, m.localFile.getOrElse("-1")), m)) //keyBy pdr and local_file
          .leftOuterJoin(pdrWithFileToExcludeRDD) //left join with couples (pdr, file) to exclude
          .filter({ case ((pdr, file), (flow, hasMatched)) => hasMatched.isEmpty }) //exclude all the measures with a match
          .map({ case ((pdr, file), (flow, hasMatched)) => (file, flow) }) //key by local_file
          .leftOuterJoin(fileToExcludeRDD) //left join with files to exclude
          .filter({ case (file, (flow, hasMatched)) => hasMatched.isEmpty }) //exclude all the measures with a match
          .map({ case (file, (flow, hasMatched)) => (flow.pdr, flow) }) //get the measures
          .leftOuterJoin(pdrToExcludeRDD) //left join with files to exclude
          .filter({ case (pdr, (flow, hasMatched)) => hasMatched.isEmpty }) //exclude all the measures with a match
          .map({ case (pdr, (flow, hasMatched)) => flow }) //get the measures
      }

    } else {
      measures
    }
  }

  /**
   * Esclude le misure contenute nel file di esclusione [[exclusionFileDf]] dal calcolo dei consumi.
   * Questa funzione viene richiamata successivamente rispetto a [[excludeFlows]] perché abbiamo bisogno del campo `cod_remi` valorizzato.
   * @param measures misure dalle quali effettuare l'esclusione
   * @param rcuGasSqoopDate data alla quale considerare il codice remi da rcugas
   * @return [[measures]] senza le misure contenute nel file di esclusione
   */
  def excludeRemiPdr(measures: RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))], rcuGasSqoopDate: String): RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))] = {
    if (isEnabled) {
      /** Sottoinsieme delle misure da escludere, con codice remi valorizzato */
      val remiToExcludeRDD: RDD[(String, Boolean)] = exclusionFileDf
        .where(isNotNullNorEmpty(col(ExclusionFilterSchema.cod_remi)))
        .where(isNullOrEmpty(col(ExclusionFilterSchema.pdr)))
        .where(isNullOrEmpty(col(ExclusionFilterSchema.file)))
//        .where(isNullOrEmpty(col(ExclusionFilterSchema.anno_mese)))
        .select(col(ExclusionFilterSchema.cod_remi))
        .distinct
        .rdd
        .map(row => (row.getAs[String](ExclusionFilterSchema.cod_remi), true))

      /** Sottoinsieme delle misure da escludere, con PdR valorizzato */
      val pdrToExcludeRDD: RDD[(String, Boolean)] = exclusionFileDf
        .where(isNotNullNorEmpty(col(ExclusionFilterSchema.pdr)))
        .where(isNullOrEmpty(col(ExclusionFilterSchema.file)))
        .where(isNullOrEmpty(col(ExclusionFilterSchema.cod_remi)))
//        .where(isNullOrEmpty(col(ExclusionFilterSchema.anno_mese)))
        .select(col(ExclusionFilterSchema.pdr))
        .distinct
        .rdd
        .map(row => (row.getAs[String](ExclusionFilterSchema.pdr), true))

      if (canBroadcast) { //perform a BC join to boost performances
        val remiToExcludeBC = Environment.getSpark.sparkContext.broadcast(remiToExcludeRDD.collectAsMap())
        val pdrToExcludeBC = Environment.getSpark.sparkContext.broadcast(pdrToExcludeRDD.collectAsMap())
        measures.filter({ case (pdr, (flowWithInfo, extInfos)) =>
          val sqoopDate = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(rcuGasSqoopDate).withTimeAtStartOfDay()
          val remiAtSqoopDate = extInfos.findRcuGasConnessioniDistr2(sqoopDate).map(_.tRemi).getOrElse("")
          !remiToExcludeBC.value.contains(remiAtSqoopDate)
        })
          .filter({ case (pdr, (flowWithInfo, extInfos)) => !pdrToExcludeBC.value.contains(pdr)})
      }
      else {
        measures.map({ case (pdr, (flowWithInfo, extInfos)) =>
          val sqoopDate = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(rcuGasSqoopDate).withTimeAtStartOfDay()
          val remiAtSqoopDate = extInfos.findRcuGasConnessioniDistr2(sqoopDate).map(_.tRemi).getOrElse("")
          (remiAtSqoopDate, (pdr, flowWithInfo, extInfos))
        })
          .leftOuterJoin(remiToExcludeRDD)
          .filter({ case (remi, ((pdr, flowWithInfo, extInfos), hasMatched)) => hasMatched.isEmpty })
          .map({ case (remi, ((pdr, flowWithInfo, extInfos), hasMatched)) => (pdr, (flowWithInfo, extInfos)) })
          .leftOuterJoin(pdrToExcludeRDD)
          .filter({ case (pdr, ((flowWithInfo, extInfos), hasMatched)) => hasMatched.isEmpty })
          .map({ case (pdr, ((flowWithInfo, extInfos), hasMatched)) => (pdr, (flowWithInfo, extInfos)) })
      }
    }
    else
      measures
  }

  /**
   * Esegue il processo i backup per i record contenuti nel CSV filtro di eslcusione definitivo (o strong).
   * In questo modo, possiamo risalire ai filtri utilizzati per un certo lancio tramite l'executionid
   */
  def backupExclusionFolder(): Unit = {
    if (isStrong && isEnabled) {
      // we backup the entries in the CSV files into a table
      val backupTable = Environment.getStrongExclusionFilterBackupTable

      exclusionFileDf
        .withColumn("exclusion_type", lit("esclusione dal calcolo"))
        .withColumn("process_type", lit(Environment.getSession))
        .withColumn("execution_id", lit(Environment.executionId).cast(LongType))
        .write
        .mode(SaveMode.Append)
        .partitionBy("execution_id")
        .parquet(backupTable)
    }
  }

  private def isNotNullNorEmpty(c: Column): Column = not(isNullOrEmpty(c))

  private def isNullOrEmpty(c: Column): Column = c.isNull or (trim(c) === lit(""))
}

object ExclusionFilterController {
  private val MAX_BC_SIZE_MB = 30
}
