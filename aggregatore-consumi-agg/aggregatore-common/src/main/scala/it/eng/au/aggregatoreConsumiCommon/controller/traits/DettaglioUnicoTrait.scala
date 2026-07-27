package it.eng.au.aggregatoreConsumiCommon.controller.traits

import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.elencoFlussi.ElencoFlussiDettaglioUnico
import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.pdr.PdrDettaglioUnico
import it.eng.au.aggregatoreConsumiCommon.schema.{DettaglioUnicoSchema, InfoOutputSchema, ValidatedFlowsAggSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.{Environment, FileUtility}
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode

import java.io.{BufferedInputStream, FileInputStream}
import java.util.zip.ZipEntry
//import it.eng.au.etlAuFramework.utility.FileUtility

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ceil, col, count, monotonically_increasing_id}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row}

import java.io.{File, FileOutputStream}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.ZipOutputStream
import scala.collection.immutable.ListMap

trait DettaglioUnicoTrait extends RunnableAggregatorTrait {
  val operationName = "DETTAGLIO_UNICO"
  val annoMese: String = DettaglioUnicoSchema.Annomese.toString

  //  override val aggregatoColumns: ListMap[String, String] = null
  val keyPiva1: String
  val keyPiva2: String
  val mainPiva: String
  val counterCsv: String = "counterCsv"
  //  override def getAggregato(df: DataFrame): DataFrame = throw new Exception("not supported")

  val pdrDettaglioUnico: PdrDettaglioUnico
  val elencoFlussiDettaglioUnico: ElencoFlussiDettaglioUnico
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  /**
   * Esegue il processo di creazione CSV parallelamente per
   *  - l'insieme dei PdR da pubblicare e i relativi consumi (CSV dei consumi);
   *  - l'insieme dei PdR da pubblicare e i relativi flussi (CSV dell'elenco flussi).
   *  Prima della scrittura dello ZIP, effettua l'union tra i due RDD in modo tale da inserire le due tipologie di CSV nello stesso ZIP.
   *  Infine scrive l'esito della pubblicazione nella tabella di reportistica.
   * @param df tabella dei consumi
   */
  override def run(df: DataFrame): Unit = {
    val dfAggregatoElencoFlussi = elencoFlussiDettaglioUnico.getAggregato(df, getAndPrepareValidateFlow())
    val dfAggregatoPdr = pdrDettaglioUnico.getAggregato(df)

    val dfAggregatoElencoFlussiForCsv = convertColumnsToString(dfAggregatoElencoFlussi).na.fill("")
    val dfAggregatoPdrForCsv = convertColumnsToString(dfAggregatoPdr).na.fill("")

    val fieldsElencoFlussi = elencoFlussiDettaglioUnico.getCsvFields(dfAggregatoElencoFlussiForCsv)
    val fieldsPdr = pdrDettaglioUnico.getCsvFields(dfAggregatoPdrForCsv)

    val csvOutputModelElencoFlussi = getCsvOutputModel(dfAggregatoElencoFlussiForCsv, List(keyPiva1, keyPiva2, counterCsv) ::: fieldsElencoFlussi)
    val csvOutputModelPdr = getCsvOutputModel(dfAggregatoPdrForCsv, List(keyPiva1, keyPiva2, counterCsv) ::: fieldsPdr)

    val rddInfoElencoFlussi = writeCsvAnnoMese(csvOutputModelElencoFlussi, fieldsElencoFlussi, elencoFlussiDettaglioUnico)
    val rddInfoPdr = writeCsvAnnoMese(csvOutputModelPdr, fieldsPdr, pdrDettaglioUnico)
    val rddInfo = rddInfoElencoFlussi.union(rddInfoPdr).coalesce(rddInfoPdr.getNumPartitions)
      .groupByKey()
      .mapValues(values => values.head)

    val rddWithInfo = writeZipAnnoMese(rddInfo)

    //    rddWithInfo.persist(StorageLevel.MEMORY_AND_DISK)
    //    logger.warn(s"Count di rddWithInfo: "+ rddWithInfo.count())

    writeInfoInTable(rddWithInfo)
    //    pdrDettaglioUnico.write(dfAggregatoPdr, fieldsPdr)
    //    elencoFlussiDettaglioUnico.write(dfAggregatoElencoFlussi, fieldsElencoFlussi)

    //    val rddInfo = mergeAndZipFiles(fieldsPdr, fieldsElencoFlussi)

    //    writeInfoInTable(rddInfo)
  }

  def writeInfoInTable(rdd: RDD[(String, String, String, String, Timestamp, Long)]): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    rdd
      .toDF(InfoOutputSchema.getValues: _*)
      .repartition(10)
      .write.partitionBy(InfoOutputSchema.partition_date).mode(SaveMode.Append).parquet(getHdfsOutputBasepathInfoLog)
  }

  /**
   * the method write csv with two header ex:
   * PIVA_UDD	PIVA_DISTR	ANNO_COMPETENZA
   * COD_PDR	COD_REMI	CAT_USO	CLASSE_PRELIEVO
   * ----------------------------------------------------
   * codicepdr2	codiceremi2	catuso2	classeprelievo2
   * codicepdr3	codiceremi3	catuso3	classeprelievo3
   * codicepdr4	codiceremi4	catuso4	classeprelievo4
   * codicepdr5	codiceremi5	catuso5	classeprelievo5
   *
   * @param rddCsvOutput
   * @param daterun
   * @param prop
   * @param sc
   * @return rdd of (piva,path) unique because i need the parent path
   *         Ex. | piva  | path
   *         --------------------------------------
   *         | piva1 | piva1/piva1_pivadistr1.csv
   *         | piva1 | piva1/piva1_pivadistr2.csv
   *         | piva1 | piva1/piva1_pivadistr3.csv
   *         | piva1 | piva1/piva1_pivadistr4.csv
   *
   *         return
   *         | piva  | path
   *         --------------------------------------
   *         | piva1 | piva1/piva1_pivadistr1.csv
   */
  val CSV_SEPARATOR = ";"

  def writeCsvAnnoMese(rddCsvOutput: RDD[(ListMap[String, String], Row)], columnsField: List[String], csvOutputPath: RunnableAggregatorPerfomanceOld): RDD[((String, String), String)] = {

    val tmpCsvOutput = getTmpCsvOutput
    val publicationType = getPublicationType
    val daterun = convertStringTimestampToLocalDateTime(getDateToRun)
    val sessionName = getSessionName
    val baseName = getPublicationType + baseNumber

    //    FileUtility.setYarn777toTmpFolder(tmpCsvOutput)
    FileUtils.deleteDirectory(new File(tmpCsvOutput))

    rddCsvOutput.groupByKey().map({ case (mapKeys, rows) =>
      val piva1 = mapKeys(keyPiva1)
      val piva2 = mapKeys(keyPiva2)
      val mainPivaValue = mapKeys(mainPiva)
      val annomese = mapKeys(annoMese)
      val countCsv = mapKeys(counterCsv)
      val path = tmpCsvOutput + csvOutputPath.getCsvOutputPath(baseName, piva1, piva2, annomese, sessionName, daterun, countCsv)

      val records = rows.toList.map(row => {
        columnsField.map(column => {
          row.getAs[String](column)
        }).mkString(CSV_SEPARATOR)
      })

      val header = publicationType match {
          case "AGG" => Some(columnsField.mkString(CSV_SEPARATOR))
          case "SBG" => Some(columnsField.map(_.toLowerCase).mkString(CSV_SEPARATOR))
        }

      FileUtility.writeCsv(path, header, records, appendMode = true)
      ((mainPivaValue, annomese), path)
    }).groupByKey()
      .mapValues(values => values.head)
  }

  def convertStringTimestampToLocalDateTime(localDateTime: String): LocalDateTime = {
    Timestamp.valueOf(localDateTime).toLocalDateTime
  }

  def getCsvOutputModel(aggFilter: DataFrame, columns: List[String]): RDD[(ListMap[String, String], Row)] = {

    distribution(aggFilter)
      .selectExpr(
        columns: _*
      )
      .rdd
      .map(row => {

        val listMap =
          ListMap(
            keyPiva1 -> row.getAs[String](keyPiva1),
            keyPiva2 -> row.getAs[String](keyPiva2),
            annoMese -> row.getAs[String](annoMese),
            counterCsv -> row.getAs[String](counterCsv)
          )
        (
          listMap,
          row
        )
      })
  }

  def distribution(df: DataFrame): DataFrame = {
    val csvMaxNumberRow = getCsvMaxRowLength.get

    val window = Window.partitionBy(keyPiva1, keyPiva2, annoMese)

    val df1 = df
      .withColumn("counterCsvTemp", count("*").over(window))
      .withColumn(counterCsv, ((monotonically_increasing_id % ceil(col("counterCsvTemp") / csvMaxNumberRow)) + 1).cast(StringType))
    /*
        df1.persist(StorageLevel.MEMORY_AND_DISK_SER)
        logger.warn(s"Count di df1: "+ df1.count())

        val executionId = Timestamp.valueOf(LocalDateTime.now()).getTime

        df1
          .select(
            col(keyPiva1).cast(StringType),
            col(keyPiva2).cast(StringType),
            col(annoMese).cast(StringType),
            col(counterCsv).cast(StringType),
            col("counterCsvTemp").cast(StringType)
          )
          .withColumn("execution_id", lit(executionId))
          .write.mode("append").partitionBy("execution_id").parquet("/user/hive/warehouse/eng_test.db/aggregatore_verify")
    */
    //      .repartition(4000)
    val df2 = df1.drop("counterCsvTemp")
      .repartition(4000, col(keyPiva1), col(keyPiva2), col(annoMese), col(counterCsv))

    /* df2.persist(StorageLevel.MEMORY_AND_DISK_SER)
     logger.warn(s"Count di df2: "+ df2.count())
     df1.unpersist()
 */
    df2
  }

  def putIntoZip(zip: ZipOutputStream, name: File): Unit = {
    zip.putNextEntry(new ZipEntry(name.getName))
    val in = new BufferedInputStream(new FileInputStream(name.getPath))
    var b = in.read()
    while (b > -1) {
      zip.write(b)
      b = in.read()
    }
    in.close()
    zip.closeEntry()
  }

  def writeZipAnnoMese(rddCsvPath: RDD[((String, String), String)]): RDD[(String, String, String, String, Timestamp, Long)] = {
    val tmpCsvOutput = getTmpCsvOutput
    val pathZipOutput = getPathZipOutput
    val daterun = convertStringTimestampToLocalDateTime(getDateToRun)
    val maxDimensionZip = getMaxSizeThresholdZip.toLong
    val executionId = getExecutionId
    val timestampToRun = Timestamp.valueOf(daterun)
    val sessionName = getSessionName
    val baseName = getPublicationType + baseNumber
    val year = getYear

    val rddInfo = rddCsvPath.flatMap({ case ((pivaHead, annomese), path) =>
      val pathInputFileCsv = new File(path)
      val outputFolder = pathInputFileCsv.getParent.replaceAll(tmpCsvOutput, pathZipOutput)
      var zipName = getZipOutputName(pivaHead, annomese, sessionName, daterun, year)
      val originalZipName = zipName
      val outputFolderIfExists = new File(outputFolder)

      var count = 1
      val exist = if (outputFolderIfExists.exists()) {

        var zip = new ZipOutputStream(new FileOutputStream(FileUtility.create777File(outputFolder + zipName)))

        pathInputFileCsv.getParentFile.listFiles().filter(value => value.getName.substring(value.getName.length - 4).equals(".csv") && value.getName.contains(sessionName + "_" + annomese + "_")).foreach { csvFile =>
          val readZip = new File(outputFolder + zipName)
          val dimensionZipFile = readZip.length()
          if (dimensionZipFile < maxDimensionZip) {
            putIntoZip(zip, csvFile)
          }
          else {
            zip.close()
            count += 1
            zipName = originalZipName.replace("_1.zip", "_" + count + ".zip")
            zip = new ZipOutputStream(new FileOutputStream(FileUtility.create777File(outputFolder + zipName)))
            putIntoZip(zip, csvFile)
          }
        }
        zip.close()
        ""
      } else {
        logger.warn(s"Couldn't write to $outputFolder, the path does not exits.")
        s" Couldn't write to ${outputFolder + originalZipName}, the path does not exits."
      }
      val result = (1 to count).toList.map { num =>
        (executionId, operationName, baseName,
          if (exist == "") outputFolder + originalZipName.replace("_1.zip", s"_$num.zip")
          else exist
          , timestampToRun, timestampToRun.getTime)
      }
      result
    })

    //    (executionId, operationNameVal, baseNameVal, outputFolder.getPath + zipName.replace("_1.zip", s"_$num.zip"), timestampRun, timestampRun.getTime)

    rddInfo
  }

  def getZipOutputName(pivaFolder: String, annomese: String, sessionName: String, today: LocalDateTime, year: String): String = {
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    //es: 0123456789_DETTAGLIO_PDR_(AGG_S1_PRE|SBG)_2022(04)_20220428105421_202204_1.zip
    val zipName = s"/${pivaFolder}_DETTAGLIO_PDR_${sessionName}_${year}_${timestamp}_${annomese}_1.zip"
    zipName
  }

  def getAndPrepareValidateFlow(): DataFrame = {
    Environment.sqlContext.table(Environment.getValidatedFlowTableName)
      .selectExpr(ValidatedFlowsAggSchema.getValues: _*)
      .filter(col(ValidatedFlowsAggSchema.executionid) === Environment.getDailyConsumptionExecutionid)
      .drop(col(ValidatedFlowsAggSchema.executionid))
  }


  def runTest(df: DataFrame): RDD[(String, String, String, String, Timestamp, Long)] = {
    val dfAggregatoElencoFlussi = elencoFlussiDettaglioUnico.getAggregato(df, getAndPrepareValidateFlowTest())
    val dfAggregatoPdr = pdrDettaglioUnico.getAggregato(df)

    val dfAggregatoElencoFlussiForCsv = convertColumnsToString(dfAggregatoElencoFlussi).na.fill("")
    val dfAggregatoPdrForCsv = convertColumnsToString(dfAggregatoPdr).na.fill("")

    val fieldsElencoFlussi = elencoFlussiDettaglioUnico.getCsvFields(dfAggregatoElencoFlussiForCsv)
    val fieldsPdr = pdrDettaglioUnico.getCsvFields(dfAggregatoPdrForCsv)

    val csvOutputModelElencoFlussi = getCsvOutputModel(dfAggregatoElencoFlussiForCsv, List(keyPiva1, keyPiva2, counterCsv) ::: fieldsElencoFlussi)
    val csvOutputModelPdr = getCsvOutputModel(dfAggregatoPdrForCsv, List(keyPiva1, keyPiva2, counterCsv) ::: fieldsPdr)

    val rddInfoElencoFlussi = writeCsvAnnoMese(csvOutputModelElencoFlussi, fieldsElencoFlussi, elencoFlussiDettaglioUnico).cache
    val rddInfoPdr = writeCsvAnnoMese(csvOutputModelPdr, fieldsPdr, pdrDettaglioUnico).cache()

    rddInfoPdr.collect().foreach(println)
    rddInfoElencoFlussi.collect().foreach(println)
    val rddInfo = rddInfoPdr.union(rddInfoElencoFlussi).coalesce(rddInfoPdr.getNumPartitions)
      .groupByKey()
      .mapValues(values => values.head)

    val rddWithInfo = writeZipAnnoMese(rddInfo)

    //    rddWithInfo.persist(StorageLevel.MEMORY_AND_DISK)
    //    logger.warn(s"Count di rddWithInfo: "+ rddWithInfo.count())

    //    writeInfoInTable(rddWithInfo)
    rddWithInfo
    //    pdrDettaglioUnico.write(dfAggregatoPdr, fieldsPdr)
    //    elencoFlussiDettaglioUnico.write(dfAggregatoElencoFlussi, fieldsElencoFlussi)

    //    val rddInfo = mergeAndZipFiles(fieldsPdr, fieldsElencoFlussi)

    //    writeInfoInTable(rddInfo)
  }

  def getAndPrepareValidateFlowTest(): DataFrame = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val date = java.sql.Date.valueOf("2020-12-12")

    Environment.sparkContext.parallelize(
      List(
        ("000PDR", date, "IGMGPRE", true, "/mnt/isilon/piva11111111_piva000000000/2020/0101/piva11111111_piva000000000_file.AAOOlls._sx.zml", "123")
        , ("000PDR", date, "RGL", false, "/mnt/isilon/piva11111111_piva1234/2020/0101/piva11111111_piva000000000_file.AAOOlls._sx.zml", "123")
      )
    ).toDF(ValidatedFlowsAggSchema.getValues: _*)

      //      .filter(col(ValidatedFlowsAggSchema.executionid) === Environment.getDailyConsumptionExecutionid)
      .drop(col(ValidatedFlowsAggSchema.executionid))
  }
}
