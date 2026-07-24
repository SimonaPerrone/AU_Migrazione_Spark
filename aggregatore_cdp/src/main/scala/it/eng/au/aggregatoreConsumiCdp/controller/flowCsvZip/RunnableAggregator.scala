package it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip

import it.eng.au.aggregatoreConsumiCdp.schema.{InfoOutputSchema, OutputCsvSchema}
import it.eng.au.aggregatoreConsumiCdp.utility.Constants.CSV_SEPARATOR
import it.eng.au.aggregatoreConsumiCdp.utility.DateTimeUtility.convertStringTimestampToLocalDateTime
import it.eng.au.aggregatoreConsumiCdp.utility.{Environment, FileUtility}
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import java.io.{BufferedInputStream, File, FileInputStream, FileOutputStream}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.immutable.ListMap

trait RunnableAggregator extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val separator = ";"

  // Campi chiavi utilizzati per filtrare elementi non validi delle tabelle e per raggruppare i CSV
  val keyFields: List[String]
  // Nome colonna contenente il valore di partita iva utilizzata per raggruppare i csv e gli zip
  val mainPiva: String
  // Nome colonna contenente il progressivo del nome del CSV
  val counterCsv: String = "counterCsv"
  val aggregatoColumns: ListMap[String, String]
  val baseName: String
  val operationName: String
  val annoTermicoColumnName: String = OutputCsvSchema.anno_competenza
  val writeCsvHeader: Boolean = true
  // Lista delle colonna da usare per il contenuto del file CSV
  val csvFields: List[String]

  def getAggregato(df: DataFrame): DataFrame

  def getTmpCsvOutput: String = Environment.getIsilonBasepathTmp + "/tmp/CDP/" + baseName + "/" + operationName

  def getPathZipOutput: String = Environment.getIsilonBasepathOut + "/CDP/" + baseName

  def getCsvMaxRowLength: Some[Long] = Some(Environment.getCsvMaxRowLength.toLong)

  def getDateToRun: String = Environment.dateRun

  def getCurrentThermalYear: String = Environment.dateCurrentThermalYear

  def getHdfsOutputBasepathInfoLog: String = Environment.getHDFSInfoLogPath

  def getMaxSizeThresholdZip: String = Environment.getMaxSizeThresholdZip

  def getExecutionId: String = Environment.getCaFinalExecutionId

  def convertColumnsToString(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df)((current, c) => current.withColumn(c, col(c).cast(StringType)))
  }

  def obligatoryExpression: Column =
    col(OutputCsvSchema.anno_competenza).isNotNull &&
      col(OutputCsvSchema.cod_pdr).isNotNull &&
      col(OutputCsvSchema.cod_remi).isNotNull &&
      col(OutputCsvSchema.cat_uso).isNotNull &&
      col(OutputCsvSchema.classe_prelievo).isNotNull &&
      col(OutputCsvSchema.zona_climatica).isNotNull &&
      col(OutputCsvSchema.cod_prof_prel_std).isNotNull &&
      col(OutputCsvSchema.prelievo_annuo_prev).isNotNull &&
      col(OutputCsvSchema.data_decorrenza).isNotNull

  def run(df: DataFrame): Unit = {
    val aggregato = getAggregato(df)
      .filter(keyFields.map(f => col(f).isNotNull).reduce(_ && _))

    val aggregatoForCsv = convertColumnsToString(aggregato)
      .na.fill("")

    val csvOutputModel = getCsvOutputModel(aggregatoForCsv, csvFields)
    //    aggregato.persist(StorageLevel.MEMORY_AND_DISK)
    //    logger.warn(s"Count di aggregato: "+ aggregato.count())

    val rddCsvPath = writeCsv(csvOutputModel, csvFields)
    //    rddCsvPath.persist(StorageLevel.MEMORY_AND_DISK)
    //    logger.warn(s"Count di rddCsvPath: "+ rddCsvPath.count())

    val rddWithInfo = writeZip(rddCsvPath)
    //    rddWithInfo.persist(StorageLevel.MEMORY_AND_DISK)
    //    logger.warn(s"Count di rddWithInfo: "+ rddWithInfo.count())

    writeInfoInTable(rddWithInfo)
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
  def writeCsv(rddCsvOutput: RDD[(Map[String, String], Row)], columnsField: List[String]): RDD[(String, (String, String, String))] = {

    val tmpCsvOutput = getTmpCsvOutput
    val daterun = convertStringTimestampToLocalDateTime(getDateToRun)

    //    FileUtility.setYarn777toTmpFolder(tmpCsvOutput)
    FileUtils.deleteDirectory(new File(tmpCsvOutput))

    val sessione = Environment.getCsvFileSessionName
    rddCsvOutput.groupByKey().map({ case (mapKeys, rows) =>
      val countCsv = mapKeys(counterCsv)
      val annoCompetenza = mapKeys(annoTermicoColumnName)
      //val sessione = mapKeys(OutputCsvSchema.sessione)
      val path = tmpCsvOutput + getCsvOutputPath(baseName, mapKeys, daterun, sessione, annoCompetenza, countCsv)

      val records = rows.toList.map(row => {
        columnsField.map {
          case column: String if column == OutputCsvSchema.sessione.toString => sessione
          case column => row.getAs[String](column)
        }.mkString(CSV_SEPARATOR)
      })

      val header = getHeader(csvFields, mapKeys, annoCompetenza, baseName)

      FileUtility.writeCsv(path, header, records, appendMode = true)
      (mapKeys(mainPiva), (path, sessione, annoCompetenza))
    }).groupByKey()
      .mapValues(values => values.head)
  }

  def getHeader(csvFields: List[String], mapKeys: Map[String, String], annoCompetenza: String, baseName: String): String = {
    val firstHeader = baseName match {
      case "CDP1" => (keyFields.map(mapKeys(_)) :+ annoCompetenza).mkString(CSV_SEPARATOR)
      case "CDP2" => (keyFields.reverse.map(mapKeys(_)) :+ annoCompetenza).mkString(CSV_SEPARATOR)
      case "CDP3" => (keyFields.map(mapKeys(_)) :+ annoCompetenza).mkString(CSV_SEPARATOR)
    }

    firstHeader + "\n" + csvFields.mkString(CSV_SEPARATOR).toUpperCase
  }

  def writeZip(rddCsvPath: RDD[(String, (String, String, String))]): RDD[(String, String, String, String, Timestamp, Long)] = {
    val tmpCsvOutput = getTmpCsvOutput
    val pathZipOutput = getPathZipOutput
    val daterun = convertStringTimestampToLocalDateTime(getDateToRun)
    val maxDimensionZip = getMaxSizeThresholdZip.toLong
    val caFinalExecutionId = getExecutionId
    val executionId = Environment.executionId
    val timestampToRun = Timestamp.valueOf(daterun)

    val rddInfo = rddCsvPath.flatMap({ case (pivaHead, (path, sessione, annoCompetenza)) =>
      val pathInputFileCsv = new File(path)
      val outputFolder = pathInputFileCsv.getParent.replaceAll(tmpCsvOutput, pathZipOutput)
      var zipName = getZipOutputName(pivaHead, sessione, annoCompetenza, daterun)
      val originalZipName = zipName
      val outputFolderIfExists = new File(outputFolder)

      var count = 1
      val exist = if (outputFolderIfExists.exists()) {

        var zip = new ZipOutputStream(new FileOutputStream(FileUtility.create777File(outputFolder + zipName)))

        pathInputFileCsv.getParentFile.listFiles().filter(value => value.getName.substring(value.getName.length - 4).equals(".csv")).foreach { csvFile =>
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
        (caFinalExecutionId, operationName, baseName,
          if (exist == "") outputFolder + originalZipName.replace("_1.zip", s"_$num.zip")
          else exist
          , timestampToRun, executionId)
      }
      result
    })

    //    (executionId, operationNameVal, baseNameVal, outputFolder.getPath + zipName.replace("_1.zip", s"_$num.zip"), timestampRun, timestampRun.getTime)

    rddInfo
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

  def getCsvOutputPath(baseName: String, mapKeys: Map[String, String], date: LocalDateTime, sessione: String, annoCompetenza: String, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaPathFolderHead = mapKeys(mainPiva)
    val pivaNameFile = keyFields.map(mapKeys(_)).mkString("_")

    s"/${baseName}_$pivaPathFolderHead/$year/$month/${pivaNameFile}_CDP_${operationName}_${annoCompetenza}_${timestamp}_${counterCsv}.csv"
  }

  def getZipOutputName(pivaFolder: String, sessione: String, annoCompetenza: String, today: LocalDateTime): String = {
    //val month = ("0" + today.getMonthValue.toString).takeRight(2)
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    //    val zipName = s"/${baseName}_$pivaFolder/$year/$month/${pivaFolder}_${operationName}_${year}_${timestamp}_1.zip"
    val zipName = s"/${pivaFolder}_CDP_${operationName}_${annoCompetenza}_${timestamp}_1.zip"
    zipName
  }

  def writeInfoInTable(rdd: RDD[(String, String, String, String, Timestamp, Long)]): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    rdd
      .toDF(InfoOutputSchema.getValues: _*)
      .repartition(10)
      .write.partitionBy(InfoOutputSchema.partition_date).mode(SaveMode.Append).parquet(getHdfsOutputBasepathInfoLog)

    if (!Environment.isLocalMode) Environment.spark.sql(s"MSCK REPAIR TABLE ${Environment.getAggregatoreInfoLogTableName}")
  }

  def getCsvOutputModel(aggFilter: DataFrame, csvFields: List[String]): RDD[(Map[String, String], Row)] = {
    val keys = keyFields :+ counterCsv :+ annoTermicoColumnName :+ OutputCsvSchema.sessione.toString
    // colonne finali file CSV
    val columns = (csvFields ++ keys).distinct

    distribution(aggFilter) // aggiunge colonna counterCsv
      .selectExpr(columns: _*)
      .rdd
      .map(row => {
        val listMap = keys.map(column => column -> row.getAs[String](column)).toMap

        (listMap, row)
      })
  }

  /**
   * input:
   * keyPiva1
   * 1
   * 1
   * 1
   * 2
   * 3
   *
   * @param df
   * @param prop
   * @return
   * after count (ex. max 2 row in csv):
   * keyPiva1 counterCsv
   * 1          3
   * 1          3
   * 1          3
   * 2          1
   * 3          1
   *
   * ouput:
   * keyPiva1 counterCsv
   * 1          1
   * 1          2
   * 1          1
   * 2          1
   * 3          1
   */
  def distribution(df: DataFrame): DataFrame = {
    val csvMaxNumberRow = getCsvMaxRowLength.get
    val window = Window.partitionBy(keyFields.map(col): _*)

    df
      .withColumn(counterCsv, count("*").over(window))
      .withColumn(counterCsv, ((monotonically_increasing_id % ceil(col(counterCsv) / csvMaxNumberRow)) + 1).cast(StringType))
  }
}
