package it.eng.au.pubblicazioneIndennizzi.controller.traits

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import it.eng.au.pubblicazioneIndennizzi.dao.Dao
import it.eng.au.pubblicazioneIndennizzi.dao.cig.CIGPubblicazioneIndennizziDao
import it.eng.au.pubblicazioneIndennizzi.model.CIGPubblicazioneIndennizzi
import it.eng.au.pubblicazioneIndennizzi.utility.Constants.{EURO, PERCENT, _EURO_SYMBOL_, _PERCENT_SYMBOL_}
import it.eng.au.pubblicazioneIndennizzi.utility.{FileUtility, Properties}
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ceil, col, count, monotonically_increasing_id}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, Row}

import java.io.{BufferedInputStream, File, FileInputStream, FileOutputStream}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.immutable.ListMap

trait RunnableAggregator extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val CSV_SEPARATOR = ";"
  //val writeCsvHeader = true
  val annoMese: String = "annoMese"
  val counterCsv: String = "counterCsv"
  val aggregatoColumns: ListMap[String, String]
  val keyFields: ListMap[String, String]
  val pivaFirst: String = "pivaFirst"
  val pivaSecond: String = "pivaSecond"
  val csvFields: List[String]
  val flowName: String // It can take the values IZG1, IZG2, DETTAGLIO_PDR_IZG1 or DETTAGLIO_PDR_IZG2.
  val flowBaseName: String // It can take the values IZG1 or IZG2. In practice it specify wheter the flow is directed to the ID (IZG1) or to the UDD (IZG2).
  val baseName: String // It can take the values CIG1 CIG2. It's like the above flowBaseName val and for this reason we have to tell AQ weather this should be changed.
  val operationName: String

  def daoTableName: Dao

  // A couple of examples of the output of the following function:
  // Ex. ../tmp/IZG/IZG1/IZG_AGGREGATOR (For the IZG flow directed to the ID)
  // Ex. ../tmp/IZG/IZG2/DETTAGLIO_PDR_IZG (For the DETTAGLIO_PDR_IZG flow directed to the UDD) .... and so on..
  def getTmpCsvOutput: String = Properties.getIsilonBasepathTmp + s"/tmp/$getPublicationType/" + flowBaseName + "/" + operationName
  def getPathZipOutput: String = Properties.getIsilonBasepathOut + s"/$getPublicationType/" + flowBaseName

  def fileSpecificFilterExpression: Column

  def getAggregato(df: DataFrame): DataFrame

  def run(): Unit = {
    val (df, partitionRead) = if (!Properties.isRecoveryMode)
      daoTableName.readLastPartition()
    else daoTableName.readTableByPartiton(Properties.getInputTableExecutionId)

    specificRun(df, partitionRead)
  }

  def specificRun(df: DataFrame, partitionRead: String): Unit = {
    val aggregato = getAggregato(df)
    val aggregatoFiltered = aggregato.filter(keyFields.values.toList.map(f => col(f).isNotNull).reduce(_ && _))
    val aggregatoForCsv = convertColumnsToString(aggregatoFiltered).na.fill("")

    val csvOutputModel = getCsvOutputModel(aggregatoForCsv, csvFields)
    //    aggregato.persist(StorageLevel.MEMORY_AND_DISK)
    //    logger.warn(s"Count di aggregato: "+ aggregato.count())

    val rddCsvPath = writeCsv(csvOutputModel, csvFields)

    //    rddCsvPath.persist(StorageLevel.MEMORY_AND_DISK)
    //    logger.warn(s"Count di rddCsvPath: "+ rddCsvPath.count())
    val rddWithInfo = writeZip(rddCsvPath, partitionRead)

    //    rddWithInfo.persist(StorageLevel.MEMORY_AND_DISK)
    //    logger.warn(s"Count di rddWithInfo: "+ rddWithInfo.count())

    //writeInfoInTable(rddWithInfo)
    val pubblicazioneIndennizziDao = new CIGPubblicazioneIndennizziDao()
    pubblicazioneIndennizziDao.writeParquet(rddWithInfo)
  }

  def getCsvOutputModel(aggFilter: DataFrame, csvFields: List[String]): RDD[(Map[String, String], Row)] = {
    val keys = keyFields.values.toList :+ counterCsv

    val columns = ((csvFields :+ counterCsv) ++ keyFields.values.toList).distinct

    //aggFilter.show()
    var dfDistribution = distribution(aggFilter)
      .selectExpr(
        columns: _*
      )

    columns.foreach(column =>
      dfDistribution = dfDistribution.withColumn(column, col(column).cast(StringType))
    )

    dfDistribution
      .rdd
      .map(row => {
        val listMap = keys.map(column => column -> row.getAs[String](column)).toMap
        (listMap, row)
      })
  }

  def distribution(df: DataFrame): DataFrame = {
    val csvMaxNumberRow = getCsvMaxRowLength.get
    val window = Window.partitionBy(keyFields.values.toList.map(col): _*)
    df
      .withColumn(counterCsv, count("*").over(window))
      .withColumn(counterCsv, ((monotonically_increasing_id % ceil(col(counterCsv) / csvMaxNumberRow)) + 1).cast(StringType))
  }

  def convertColumnsToString(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df)((current, c) => current.withColumn(c, col(c).cast(StringType)))
  }

  def getCsvMaxRowLength: Some[Long] = Some(Properties.getMaxNumRowFile.toLong)
  def getMaxSizeThresholdZip: String = Properties.getMaxSizeThresholdZip

  //def getDateToRun:String = PropertyUtilityPubblicazioneIndennizzi.getDateToRun // è sostituito da env.startdate
  def convertStringTimestampToLocalDateTime(localDateTime: String): LocalDateTime = {
    Timestamp.valueOf(localDateTime).toLocalDateTime
  }

  def getPublicationType: String = Properties.getPublicationType

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

  def writeCsv(rddCsvOutput: RDD[(Map[String, String], Row)], columnsField: List[String], optionalOperationName: Option[String] = None): RDD[(Map[String, String], String)] = {
    val tmpCsvOutput = getTmpCsvOutput
    //val publicationType = getPublicationType
    //val baseName = publicationType + baseNumber
    val daterun = Environment.startDateTime

    //        FileUtility.setYarn777toTmpFolder(tmpCsvOutput)
    FileUtils.deleteDirectory(new File(tmpCsvOutput))
    rddCsvOutput.groupByKey().map({ case (mapKeys, rows) =>

      //val countCsv = mapKeys(counterCsv)
      //val am = mapKeys(annoMese)
      //val path = tmpCsvOutput + getCsvOutputPath(baseName, mapKeys, daterun, publicationType, countCsv, am, optionalOperationName)
      val path = tmpCsvOutput + getCsvOutputPath(mapKeys, daterun)
      val records = rows.toList.map(row => {
        columnsField.map(column => {
          row.getAs[String](column)
        }).mkString(CSV_SEPARATOR)
      })

      val header = columnsField.mkString(CSV_SEPARATOR)
        .replace(_PERCENT_SYMBOL_, PERCENT)
        .replace(_EURO_SYMBOL_, EURO)

      FileUtility.writeCsv(path, Some(header), records, appendMode = true)
      //((mapKeys(pivaFirst), mapKeys(pivaSecond)), path) // Da ragionarci dopo, durante la scrittura della funzione writeZip..
      (mapKeys.-(counterCsv), path)
    }).groupByKey()
      .mapValues(values => values.head)
  }

  def writeZip(rddCsvPath: RDD[(Map[String, String], String)], partitionRead: String): RDD[CIGPubblicazioneIndennizzi] = { //RDD[(String, String, String, String, Timestamp, Long)] = {
    val tmpCsvOutput = getTmpCsvOutput
    val pathZipOutput = getPathZipOutput
    val daterun = Environment.startDateTime
    val maxDimensionZip = getMaxSizeThresholdZip.toLong
    val inputTableExecutionid = partitionRead
    val timestampToRun = Timestamp.valueOf(daterun)
    //val baseName = getPublicationType + baseNumber
    //    val publicationType = getPublicationType
    //val year = getYear

    val rddInfo = rddCsvPath.flatMap({ case (mapKeys, path) =>
      val pathInputFileCsv = new File(path)
      val outputFolder = pathInputFileCsv.getParentFile.getParent.replaceAll(tmpCsvOutput, pathZipOutput)
      var zipName = getZipOutputName(mapKeys, daterun)
      val originalZipName = zipName
      val outputFolderIfExists = new File(outputFolder)

      var count = 1
      val exist = if (outputFolderIfExists.exists() && outputFolderIfExists.canWrite) {

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
        logger.warn(s"Couldn't write to $outputFolder, the path does not exits or permission are not set properly.")
        s" Couldn't write to ${outputFolder + originalZipName}, the path does not exits or permission are not set properly."
      }

      val result = (1 to count).toList.map { num =>
        CIGPubblicazioneIndennizzi(
          input_table_execution_id = inputTableExecutionid,
          operation_name = operationName,
          base_name = baseName,
          path_name = if (exist == "") outputFolder + originalZipName.replace("_1.zip", s"_$num.zip") else exist,
          load_date = timestampToRun,
          annomese = mapKeys(keyFields(annoMese))
        )
      }
      result
    })

    rddInfo
  }


  def getZipOutputName(mapKeys: Map[String, String], today: LocalDateTime): String = {
    //    val year = today.getYear
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    //val pivaFolder = mapKeys(piva)
    //val year = mapKeys(periodoCompetenza)
    //val idRequest = mapKeys(idrichiesta)

    //val zipName = s"/${pivaFolder}_${ccg}_${operationName}_${year}_${timestamp}_${idRequest}_1.zip"
    //val zipName = s"/${mapKeys(pivaFirst)}_${mapKeys(pivaSecond)}_${mapKeys(annoMese)}_${flowName}_${timestamp}_${mapKeys(counterCsv)}.zip"
    val zipName = s"/${mapKeys(keyFields(pivaFirst))}_${mapKeys(keyFields(pivaSecond))}_${mapKeys(keyFields(annoMese))}_${flowName}_${timestamp}_1.zip"
    zipName
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

  def getCsvOutputPath(mapKeys: Map[String, String], date: LocalDateTime): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    // <PIVA_UdD>_<PIVA_Id>_AAAAMM_IZG1_Timestamp_progressivo.csv
    // <PIVA_UdD>_<PIVA_Id>_AAAAMM_IZG2_Timestamp_progressivo.csv
    // <PIVA_Id>_<PIVA_UdD>_AAAAMM_DETTAGLIO_PDR_IZG1_Timestamp_Progressivo.zip
    // <PIVA_Id>_<PIVA_UdD>_AAAAMM_DETTAGLIO_PDR_IZG2_Timestamp_Progressivo.zip
    // Timestamp   --> rappresenta il timestamp (anno/mese/giorno/ora/minuti/secondi) in cui è stato generato il file;
    // progressivo --> un numero progressivo utilizzato per la segmentazione dei file relativi allo stesso anno di competenza. (il nostro CounterCSV.)
    //s"/${baseName}_$pivaPathFolderHead/$year/$month/${pivaNameFile}_${publicationType}_${annoMese}_${timestamp}_${counterCsv}.csv"

    //s"/${keyFields(pivaFirst)}_${keyFields(pivaSecond)}_${mapKeys(annoMese)}_${flowName}_${timestamp}_${mapKeys(counterCsv)}.csv"

    // Example of the following output:
    // ../CIG1_0123456789/2022/10/0123456789_987654321_202101_DETTAGLIO_PDR_IZG1_20221028104406_15

    //for ((k,v) <- mapKeys) println(s"\nkey: $k, value: $v\n") // TODO: Remove Debug
    //println(mapKeys(keyFields(pivaFirst)))
    val pivaid = mapKeys(keyFields(pivaFirst))
    s"/${baseName}_${mapKeys(keyFields(pivaFirst))}/$year/$month/${baseName}_${mapKeys(keyFields(pivaFirst))}_${mapKeys(keyFields(pivaSecond))}/${mapKeys(keyFields(pivaFirst))}_${mapKeys(keyFields(pivaSecond))}_${mapKeys(keyFields(annoMese))}_${flowName}_${timestamp}_${mapKeys(counterCsv)}.csv"
  }

}
