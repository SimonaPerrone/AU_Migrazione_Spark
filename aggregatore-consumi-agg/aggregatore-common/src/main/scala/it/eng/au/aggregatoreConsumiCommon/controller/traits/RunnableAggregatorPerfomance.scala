package it.eng.au.aggregatoreConsumiCommon.controller.traits

import it.eng.au.aggregatoreConsumiCommon.schema.InfoOutputSchema
import it.eng.au.aggregatoreConsumiCommon.utility.{Environment, FileUtility}
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import java.io.{BufferedInputStream, File, FileInputStream, FileOutputStream}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.immutable.ListMap
import scala.language.postfixOps

/**
 * Trait principale del progetto, contenente il metodo [[run]] che viene richiamato nel Driver per ogni pubblicazione da effettuare.
 */
trait RunnableAggregatorPerfomance extends RunnableAggregatorTrait {
  val CSV_SEPARATOR = ";"

  /** Utilizzato per splittare i record in file CSV */
  val counterCsv: String = "counterCsv"
  /** Colonne utilizzate in fase di scrittura del CSV; comprendono i campi da scrivere nel CSV più eventuali campi utilizzati
   * in fase di scrittura dei CSV e degli ZIP (e.g. i campi chiave piva, annomese) */
  val aggregatoColumns: ListMap[String, String]
  /** Chiave/i da utilizzare per lo split dei CSV. In genere è composto da una o più partite ive, e.g. (piva_udd, piva_id) */
  val keyFields: List[String]
  /** Tra le piva presenti in [[keyFields]], è la piva principale */
  val mainPiva: String = ""
  /** Necessario nell'elenco flussi per evitare ambiguità */
  val annoMese: String = "annoMese"
  /** Campi da pubblicare nel CSV */
  val csvFields: List[String]
  /** Indica se l'header nel CSV va scritto o meno */
  val writeCsvHeader: Boolean = true

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  /**
   * Esegue la pubblicazione dei file CSV compressi in ZIP per una certa tipologia (aggregato, incoerenti, ...):
   *  - filtra la tabella dei consumi [[df]] e esegue eventuali operazioni di raggruppamento e pivot necessarie per la pubblicazione ([[getAggregato]]);
   *  - esegue lo split dei record, assegnando a ognuno di essi un counter che indica in quale CSV verrà inserito ([[getCsvOutputModel]]);
   *  - esegue la scrittura dei CSV in una cartella temporanea ([[writeCsv]]);
   *  - comprime i CSV in file ZIP, che vengono scritti nella cartella di sincronizzazione ([[writeZip]]);
   *  - scrive l'esito della pubblicazione in una tabella di reportistica ([[writeInfoInTable]]).
   * @param df tabella dei consumi AGG/SBG
   */
  override def run(df: DataFrame): Unit = {
    val aggregato = getAggregato(df)
    val aggregatoFiltered = aggregato.filter(keyFields.map(f => col(f).isNotNull).reduce(_ && _))
    val aggregatoForCsv = convertColumnsToString(aggregatoFiltered).na.fill("")

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
   * Filtra i record da pubblicare, ed esegue delle operazioni di raggruppamento/pivot per il calcolo giornaliero (o aggregato) dei consumi.
   * Per maggiori info, vedere le singole specializzazioni di [[getAggregato]].
   * @param df dataframe dei consumi AGG/SBG
   * @return dataframe pronto per la pubblicazione su CSV
   */
  def getAggregato(df: DataFrame): DataFrame

  /**
   * Esegue la scrittura dei file CSV in una cartella temporanea, splittandoli per [[annoMese]] e utilizzando [[counterCsv]] come progressivo.
   * @param rddCsvOutput `RDD` in cui la chiave è una mappa (contiene le piva, l'annomese, il counterCsv), e i valori sono i record da pubblicare per quella chiave.
   * @param columnsField campi da pubblicare
   * @param optionalOperationName parametro utilizzato soltanto nella procedura di incoerenti SBG
   * @return un `RDD` che associa le pive [[mainPiva]] al percorso della cartella in cui sono stati scritti i file CSV
   */
  def writeCsv(rddCsvOutput: RDD[(Map[String, String], Row)], columnsField: List[String], optionalOperationName: Option[String] = None): RDD[(String, String)] = {
    val tmpCsvOutput = getTmpCsvOutput
    val publicationType = getPublicationType
    val sessionName = getSessionName
    val baseName = publicationType + baseNumber
    val daterun = convertStringTimestampToLocalDateTime(getDateToRun)

    //        FileUtility.setYarn777toTmpFolder(tmpCsvOutput)
    FileUtils.deleteDirectory(new File(tmpCsvOutput))
    rddCsvOutput.groupByKey().map({ case (mapKeys, rows) =>

      val countCsv = mapKeys(counterCsv)
      val am = mapKeys(annoMese)
      val path = tmpCsvOutput + getCsvOutputPath(baseName, mapKeys, daterun, publicationType, sessionName, countCsv, am, optionalOperationName)

      val records = rows.toList.map(row => {
        columnsField.map(column => {
          row.getAs[String](column)
        }).mkString(CSV_SEPARATOR)
      })

      val header = if (writeCsvHeader)
        publicationType match {
          case "AGG" => Some(columnsField.mkString(CSV_SEPARATOR))
          case "SBG" => Some(columnsField.map(_.toLowerCase).mkString(CSV_SEPARATOR))
        } else None

      FileUtility.writeCsv(path, header, records, appendMode = true)
      (mapKeys(mainPiva), path)
    }).groupByKey()
      .mapValues(values => values.head)
  }

  /**
   * Comprime i file CSV in file ZIP, scrivendoli nella cartella finale. Il processo aggiunge i CSV in uno ZIP finché quest'ultimo non supera la soglia determinata da [[getMaxSizeThresholdZip]];
   * dopodiché chiude lo ZIP ed esegue lo stesso processo con un nuovo ZIP.
   * @param rddCsvPath `RDD` contenente le piva di cui si devono creare gli ZIP e il percorso dove sono stati scritti i file CSV
   * @return `RDD` con le informazioni necessarie per popolare la tabella di reportistica:
   */
  def writeZip(rddCsvPath: RDD[(String, String)]): RDD[(String, String, String, String, Timestamp, Long)] = {
    val tmpCsvOutput = getTmpCsvOutput
    val pathZipOutput = getPathZipOutput
    val daterun = convertStringTimestampToLocalDateTime(getDateToRun)
    val maxDimensionZip = getMaxSizeThresholdZip.toLong
    val executionId = getExecutionId
    val timestampToRun = Timestamp.valueOf(daterun)
    val baseName = getPublicationType + baseNumber
    val publicationType = getPublicationType
    val sessionName = getSessionName
    val year = getYear

    val rddInfo = rddCsvPath.flatMap({ case (pivaHead, path) =>
      val pathInputFileCsv = new File(path)
      val outputFolder = pathInputFileCsv.getParent.replaceAll(tmpCsvOutput, pathZipOutput)
      var zipName = getZipOutputName(pivaHead, publicationType, sessionName, daterun, year)
      val originalZipName = zipName
      val outputFolderIfExists = new File(outputFolder)

      var count = 1

      /**
       * Determina se è stato possibile creare o meno il file ZIP. Le casistiche sono due:
       *  - se è possibile creare il file ZIP nella cartella [[outputFolder]], nella tabella di reportistica verranno inseriti il percorso e il nome dello ZIP;
       *  - se la cartella è inesistente o il processo non possiede i permessi di scrittura per quella cartella, nella tabella di reportistica verrà inserita la
       *  dicitura "Couldn't write to [...], the path does not exits or permission are not set properly.".
       *
       *  È importante che i nostri processo non creino cartelle nelle alberature gestite da AU
       *  (`/mnt/Settlement` in produzione e `/mnt/settlementSYN_parallelo/` in ambiente di test)
       */
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
        logger.warn(s"Couldn't write to $outputFolder, the path does not exits.")
        s"Couldn't write to ${outputFolder + originalZipName}, the path does not exits or permission are not set properly."
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

  /**
   * Inserisce il file [[name]] nello zip [[zip]].
   */
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

  /**
   * Ottiene il nome, comprensivo del percorso, del file CSV da scrivere.
   * @param baseName nome del processo (AGG/SBG) incluso il baseNumber (1,2,3,4,5)
   * @param mapKey mappa per l'estrazione delle partite ive
   * @param date data di lancio per la creazione del timestamp
   * @param publicationType nome del processo (AGG/SBG)
   * @param sessionName sessione di lancio (e.g. AGG_S1_PRE)
   * @param counterCsv progressivo da inserire nel nome del file CSV
   * @param annoMese anno-mese di riferimento del CSV
   * @param optionalOperationName (opzionale) corrisponde al nome dell'operazione, viene utilizzato soltanto in [[IncoerentiTraitSbg]]
   * @return il nome del file CSV da scrivere comprensivo del percorso
   */
  def getCsvOutputPath(baseName: String, mapKey: Map[String, String], date: LocalDateTime, publicationType: String, sessionName: String, counterCsv: String, annoMese: String, optionalOperationName: Option[String] = None): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaPathFolderHead = mapKey(keyFields.head)
    val pivaNameFile = keyFields.map(key => mapKey(key)).mkString("_")

    // Unfortunately, optionalOperationName is needed by incoerenti in SBG, when we write CONF/NOCONF CSVs. See getCsvOutputPath in IncoerentiTraitSbg for more details

    //es (AGG|SBG)1_0123456789/2022/04/0123456789_(AGG_S1_PRE|SBG)_202204_20220428105421_1.csv
    s"/${baseName}_$pivaPathFolderHead/$year/$month/${pivaNameFile}_${sessionName}_${annoMese}_${timestamp}_${counterCsv}.csv"
  }

  /**
   * Restituisce il nome dello ZIP da creare
   * @param pivaFolder partita iva principale
   * @param publicationType nome del processo (AGG/SBG)
   * @param sessionName sessione di lancio (e.g. AGG_S1_PRE)
   * @param today data di lancio per estrarre il timestamp
   * @param year anno (impostato da parametro) da inserire nel nome del file ZIP
   * @return il nome del file ZIP da creare
   */
  def getZipOutputName(pivaFolder: String, publicationType: String, sessionName: String, today: LocalDateTime, year: String): String = {
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    //es: 0123456789_(AGG|SBG)_2022(04)_20220428105421_1.zip
    val zipName = s"/${pivaFolder}_${sessionName}_${year}_${timestamp}_1.zip"
    zipName
  }

  /**
   * Scrive l'esito della pubblicazione nella tabella di reportistica.
   * @param rdd RDD contenente le informazioni da scrivere
   */
  def writeInfoInTable(rdd: RDD[(String, String, String, String, Timestamp, Long)]): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._
    rdd
      .toDF(InfoOutputSchema.getValues: _*)
      .repartition(10)
      .write.partitionBy(InfoOutputSchema.partition_date).mode(SaveMode.Append).parquet(getHdfsOutputBasepathInfoLog)
  }

  /**
   * Suddivide i record di [[aggFilter]] per CSV tramite la funzione [[distribution]], e crea un key-value RDD,
   * dove la chiave è una mappa che identifica un file CSV (piva, annomese, progressivo) e i valori sono i record da inserire
   * in quel CSV.
   * @param aggFilter tabella dei consumi filtrata
   * @param csvFields campi da inserire nel CSV
   * @return un key-value RDD in cui ogni chiave corrisponde a un file CSV da scrivere
   */
  def getCsvOutputModel(aggFilter: DataFrame, csvFields: List[String]): RDD[(Map[String, String], Row)] = {
    val keys = keyFields :+ annoMese :+ counterCsv

    val columns = ((csvFields :+ annoMese :+ counterCsv) ++ keyFields).distinct

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

  /**
   * Suddivide i record in [[df]] in un certo numero di CSV, in modo tale che ogni CSV abbia al più [[getCsvMaxRowLength]] righe.
   * @param df tabella dei consumi
   * @return [[df]] con un campo aggiuntivo, [[counterCsv]], che corrisponde al progressivo del CSV nel quale verrà inserito il record
   */
  def distribution(df: DataFrame): DataFrame = {
    val csvMaxNumberRow = getCsvMaxRowLength.get

    val window = Window.partitionBy((keyFields :+ annoMese).map(col): _*)
    df
      .withColumn(counterCsv, count("*").over(window))
      .withColumn(counterCsv, ((monotonically_increasing_id % ceil(col(counterCsv) / csvMaxNumberRow)) + 1).cast(StringType))
  }

  def convertStringTimestampToLocalDateTime(localDateTime: String): LocalDateTime = {
    Timestamp.valueOf(localDateTime).toLocalDateTime
  }

}
