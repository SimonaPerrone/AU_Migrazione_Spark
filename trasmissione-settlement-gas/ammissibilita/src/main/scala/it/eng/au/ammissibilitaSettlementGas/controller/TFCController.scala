package it.eng.au.ammissibilitaSettlementGas.controller

import it.eng.au.ammissibilitaSettlementGas.args.ArgsFactory.logger
import it.eng.au.ammissibilitaSettlementGas.model.rules.{TFCCsvRule, TFCRecordRule}
import it.eng.au.ammissibilitaSettlementGas.model.{ReportFileAmmissibilita, ReportMessage, TFC, TFCMetadata}
import it.eng.au.ammissibilitaSettlementGas.schema.AmmissibilitaCsvSchema._
import it.eng.au.ammissibilitaSettlementGas.schema.{AmmissibilitaCsvSchema, TFCCsvSchema}
import it.eng.au.ammissibilitaSettlementGas.utility.Constants._
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import it.eng.au.ammissibilitaSettlementGas.utility.file.FileUtility
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, explode, lit}
import org.apache.spark.storage.StorageLevel
import org.joda.time.format.DateTimeFormat

import java.io._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.stream.Collectors
import scala.util.Try
import scala.util.matching.Regex

object TFCController extends Serializable {
  private val TFCFileRegex: Regex = "^([A-Za-z0-9]+)_(TFC)_(\\d{6})_(\\d+)\\.(?i)([A-Za-z0-9]+)".r
  //private val TFCFileRegex: Regex = "^([A-Za-z0-9]+)_(TFC)_(\\d{6})_(\\d+)\\.(?i)zip".r

  def getTFCListFromFile(tfcMeta:TFCMetadata): Option[List[TFC]] = {
    val buffSourceFile = scala.io.Source.fromFile(tfcMeta.file)
    //bufSourceFiles.map(file => file.getLines().toList.sp)

    val rowsList = buffSourceFile.getLines().map(_.split(CSV_DELIMITER)).toList
    val rowsListWithIndex = rowsList.zipWithIndex
    val rowsListFiltered = rowsListWithIndex.filter(line => line._1.length > 1)

    val TFCList = rowsListFiltered.slice(1, rowsList.length).map(line => TFC(fileName = tfcMeta.file.getName, tfcMeta.yearDir, tfcMeta.monthDir, line._2, fields = line._1, data = getField(line._1,0),idRegClimatica = getField(line._1,1),wkr = getField(line._1,2)))
    if (TFCList.length > 0) Some(TFCList) else Some(List(TFC(fileName = tfcMeta.file.getName, tfcMeta.yearDir, tfcMeta.monthDir, 1, null, None, None, None)))
  }

  def getField(csvFields: Array[String], index: Int): Option[String] = {
    csvFields.lift(index).filter(_.nonEmpty)
  }



  // Returns an RDD
  def readCsvFiles(): (RDD[TFCMetadata], String, String) = {
    val rootFolder = new File(Properties.getCsvInputPath)

    val settlement = rootFolder.listFiles.toList.filter(_.isDirectory)

    val years_rdd = Environment.spark.sparkContext.parallelize(settlement)

    val currentYear = Properties.getCurrentYear
    val currentMonth = Properties.getCurrentMonth

    val files_rdd = years_rdd.flatMap(_.listFiles().filter(f => f.isDirectory && f.getName == currentYear)) // && f.getName == "2020"))
      .flatMap(_.listFiles().filter(f => f.isDirectory && f.getName == currentMonth))
      .flatMap(_.listFiles().filter(f => f.isFile && !f.getName.contains("VPG") && !f.getName.contains("QKRIUD") && !f.getName.contains("AMM")))

      val TFCFilesWithMeta = files_rdd.map(file => {
        val year = file.getParentFile.getParentFile.getName.toLowerCase
        val month = file.getParentFile.getName.toLowerCase
        file.getName match {
          case TFCFileRegex(pivaRdb, tfc, annoMese, progressivo, tipoFile) =>
            TFCMetadata(
              file = file,
              tipoFile = tipoFile,
              lastModified = file.lastModified(),
              yearDir = year,
              monthDir = month,
              pivaRdb = Some(pivaRdb),
              annoMese = Some(annoMese),
              csv = None,
              progressivo = Some(progressivo),
              isAmmissibile = true
            )
          case _ =>
            TFCMetadata(
              file = file,
              lastModified = file.lastModified(),
              tipoFile = "",
              yearDir = year,
              monthDir = month,
              pivaRdb = None,
              annoMese = None,
              csv = None,
              progressivo = None,
              isAmmissibile = false,
              statusCode = COD_200,
              statusMessage = MOTIVAZIONE_FILENAME
            )
        }
      })

    val yearMonthList = TFCFilesWithMeta.filter(_.isAmmissibile).map(_.annoMese.get).persist(StorageLevel.MEMORY_AND_DISK)

    if (yearMonthList.isEmpty) (TFCFilesWithMeta, YEARMONTH_MIN, YEARMONTH_MAX)
    else (TFCFilesWithMeta, yearMonthList.min, yearMonthList.max)


  }


  def checkTFCCsv(TFCMeta: TFCMetadata, csvRules: List[TFCCsvRule]): TFCMetadata = {
    val okMessage = ReportMessage()
    val errorRule = csvRules.find(rule => rule.isEnabled && !rule.condition(TFCMeta))
    val message = if (errorRule.isDefined) errorRule.get.message else okMessage

    TFCMeta.copy(
      isAmmissibile = message.isAmmissibile,
      statusCode = message.statusCode,
      statusMessage = message.statusMessage
    )
  }

  def checkTFCRecord(TFCRecordMeta:TFC, recordRules:List[TFCRecordRule]): TFC = {
    val okMessage = ReportMessage()
    val errorRule = recordRules.find(rule => rule.isEnabled && !rule.condition(TFCRecordMeta))
    val message = if (errorRule.isDefined) errorRule.get.message else okMessage

    TFCRecordMeta.copy(
      isAmmissibile = message.isAmmissibile,
      statusCode = message.statusCode,
      statusMessage = message.statusMessage
    )
  }

  def writeCSVTFCAmmFile(TFCFilesAndRecordsChecked: RDD[TFCMetadata]): RDD[ReportFileAmmissibilita] = {
    val daterun = Environment.startDateTime
    val year = Properties.getCurrentYear
    val month = Properties.getCurrentMonth
    val basePath = Properties.getCsvAmmissibilitaOutputPath
    val header = List(NUM_RIGA, COD_TIPO_FILE, PIVA_UTENTE, VERIFICA_AMM, COD_CAUSALE, MOTIVAZIONE).mkString(CSV_DELIMITER)


    val rdd = TFCFilesAndRecordsChecked.map(tfcMeta => {(tfcMeta.file.getName, tfcMeta.lastModified, tfcMeta.pivaRdb, tfcMeta.isAmmissibile, tfcMeta.statusCode, tfcMeta.statusMessage, tfcMeta.csv)})

    val df = Environment.spark.createDataFrame(rdd).toDF("NOME_FILE", "LAST_MODIFIED", AmmissibilitaCsvSchema.PIVA_UTENTE,
      AmmissibilitaCsvSchema.VERIFICA_AMM, AmmissibilitaCsvSchema.COD_CAUSALE, AmmissibilitaCsvSchema.MOTIVAZIONE, "csv")

    val df1 = df.filter(col(AmmissibilitaCsvSchema.VERIFICA_AMM) === "false" ||
      col(AmmissibilitaCsvSchema.VERIFICA_AMM) === false)
      .select(col(AmmissibilitaCsvSchema.PIVA_UTENTE), col(AmmissibilitaCsvSchema.VERIFICA_AMM),
      col("NOME_FILE"), col("LAST_MODIFIED"), col(AmmissibilitaCsvSchema.COD_CAUSALE), col(AmmissibilitaCsvSchema.MOTIVAZIONE))
      .withColumn(AmmissibilitaCsvSchema.NUM_RIGA, lit(""))
      .withColumn(AmmissibilitaCsvSchema.COD_TIPO_FILE, lit("TFC"))
      .select(col("NOME_FILE"), col("LAST_MODIFIED"), col(AmmissibilitaCsvSchema.NUM_RIGA), col(AmmissibilitaCsvSchema.COD_TIPO_FILE),
        col(AmmissibilitaCsvSchema.PIVA_UTENTE), col(AmmissibilitaCsvSchema.VERIFICA_AMM), col(AmmissibilitaCsvSchema.COD_CAUSALE),
        col(AmmissibilitaCsvSchema.MOTIVAZIONE))

    val df2 = df.filter(col(AmmissibilitaCsvSchema.VERIFICA_AMM) === true).select( col(AmmissibilitaCsvSchema.PIVA_UTENTE), col("NOME_FILE"), col("LAST_MODIFIED"),
      explode(col("csv")).alias("explode"))
      .withColumn(AmmissibilitaCsvSchema.COD_TIPO_FILE, lit("TFC"))
      .withColumn(AmmissibilitaCsvSchema.NUM_RIGA, col("explode.numeroRiga"))
      .withColumn(AmmissibilitaCsvSchema.VERIFICA_AMM, col("explode.isAmmissibile"))
      .withColumn(AmmissibilitaCsvSchema.COD_CAUSALE, col("explode.statusCode"))
      .withColumn(AmmissibilitaCsvSchema.MOTIVAZIONE, col("explode.statusMessage"))
      .select(col("NOME_FILE"), col("LAST_MODIFIED"), col(AmmissibilitaCsvSchema.NUM_RIGA), col(AmmissibilitaCsvSchema.COD_TIPO_FILE),
        col(AmmissibilitaCsvSchema.PIVA_UTENTE), col(AmmissibilitaCsvSchema.VERIFICA_AMM), col(AmmissibilitaCsvSchema.COD_CAUSALE),
        col(AmmissibilitaCsvSchema.MOTIVAZIONE))

    val resultDF = df1.union(df2)
      .select(col("NOME_FILE"), col("LAST_MODIFIED"), col(AmmissibilitaCsvSchema.NUM_RIGA), col(AmmissibilitaCsvSchema.COD_TIPO_FILE),
      col(AmmissibilitaCsvSchema.PIVA_UTENTE), col(AmmissibilitaCsvSchema.VERIFICA_AMM), col(AmmissibilitaCsvSchema.COD_CAUSALE),
      col(AmmissibilitaCsvSchema.MOTIVAZIONE))


    //val outputFolder = basePath //"src/test/resources/input/TSG2_10238291008/2021/10/fileProdotto/"

    // TODO: verificare che il getAs sia giusto.
    resultDF.rdd.map(rddRow => {
      val nomeFile = rddRow.getAs[String](0)
      val lastModified = rddRow(1).asInstanceOf[Long]//rddRow.getAs[Long](1)
      val numRiga = rddRow.getAs[String](2)//.asInstanceOf[Int]//rddRow.getAs[Long](2)
      val codTipoFile = rddRow.getAs[String](3)
      val pivaUtente = rddRow.getAs[String](4)
      val verificaAmm = if (rddRow.getAs[Boolean](5)) "Y" else "N"
      val codCausale = rddRow.getAs[String](6)
      val motivazione = rddRow.getAs[String](7)

      val record = List(numRiga, codTipoFile, pivaUtente, verificaAmm, codCausale, motivazione).mkString(CSV_DELIMITER)

      val (middlePath, csvName) = getAmmissibilitaCsvName(nomeFile, daterun, year, month)
      val outputFolderPath = basePath + middlePath
      val fullPath = outputFolderPath + csvName
      val outputFolder = new File(outputFolderPath)

      val ammissibilitaTFC = ReportFileAmmissibilita(
        cartella_cloud = outputFolderPath,
        csv_file_name  = nomeFile,
        last_modified = lastModified,
        ammissibilita_file_name = csvName
      )

      if (outputFolder.exists() && outputFolder.canWrite) {
        if (motivazione == MOTIVAZIONE_ALREADY_TRANSMITTED)
          FileUtility.writeCsv(fullPath, header, List(record), isTmpFolder = false, appendMode = false)
        else FileUtility.writeCsv(fullPath, header, List(record), isTmpFolder = false, appendMode = true)
        ammissibilitaTFC
      }
      else {
        logger.warn(s"Couldn't write to ${ammissibilitaTFC.cartella_cloud}, the path doesn't exist or it's not writable")
        ammissibilitaTFC.copy(cartella_cloud = s"Couldn't write to ${ammissibilitaTFC.cartella_cloud}, the path doesn't exist or it's not writable")
      }

      //FileUtility.writeCsv(fullPath, header, List(record), isTmpFolder = false, appendMode = true)

      //println(record)
    })


  }

  def getAmmissibilitaCsvName(inputFileName: String, daterun: LocalDateTime, year: String, month: String): (String, String) = {
    val timestamp = daterun.format(DateTimeFormatter.ofPattern("yyyyMMdd"))

    val inputFileNameWithoutExtension = inputFileName.replace(".csv", "")

    (s"/TSG2_10238291008/$year/$month/", s"${inputFileNameWithoutExtension}_AMM_${timestamp}.csv")
  }

  // Controlli a livello FILE.
  def ruleCheckTFCFileName: TFCCsvRule = TFCCsvRule(
    ruleName = "ruleCheckTFCFileName",
    isEnabled = Properties.isRuleCheckTFCFileNameEnabled,
    condition = csv => {
      val fileName = csv.file.getName()

      fileName.matches(".*_TFC_.*_.*.csv")
      },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_200,
      statusMessage = MOTIVAZIONE_FILENAME
    )
  )

  def ruleCheckTFCPivaRdb: TFCCsvRule = TFCCsvRule(
    ruleName = "ruleCheckTFCPivaRdb",
    isEnabled = Properties.isRuleCheckTFCPivaRdbEnabled,
    condition = csv => {
      val fileName = csv.file.getName()

      fileName.split("_")(0).equals(PIVA_SNAM)
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_209,
      statusMessage = MOTIVAZIONE_PIVA_RDD
    )
  )

  def ruleCheckTFCFileHeader: TFCCsvRule = TFCCsvRule(
    ruleName = "ruleCheckTFCFileHeader",
    isEnabled = Properties.isRuleCheckTFCFileHeaderEnabled,
    condition = csv => {
      val header = scala.io.Source.fromFile(csv.file).getLines().next()
      val splitHeader = header.toUpperCase.split(CSV_DELIMITER)
      val schema = TFCCsvSchema.getValues

      (splitHeader.length == schema.length && schema.zipWithIndex.map({ case (value, index) => value.equals(Try(splitHeader(index)).getOrElse("")) }).min)
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_200,
      statusMessage = MOTIVAZIONE_FILE_HEADER
    )
  )

  def ruleCheckTFCAlreadyTransmitted: TFCCsvRule = TFCCsvRule(
    ruleName = "ruleCheckTFCAlreadyTransmitted",
    isEnabled = Properties.isRuleAlreadyTransmittedTFCEnabled,
    condition = csv => !csv.isAlreadyTransmitted,
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_209,
      statusMessage = MOTIVAZIONE_ALREADY_TRANSMITTED
    )
  )

  def ruleCheckTFCFileExtension: TFCCsvRule = TFCCsvRule(
    ruleName = "ruleCheckTFCFileExtension",
    isEnabled = Properties.isRuleCheckTFCFileExtensionEnabled,
    condition = csv => {
      csv.tipoFile == "csv" || csv.tipoFile == "xml"
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_200,
      statusMessage = MOTIVAZIONE_FILE_EXTENSION
    )
  )

  def ruleCheckTFCFileIntegrity: TFCCsvRule = TFCCsvRule(
    ruleName = "ruleCheckTFCFileIntegrity",
    isEnabled = Properties.isRuleCheckTFCFileIntegrityEnabled,
    condition = csv => try {
      val csvReader = new InputStreamReader(new FileInputStream(csv.file), "UTF-8")
      val bufferedReader = new BufferedReader(csvReader)
      val lines = bufferedReader.lines().collect(Collectors.joining)

      // The following condition check if \uFFFD (replacement character for values unknown in Unicode) is not present in the lines read
      // and if \uFEFF (Byte Order Mark) is not present in position 0 of the file
      (lines.indexOf('\uFFFD') == -1) && (lines.indexOf('\uFEFF') != 0)
    } catch {
      case _: UnsupportedEncodingException => false
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_200,
      statusMessage = MOTIVAZIONE_FILE_INTEGRITY
    )
  )
  // Il seguente controllo è indicato a livello di record nel File di Analisi, di seguito la proposta a livello di file.

  def ruleCheckTFCFieldsNumber_FileLevel: TFCCsvRule = TFCCsvRule(
    ruleName = "ruleCheckTFCFieldsNumber",
    isEnabled = Properties.isRuleCheckTFCFieldsNumberEnabled,
    condition = csv => {

      val buffSourceFile = scala.io.Source.fromFile(csv.file)
      val linesIterator = buffSourceFile.getLines().map(line => line.split(";"))

      val schema = TFCCsvSchema.getValues

      linesIterator.map(line => (line.length == schema.length )).reduce(_ && _)
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_001,
      statusMessage = MOTIVAZIONE_FIELDS_NUMBER
    )
  )

  // Controlli a livello RECORD

  def ruleCheckTFCDate: TFCRecordRule = TFCRecordRule(
    ruleName = "ruleCheckTFCDate",
    isEnabled = Properties.isRuleCheckTFCDateEnabled,
    condition = record => {
      record.data.isDefined && record.data.nonEmpty
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_004,
      statusMessage = MOTIVAZIONE_DATE
    )
  )

  def ruleCheckTFCDateFormat: TFCRecordRule = TFCRecordRule(
    ruleName = "ruleCheckTFCDateFormat",
    isEnabled = Properties.isRuleCheckTFCDateFormatEnabled,
    condition = record => {
      isDate(record.data)
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_224,
      statusMessage = MOTIVAZIONE_DATE_FORMAT
    )
  )

  def ruleCheckTFCDateConsistence: TFCRecordRule = TFCRecordRule(
    ruleName = "ruleCheckTFCDateConsistence",
    isEnabled = Properties.isRuleCheckTFCDateConsistenceEnabled,
    condition = record => {
      val dateArray = record.data.get.split("/")
      val aaaamm = dateArray(2) + dateArray(1)

      aaaamm == record.fileName.split("_")(2)
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_209,
      statusMessage = MOTIVAZIONE_DATE_MISMATCH
    )
  )

  def ruleCheckTFCRegClimMandatory: TFCRecordRule = TFCRecordRule(
    ruleName = "ruleCheckTFCRegClimMandatory",
    isEnabled = Properties.isRuleCheckTFCregClimMandatoryEnabled,
    condition = record => {
      record.idRegClimatica.isDefined && record.idRegClimatica.nonEmpty
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_004,
      statusMessage = MOTIVAZIONE_ID_REG_CLIM_MANDATORY
    )
  )

  def ruleCheckTFCRegClimMismatch: TFCRecordRule = TFCRecordRule(
    ruleName = "ruleCheckTFCRegClimMismatch",
    isEnabled = Properties.isRuleCheckTFCRegClimMismatchEnabled,
    condition = record => {
      ID_REG_CLIM_VALUES.contains(record.idRegClimatica.get)
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_004,
      statusMessage = MOTIVAZIONE_ID_REG_CLIM_COMPLIANCE
    )
  )

  def ruleCheckTFCWKRDefined: TFCRecordRule = TFCRecordRule(
    ruleName = "ruleCheckTFCWKRDefined",
    isEnabled = Properties.isRuleCheckTFCWKRDefinedEnabled,
    condition = record => {
      record.wkr.isDefined && record.wkr.nonEmpty
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_004,
      statusMessage = MOTIVAZIONE_WKR_DEFINED
    )
  )

  def ruleCheckTFCWKRFormat: TFCRecordRule = TFCRecordRule(
    ruleName = "ruleCheckTFCWKRFormat",
    isEnabled = Properties.isRuleCheckTFCWKRFormatEnabled,
    condition = record => {
      case class ParseOp[T](op: String => T)
      implicit val popDouble = ParseOp[Double](_.toDouble)
      implicit val popInt = ParseOp[Int](_.toInt)
      // etc.
      def parse[T: ParseOp](s: String) = try { Some(implicitly[ParseOp[T]].op(s)) }
      catch {case _ => None}

      record.wkr.isDefined && record.wkr.get.contains(".") && parse[Double](record.wkr.get).isDefined
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_004,
      statusMessage = MOTIVAZIONE_WKR_FORMAT
    )
  )

  def ruleCheckTFCFieldsNumber: TFCRecordRule = TFCRecordRule(
    ruleName = "ruleCheckTFCFieldsNumber",
    isEnabled = Properties.isRuleCheckTFCFieldsNumberEnabled,
    condition = record => {
      val schema = TFCCsvSchema.getValues
      record.fields.length == schema.length
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_001,
      statusMessage = MOTIVAZIONE_FIELDS_NUMBER
    )
  )



  def isDate(field: Option[String]): Boolean = {
    val format = DateTimeFormat.forPattern(DATA_COMP_FORMAT)
    Try(format.parseDateTime(field.get)).isSuccess
  }







}
