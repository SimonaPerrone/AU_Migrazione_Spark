package it.eng.au.ammissibilitaSettlementGas.controller

import it.eng.au.ammissibilitaSettlementGas.args.ArgsFactory.logger
import it.eng.au.ammissibilitaSettlementGas.model.{QKRIUD, QKRIUDMetadata, ReportFileAmmissibilita, ReportMessage}
import it.eng.au.ammissibilitaSettlementGas.model.rules.{QKRIUDCsvRule, QKRIUDRecordRule}
import it.eng.au.ammissibilitaSettlementGas.schema.{AmmissibilitaCsvSchema, QKRIUDCsvSchema}
import it.eng.au.ammissibilitaSettlementGas.schema.AmmissibilitaCsvSchema._
import it.eng.au.ammissibilitaSettlementGas.utility.Constants._
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import it.eng.au.ammissibilitaSettlementGas.utility.file.FileUtility
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, explode, lit}
import org.apache.spark.sql.types.BooleanType
import org.joda.time.format.DateTimeFormat

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader, UnsupportedEncodingException}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.stream.Collectors
import scala.util.Try
import scala.util.matching.Regex

object QKRIUDController extends Serializable {
  private val QKRIUDFileRegex: Regex = "^([A-Za-z0-9]+)_(QKRIUD)_(\\d{6})_(\\d+)\\.(?i)([A-Za-z0-9]+)".r
  private var QKRIUDFieldIncorrect: Option[String] = None
  private var QKRIUDFieldMissing: Option[String] = None

  // Returns an RDD
  def readCsvFiles(): RDD[QKRIUDMetadata] = {
    val rootFolder = new File(Properties.getCsvInputPath)

    val settlement = rootFolder.listFiles.toList.filter(_.isDirectory)

    val years_rdd = Environment.spark.sparkContext.parallelize(settlement)

    val currentYear = Properties.getCurrentYear
    val currentMonth = Properties.getCurrentMonth

    val files_rdd = years_rdd.flatMap(_.listFiles().filter(f => f.isDirectory && f.getName == currentYear))
      .flatMap(_.listFiles().filter(f => f.isDirectory && f.getName == currentMonth))
      .flatMap(_.listFiles().filter(f => f.isFile && f.getName.contains("QKRIUD") && !f.getName.contains("AMM")))

    files_rdd.map(file => {
      val year = file.getParentFile.getParentFile.getName.toLowerCase
      val month = file.getParentFile.getName.toLowerCase
      file.getName match {
        case QKRIUDFileRegex(pivaRdb, qkriud, annoTermico, progressivo, tipoFile) =>
          QKRIUDMetadata(
            file = file,
            tipoFile = tipoFile,
            lastModified = file.lastModified(),
            yearDir = year,
            monthDir = month,
            pivaRdb = Some(pivaRdb),
            annoTermico = Some(annoTermico),
            csv = None,
            progressivo = Some(progressivo),
            isAmmissibile = true
          )
        case _ =>
          QKRIUDMetadata(
            file = file,
            lastModified = file.lastModified(),
            tipoFile = "",
            yearDir = year,
            monthDir = month,
            pivaRdb = Some(""),
            annoTermico = Some(""),
            csv = None,
            progressivo = Some(""),
            isAmmissibile = false,
            statusCode = COD_200,
            statusMessage = MOTIVAZIONE_FILENAME
          )
      }
    })
  }

  def checkQKRIUDCsv(QKRIUDMeta: QKRIUDMetadata, csvRules: List[QKRIUDCsvRule]): QKRIUDMetadata = {
    val okMessage = ReportMessage()
    val errorRule = csvRules.find(rule => rule.isEnabled && !rule.condition(QKRIUDMeta))
    val message = if (errorRule.isDefined) errorRule.get.message else okMessage

    QKRIUDMeta.copy(
      isAmmissibile = message.isAmmissibile,
      statusCode = message.statusCode,
      statusMessage = message.statusMessage
    )
  }

  def checkQKRIUDRecord(QKRIUDRecordMeta: QKRIUD, recordRules: List[QKRIUDRecordRule]): QKRIUD = {
    val okMessage = ReportMessage()
    val errorRule = recordRules.find(rule => rule.isEnabled && !rule.condition(QKRIUDRecordMeta))
    val message = if (errorRule.isDefined) errorRule.get.message else okMessage

    QKRIUDRecordMeta.copy(
      isAmmissibile = message.isAmmissibile,
      statusCode = message.statusCode,
      statusMessage = message.statusMessage
    )
  }

  def getQKRIUDListFromFile(file: File): Option[List[QKRIUD]] = {
    val buffSourceFile = scala.io.Source.fromFile(file)
    //bufSourceFiles.map(file => file.getLines().toList.sp)

    val rowsList = buffSourceFile.getLines().map(_.split(CSV_DELIMITER)).toList
    val rowsListWithIndex = rowsList.zipWithIndex

    val rowsListFiltered = rowsListWithIndex.filter(line => line._1.length > 1)

    val QKRIUDList = rowsListFiltered.slice(1, rowsList.length).map(line =>
      QKRIUD(fileName = file.getName, fields = line._1.toList, numeroRiga = line._2,
      data = getField(line._1, 0), codRemi = getField(line._1, 1), qkriud = getField(line._1, 2)
    ))

    if (QKRIUDList.length > 0) Some(QKRIUDList)
    else Some(List(QKRIUD(
      fileName = file.getName, numeroRiga = 1, fields = null, codRemi = Some(""), qkriud = Some("1.0"))))
  }

  def getField(csvFields: Array[String], index: Int): Option[String] = {
    csvFields.lift(index).filter(_.nonEmpty)
  }

  def writeCSVQKRIUDAmmFile(QKRIUDFilesAndRecordsChecked: RDD[QKRIUDMetadata]): RDD[ReportFileAmmissibilita] = {
    val daterun = Environment.startDateTime
    val year = Properties.getCurrentYear
    val month = Properties.getCurrentMonth
    val basePath = Properties.getCsvAmmissibilitaOutputPath
    val header = List(NUM_RIGA, COD_TIPO_FILE, PIVA_UTENTE, VERIFICA_AMM, COD_CAUSALE, MOTIVAZIONE).mkString(CSV_DELIMITER)


    val rdd = QKRIUDFilesAndRecordsChecked.map(qkriudMeta => {
      (qkriudMeta.file.getName, qkriudMeta.lastModified, qkriudMeta.pivaRdb, qkriudMeta.isAmmissibile, qkriudMeta.statusCode, qkriudMeta.statusMessage, qkriudMeta.csv)
    })

    val df = Environment.spark.createDataFrame(rdd).toDF("NOME_FILE", "LAST_MODIFIED", AmmissibilitaCsvSchema.PIVA_UTENTE,
      AmmissibilitaCsvSchema.VERIFICA_AMM, AmmissibilitaCsvSchema.COD_CAUSALE, AmmissibilitaCsvSchema.MOTIVAZIONE, "csv")

    //df.filter()
    val df1 = df.filter(col(AmmissibilitaCsvSchema.VERIFICA_AMM).cast(BooleanType) === false)
      .select(col(AmmissibilitaCsvSchema.PIVA_UTENTE), col(AmmissibilitaCsvSchema.VERIFICA_AMM),
      col("NOME_FILE"), col("LAST_MODIFIED"), col(AmmissibilitaCsvSchema.COD_CAUSALE), col(AmmissibilitaCsvSchema.MOTIVAZIONE))
      .withColumn(AmmissibilitaCsvSchema.NUM_RIGA, lit(""))
      .withColumn(AmmissibilitaCsvSchema.COD_TIPO_FILE, lit("QKRIUD"))
      .select(col("NOME_FILE"), col("LAST_MODIFIED"), col(AmmissibilitaCsvSchema.NUM_RIGA), col(AmmissibilitaCsvSchema.COD_TIPO_FILE),
        col(AmmissibilitaCsvSchema.PIVA_UTENTE), col(AmmissibilitaCsvSchema.VERIFICA_AMM), col(AmmissibilitaCsvSchema.COD_CAUSALE),
        col(AmmissibilitaCsvSchema.MOTIVAZIONE))

    val df2 = df.filter(col(AmmissibilitaCsvSchema.VERIFICA_AMM).cast(BooleanType) === true)
      .select(col(AmmissibilitaCsvSchema.PIVA_UTENTE), col("NOME_FILE"), col("LAST_MODIFIED"),
      explode(col("csv")).alias("explode"))
      .withColumn(AmmissibilitaCsvSchema.COD_TIPO_FILE, lit("QKRIUD"))
      .withColumn(AmmissibilitaCsvSchema.NUM_RIGA, col("explode.numeroRiga"))
      .withColumn(AmmissibilitaCsvSchema.VERIFICA_AMM, col("explode.isAmmissibile"))
      .withColumn(AmmissibilitaCsvSchema.COD_CAUSALE, col("explode.statusCode"))
      .withColumn(AmmissibilitaCsvSchema.MOTIVAZIONE, col("explode.statusMessage"))
      .select(col("NOME_FILE"), col("LAST_MODIFIED"), col(AmmissibilitaCsvSchema.NUM_RIGA), col(AmmissibilitaCsvSchema.COD_TIPO_FILE),
        col(AmmissibilitaCsvSchema.PIVA_UTENTE), col(AmmissibilitaCsvSchema.VERIFICA_AMM), col(AmmissibilitaCsvSchema.COD_CAUSALE),
        col(AmmissibilitaCsvSchema.MOTIVAZIONE))

    val resultDF = df1.union(df2).select(col("NOME_FILE"), col("LAST_MODIFIED"), col(AmmissibilitaCsvSchema.NUM_RIGA),
      col(AmmissibilitaCsvSchema.COD_TIPO_FILE), col(AmmissibilitaCsvSchema.PIVA_UTENTE), col(AmmissibilitaCsvSchema.VERIFICA_AMM),
      col(AmmissibilitaCsvSchema.COD_CAUSALE), col(AmmissibilitaCsvSchema.MOTIVAZIONE))

    val outputFolder = basePath //"src/test/resources/input/TSG2_10238291008/2021/10/fileProdotto/"

    resultDF.rdd.map(rddRow => {
      val nomeFile = rddRow.getAs[String](0)
      val lastModified = rddRow.getAs[Long](1)
      val numRiga = rddRow(2) //rddRow.getAs[Long](2) //FIXME: è un problema? Non funge il getAs.
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

      val ammissibilitaQKRIUD = ReportFileAmmissibilita(
        cartella_cloud = outputFolderPath,
        csv_file_name = nomeFile,
        last_modified = lastModified,
        ammissibilita_file_name = csvName
      )

      if (outputFolder.exists() && outputFolder.canWrite) {
        if (motivazione == MOTIVAZIONE_ALREADY_TRANSMITTED2)
          FileUtility.writeCsv(fullPath, header, List(record), isTmpFolder = false, appendMode = false)
        else FileUtility.writeCsv(fullPath, header, List(record), isTmpFolder = false, appendMode = true)
        ammissibilitaQKRIUD
      }
      else {
        logger.warn(s"Couldn't write to ${ammissibilitaQKRIUD.cartella_cloud}, the path doesn't exist or it's not writable")
        ammissibilitaQKRIUD.copy(cartella_cloud = s"Couldn't write to ${ammissibilitaQKRIUD.cartella_cloud}, the path doesn't exist or it's not writable")
      }
    })
  }

  def getAmmissibilitaCsvName(inputFileName: String, daterun: LocalDateTime, year: String, month: String): (String, String) = {
    val timestamp = daterun.format(DateTimeFormatter.ofPattern("yyyyMMdd"))

    val inputFileNameWithoutExtension = inputFileName.replace(".csv", "")

    (s"/TSG2_10238291008/$year/$month/", s"${inputFileNameWithoutExtension}_AMM_${timestamp}.csv")
  }

  def ruleCheckQKRIUDPivaRdb: QKRIUDCsvRule = QKRIUDCsvRule(
    ruleName = "ruleCheckQKRIUDPivaRdb",
    isEnabled = Properties.isRuleCheckQKRIUDPivaRdbEnabled,
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

  def ruleCheckQKRIUDFileExtension: QKRIUDCsvRule = QKRIUDCsvRule(
    ruleName = "ruleCheckQKRIUDFileExtension",
    isEnabled = Properties.isRuleCheckQKRIUDFileExtensionEnabled,
    condition = csv => {
      csv.tipoFile == "csv" || csv.tipoFile == "xml"
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_200,
      statusMessage = MOTIVAZIONE_FILE_EXTENSION
    )
  )

  def ruleCheckQKRIUDFileIntegrity: QKRIUDCsvRule = QKRIUDCsvRule(
    ruleName = "ruleCheckQKRIUDFileIntegrity",
    isEnabled = Properties.isRuleCheckQKRIUDFileIntegrityEnabled,
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

  def ruleCheckQKRIUDFileHeader: QKRIUDCsvRule = QKRIUDCsvRule(
    ruleName = "ruleCheckQKRIUDFileHeader",
    isEnabled = Properties.isRuleCheckQKRIUDFileHeaderEnabled,
    condition = csv => {
      val header = scala.io.Source.fromFile(csv.file).getLines().next()
      val splitHeader = header.toUpperCase.split(CSV_DELIMITER)
      val schema = QKRIUDCsvSchema.getValues

      (splitHeader.length == schema.length && schema.zipWithIndex.map({ case (value, index) => value.equals(Try(splitHeader(index)).getOrElse("")) }).min)
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_200,
      statusMessage = MOTIVAZIONE_FILE_HEADER
    )
  )

  def ruleCheckQKRIUDDate: QKRIUDRecordRule = QKRIUDRecordRule(
    ruleName = "ruleCheckQKRIUDDate",
    isEnabled = Properties.isRuleCheckQKRIUDDateEnabled,
    condition = record => {
      record.data.isDefined && record.data.nonEmpty
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_001,
      statusMessage = MOTIVAZIONE_DATE
    )
  )

  def ruleCheckQKRIUDDateConsistence: QKRIUDRecordRule = QKRIUDRecordRule(
    ruleName = "ruleCheckQKRIUDDateConsistence",
    isEnabled = Properties.isRuleCheckQKRIUDDateConsistenceEnabled,
    condition = record => {
      val dateArray = record.data.get.split("/")
      if (dateArray.length < 3) {
        false
      }
      else {
        val aaaamm = dateArray(2) + dateArray(1)
        aaaamm == record.fileName.split("_")(2)
      }
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_209,
      statusMessage = MOTIVAZIONE_DATE_MISMATCH
    )
  )

  def ruleCheckQKRIUDCodRemiMandatory: QKRIUDRecordRule = QKRIUDRecordRule(
    ruleName = "ruleCheckQKRIUDCodRemiMandatory",
    isEnabled = Properties.isRuleCheckQKRIUDcodRemiMandatoryEnabled,
    condition = record => {
      record.codRemi.isDefined && record.codRemi.nonEmpty
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_001,
      statusMessage = MOTIVAZIONE_COD_REMI_MANDATORY
    )
  )

  def ruleCheckQKRIUDfieldMandatory: QKRIUDRecordRule = QKRIUDRecordRule(
    ruleName = "ruleCheckQKRIUDfieldMandatory",
    isEnabled = Properties.isRuleCheckQKRIUDfieldMandatoryEnabled,
    condition = record => {
      record.qkriud.isDefined && record.qkriud.nonEmpty
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_001,
      statusMessage = MOTIVAZIONE_QKRIUD_MANDATORY
    )
  )

  def ruleCheckQKRIUDDateFormat: QKRIUDRecordRule = QKRIUDRecordRule(
    ruleName = "ruleCheckQKRIUDDateFormat",
    isEnabled = Properties.isRuleCheckQKRIUDDateFormatEnabled,
    condition = record => {
      isDateFormat(record.data)
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_224,
      statusMessage = MOTIVAZIONE_DATE_FORMAT
    )
  )

  def ruleCheckQKRIUDDateExistance: QKRIUDRecordRule = QKRIUDRecordRule(
    ruleName = "ruleCheckQKRIUDDateExistance",
    isEnabled = Properties.isRuleCheckQKRIUDDateExistanceEnabled,
    condition = record => {
      isDateReal(record.data)
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_224,
      statusMessage = MOTIVAZIONE_DATE_EXISTANCE
    )
  )

  def ruleCheckQKRIUDCodRemiFormat: QKRIUDRecordRule = QKRIUDRecordRule(
    ruleName = "ruleCheckQKRIUDCodRemiFormat",
    isEnabled = Properties.isRuleCheckQKRIUDCodRemiFormatEnabled,
    condition = record => {
      record.codRemi.isDefined && record.codRemi.nonEmpty && record.codRemi.get.matches("^[a-zA-Z0-9-]{1,14}$")
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_004,
      statusMessage = MOTIVAZIONE_COD_REMI_FORMAT
    )
  )

  def ruleCheckQKRIUDfieldFormat: QKRIUDRecordRule = QKRIUDRecordRule(
    ruleName = "ruleCheckQKRIUDfieldFormat",
    isEnabled = Properties.isRuleCheckQKRIUDfieldFormatEnabled,
    condition = record => {
      record.qkriud.isDefined && record.qkriud.nonEmpty && record.qkriud.get.matches("^-?[0-9]+\\.[0-9]+$")
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_004,
      statusMessage = MOTIVAZIONE_QKRIUD_FORMAT
    )
  )

  def ruleCheckCodRemiExistanceRcugas(rcugasTable: Broadcast[Set[Row]]): QKRIUDRecordRule = QKRIUDRecordRule(
    ruleName = "ruleCheckCodRemiExistanceRcugas",
    isEnabled = Properties.isRuleCheckCodRemiExistanceRcugasEnabled,
    condition = record => {
      rcugasExistance(record.codRemi.getOrElse(""), rcugasTable)
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_224,
      statusMessage = MOTIVAZIONE_COD_REMI_EXISTANCE
    )
  )

  def rcugasExistance(codRemi: String, rcugasTable: Broadcast[Set[Row]]): Boolean = {
    rcugasTable.value.exists(row => row.getString(0) == codRemi)
  }

  def isDateFormat(field: Option[String]): Boolean = {
    val dateRegex = """^\d{2}/\d{2}/\d{4}$""".r
    field match {
      case Some(value) => dateRegex.pattern.matcher(value).matches()
      case None => false
    }
  }

  def isDateReal(field: Option[String]): Boolean = {
    val formatter = DateTimeFormat.forPattern(DATA_COMP_FORMAT)
    field.exists { f =>
      Try {
        val dt = formatter.parseDateTime(f)

        // prendo i pezzi dalla stringa
        val Array(dd, mm, yyyy) = f.split("/")

        // confronto con i valori realmente parsati
        dt.getDayOfMonth == dd.toInt &&
          dt.getMonthOfYear == mm.toInt &&
          dt.getYear == yyyy.toInt
      }.getOrElse(false)
    }
  }
}
