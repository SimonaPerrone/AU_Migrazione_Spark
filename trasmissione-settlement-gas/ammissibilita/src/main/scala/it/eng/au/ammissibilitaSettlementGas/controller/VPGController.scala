package it.eng.au.ammissibilitaSettlementGas.controller

import it.eng.au.ammissibilitaSettlementGas.args.ArgsFactory.logger
import it.eng.au.ammissibilitaSettlementGas.controller.TFCController.{getAmmissibilitaCsvName, getField, isDate}
import it.eng.au.ammissibilitaSettlementGas.model.rules.{VPGCsvRule, VPGRecordRule}
import it.eng.au.ammissibilitaSettlementGas.model.{ReportFileAmmissibilita, ReportMessage, VPG, VPGMetadata}
import it.eng.au.ammissibilitaSettlementGas.schema.AmmissibilitaCsvSchema._
import it.eng.au.ammissibilitaSettlementGas.schema.{AmmissibilitaCsvSchema, VPGCsvSchema}
import it.eng.au.ammissibilitaSettlementGas.utility.Constants._
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import it.eng.au.ammissibilitaSettlementGas.utility.file.FileUtility
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, explode, lit}

import java.io._
import java.util.stream.Collectors
import scala.util.Try
import scala.util.matching.Regex

object VPGController extends Serializable {
  private val VPGFileRegex: Regex = "^([A-Za-z0-9]+)_(VPG)_(\\d{8})_(\\d+)\\.(?i)([A-Za-z0-9]+)".r
  private var VPGFieldIncorrect: Option[String] = None
  private var VPGFieldMissing: Option[String] = None

  def readCsvFiles(): RDD[VPGMetadata] = {
    val rootFolder = new File(Properties.getCsvInputPath)

    val settlement = rootFolder.listFiles.toList.filter(_.isDirectory)

    val years_rdd = Environment.spark.sparkContext.parallelize(settlement)

    val currentYear = Properties.getCurrentYear
    val currentMonth = Properties.getCurrentMonth

    val files_rdd = years_rdd.flatMap(_.listFiles().filter(f => f.isDirectory && f.getName == currentYear)) // && f.getName == "2020"))
      .flatMap(_.listFiles().filter(f => f.isDirectory && f.getName == currentMonth))
      .flatMap(_.listFiles().filter(f => f.isFile && f.getName.contains("VPG") && !f.getName.contains("AMM")))

    files_rdd.map(file => {
      val year = file.getParentFile.getParentFile.getName.toLowerCase
      val month = file.getParentFile.getName.toLowerCase
      file.getName match {
        case VPGFileRegex(pivaRdb, tfc, annoTermico, progressivo, tipoFile) =>
          VPGMetadata(
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
          VPGMetadata(
            file = file,
            lastModified = file.lastModified(),
            tipoFile = "",
            yearDir = year,
            monthDir = month,
            pivaRdb = Some(""),
            annoTermico = Some(""),
            csv=None,
            progressivo = Some(""),
            isAmmissibile = false,
            statusCode = COD_200,
            statusMessage = MOTIVAZIONE_FILENAME
          )
      }
    })
  }


  def getVPGListFromFile(file:File): Option[List[VPG]] = {
    val buffSourceFile = scala.io.Source.fromFile(file)
    //bufSourceFiles.map(file => file.getLines().toList.sp)

    val rowsList = buffSourceFile.getLines().map(_.split(CSV_DELIMITER)).toList
    val rowsListWithIndex = rowsList.zipWithIndex

    val rowsListFiltered = rowsListWithIndex.filter(line => line._1.length > 1)

    val VPGList = rowsListFiltered.slice(1, rowsList.length).map(line => VPG(fileName = file.getName, fields = line._1.toList, numeroRiga = line._2,
      giornoRiferimento = getField(line._1,0),C1A1 = getField(line._1,1),C1B1 = getField(line._1,2),C1C1 = getField(line._1,3),
      C1D1 = getField(line._1,4),C1E1 = getField(line._1,5),C1F1 = getField(line._1,6),C1A2 = getField(line._1,7),
      C1B2 = getField(line._1,8),C1C2 = getField(line._1,9),C1D2 = getField(line._1,10),C1E2 = getField(line._1,11),
      C1F2 = getField(line._1,12),C1A3 = getField(line._1,13),C1B3 = getField(line._1,14),C1C3 = getField(line._1,15),
      C1D3 = getField(line._1,16),C1E3 = getField(line._1,17),C1F3 = getField(line._1,18),C2 = getField(line._1,19),
      C4 = getField(line._1,20),T11 = getField(line._1,21),T12 = getField(line._1,22),T13 = getField(line._1,23)))

    if (VPGList.length > 0) Some(VPGList) else Some(List(VPG(fileName = file.getName, numeroRiga = 1, fields = null)))
  }



  def checkVPGCsv(VPGMeta: VPGMetadata, csvRules: List[VPGCsvRule]): VPGMetadata = {
    val okMessage = ReportMessage()
    val errorRule = csvRules.find(rule => rule.isEnabled && !rule.condition(VPGMeta))
    val message = if (errorRule.isDefined) errorRule.get.message else okMessage

    VPGMeta.copy(
      isAmmissibile = message.isAmmissibile,
      statusCode = message.statusCode,
      statusMessage = message.statusMessage
    )
  }

  def checkVPGRecord(VPGRecordMeta:VPG, recordRules:List[VPGRecordRule]): VPG = {
    val okMessage = ReportMessage()
    val errorRule = recordRules.find(rule => rule.isEnabled && !rule.condition(VPGRecordMeta))

    val message = if (errorRule.isDefined) {
      if (errorRule.get.ruleName == "ruleCheckVPGFieldsFormat")
        ReportMessage(
          isAmmissibile = false,
          statusCode = COD_004,
          statusMessage = MANDATORY_ERROR + " (" + VPGFieldIncorrect.get + " non corretto)"
        )
      else if (errorRule.get.ruleName == "ruleCheckVPGMandatoryFields")
        ReportMessage(
          isAmmissibile = false,
          statusCode = COD_004,
          statusMessage = MANDATORY_ERROR + " (" + VPGFieldMissing.get + " non presente)"
        )
      else errorRule.get.message
    }
    else okMessage

    VPGRecordMeta.copy(
      isAmmissibile = message.isAmmissibile,
      statusCode = message.statusCode,
      statusMessage = message.statusMessage
    )
  }


  def writeCSVVPGAmmFile(VPGFilesAndRecordsChecked: RDD[VPGMetadata]): RDD[ReportFileAmmissibilita] = {
    val daterun = Environment.startDateTime
    val year = Properties.getCurrentYear
    val month = Properties.getCurrentMonth
    val basePath = Properties.getCsvAmmissibilitaOutputPath
    val header = List(NUM_RIGA, COD_TIPO_FILE, PIVA_UTENTE, VERIFICA_AMM, COD_CAUSALE, MOTIVAZIONE).mkString(CSV_DELIMITER)


    val rdd = VPGFilesAndRecordsChecked.map(vpgMeta => {(vpgMeta.file.getName, vpgMeta.lastModified, vpgMeta.pivaRdb, vpgMeta.isAmmissibile, vpgMeta.statusCode, vpgMeta.statusMessage, vpgMeta.csv)})

    val df = Environment.spark.createDataFrame(rdd).toDF("NOME_FILE", "LAST_MODIFIED", AmmissibilitaCsvSchema.PIVA_UTENTE,
      AmmissibilitaCsvSchema.VERIFICA_AMM, AmmissibilitaCsvSchema.COD_CAUSALE, AmmissibilitaCsvSchema.MOTIVAZIONE, "csv")

    //df.filter()
    val df1 = df.filter(col(AmmissibilitaCsvSchema.VERIFICA_AMM) === "false" ||
      col(AmmissibilitaCsvSchema.VERIFICA_AMM) === false).select(col(AmmissibilitaCsvSchema.PIVA_UTENTE), col(AmmissibilitaCsvSchema.VERIFICA_AMM),
      col("NOME_FILE"), col("LAST_MODIFIED"), col(AmmissibilitaCsvSchema.COD_CAUSALE), col(AmmissibilitaCsvSchema.MOTIVAZIONE))
      .withColumn(AmmissibilitaCsvSchema.NUM_RIGA, lit(""))
      .withColumn(AmmissibilitaCsvSchema.COD_TIPO_FILE, lit("VPG"))
      .select(col("NOME_FILE"), col("LAST_MODIFIED"), col(AmmissibilitaCsvSchema.NUM_RIGA), col(AmmissibilitaCsvSchema.COD_TIPO_FILE),
        col(AmmissibilitaCsvSchema.PIVA_UTENTE), col(AmmissibilitaCsvSchema.VERIFICA_AMM), col(AmmissibilitaCsvSchema.COD_CAUSALE),
        col(AmmissibilitaCsvSchema.MOTIVAZIONE))

    val df2 = df.filter(col(AmmissibilitaCsvSchema.VERIFICA_AMM) === true).select( col(AmmissibilitaCsvSchema.PIVA_UTENTE), col("NOME_FILE"), col("LAST_MODIFIED"),
      explode(col("csv")).alias("explode"))
      .withColumn(AmmissibilitaCsvSchema.COD_TIPO_FILE, lit("VPG"))
      .withColumn(AmmissibilitaCsvSchema.NUM_RIGA, col("explode.numeroRiga"))
      .withColumn(AmmissibilitaCsvSchema.VERIFICA_AMM, col("explode.isAmmissibile"))
      .withColumn(AmmissibilitaCsvSchema.COD_CAUSALE, col("explode.statusCode"))
      .withColumn(AmmissibilitaCsvSchema.MOTIVAZIONE, col("explode.statusMessage"))
      .select(col("NOME_FILE"), col("LAST_MODIFIED"), col(AmmissibilitaCsvSchema.NUM_RIGA), col(AmmissibilitaCsvSchema.COD_TIPO_FILE),
        col(AmmissibilitaCsvSchema.PIVA_UTENTE), col(AmmissibilitaCsvSchema.VERIFICA_AMM), col(AmmissibilitaCsvSchema.COD_CAUSALE),
        col(AmmissibilitaCsvSchema.MOTIVAZIONE))

    val resultDF = df1.union(df2).select(col("NOME_FILE"), col("LAST_MODIFIED"), col(AmmissibilitaCsvSchema.NUM_RIGA), col(AmmissibilitaCsvSchema.COD_TIPO_FILE),
      col(AmmissibilitaCsvSchema.PIVA_UTENTE), col(AmmissibilitaCsvSchema.VERIFICA_AMM), col(AmmissibilitaCsvSchema.COD_CAUSALE),
      col(AmmissibilitaCsvSchema.MOTIVAZIONE))


    val outputFolder = basePath //"src/test/resources/input/TSG2_10238291008/2021/10/fileProdotto/"

    resultDF.rdd.map(rddRow => {
      val nomeFile = rddRow.getAs[String](0)
      val lastModified = rddRow.getAs[Long](1)
      val numRiga = rddRow(2)//rddRow.getAs[Long](2) //FIXME: è un problema? Non funge il getAs.
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

      val ammissibilitaVPG = ReportFileAmmissibilita(
        cartella_cloud = outputFolderPath,
        csv_file_name  = nomeFile,
        last_modified = lastModified,
        ammissibilita_file_name = csvName
      )

      if (outputFolder.exists() && outputFolder.canWrite) {
        if (motivazione == MOTIVAZIONE_ALREADY_TRANSMITTED)
          FileUtility.writeCsv(fullPath, header, List(record), isTmpFolder = false, appendMode = false)
        else FileUtility.writeCsv(fullPath, header, List(record), isTmpFolder = false, appendMode = true)
        ammissibilitaVPG
      }
      else {
        logger.warn(s"Couldn't write to ${ammissibilitaVPG.cartella_cloud}, the path doesn't exist or it's not writable")
        ammissibilitaVPG.copy(cartella_cloud = s"Couldn't write to ${ammissibilitaVPG.cartella_cloud}, the path doesn't exist or it's not writable")
      }
    })


  }



  // Controlli a livello File

  def ruleCheckVPGFileName: VPGCsvRule = VPGCsvRule(
    ruleName = "ruleCheckVPGFileName",
    isEnabled = Properties.isRuleCheckVPGFileNameEnabled,
    condition = csv => {
      val fileName = csv.file.getName()

      fileName.matches(".*_VPG_.*_.*.csv")
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_200,
      statusMessage = MOTIVAZIONE_FILENAME
    )
  )

  def ruleCheckVPGPivaRdb: VPGCsvRule = VPGCsvRule(
    ruleName = "ruleCheckVPGFileName",
    isEnabled = Properties.isRuleCheckVPGFileNameEnabled,
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

  def ruleCheckVPGFileHeader: VPGCsvRule = VPGCsvRule(
    ruleName = "ruleCheckVPGFileHeader",
    isEnabled = Properties.isRuleCheckVPGFileHeaderEnabled,
    condition = csv => {
      val header = scala.io.Source.fromFile(csv.file).getLines().next()
      val splitHeader = header.toUpperCase.split(CSV_DELIMITER)
      val schema = VPGCsvSchema.getValues

      (splitHeader.length == schema.length && schema.zipWithIndex.map({ case (value, index) => value.equals(Try(splitHeader(index)).getOrElse("")) }).min)
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_200,
      statusMessage = MOTIVAZIONE_FILE_HEADER
    )
  )

  def ruleCheckVPGFileExtension: VPGCsvRule = VPGCsvRule(
    ruleName = "ruleCheckVPGFileExtension",
    isEnabled = Properties.isRuleCheckVPGFileExtensionEnabled,
    condition = csv => {
      csv.tipoFile == "csv" || csv.tipoFile == "xml"
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_200,
      statusMessage = MOTIVAZIONE_FILE_EXTENSION
    )
  )

  def ruleCheckVPGFileIntegrity: VPGCsvRule = VPGCsvRule(
    ruleName = "ruleCheckVPGFileIntegrity",
    isEnabled = Properties.isRuleCheckVPGFileIntegrityEnabled,
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

  // Controlli a livello RECORD

  // Il seguente controllo implementa entrambi i controlli 06 e 07 del VPG.
  def ruleCheckVPGDate: VPGRecordRule = VPGRecordRule(
    ruleName = "ruleCheckVPGDate",
    isEnabled = Properties.isRuleCheckVPGDateEnabled,
    condition = record => {
      record.giornoRiferimento.isDefined && record.giornoRiferimento.nonEmpty && isDate(record.giornoRiferimento)
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_224,
      statusMessage = MOTIVAZIONE_DATE_FORMAT
    )
  )

  def ruleCheckVPGDateConsistence: VPGRecordRule = VPGRecordRule(
    ruleName = "ruleCheckVPGDateConsistence",
    isEnabled = Properties.isRuleCheckVPGDateConsistenceEnabled,
    condition = record => {
      val annoRiferimento = record.giornoRiferimento.get.split("/")(2)

      val annoTermico1    = record.fileName.split("_")(2).slice(0,4)
      val annoTermico2    = record.fileName.split("_")(2).slice(4,8)

      annoRiferimento == annoTermico1 ||  annoRiferimento == annoTermico2

    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_209,
      statusMessage = MOTIVAZIONE_GIORNO_RIF
    )
  )

  def ruleCheckVPGMandatoryFields: VPGRecordRule =
    VPGRecordRule(
      ruleName = "ruleCheckVPGMandatoryFields",
      isEnabled = Properties.isRuleCheckVPGMandatoryFieldsEnabled,
      condition = record => {
        val valuesList = List(("C1A1", record.C1A1), ("C1A2", record.C1A2), ("C2", record.C2), ("C4", record.C4), ("C1A3", record.C1A3),
          ("C1B1", record.C1B1), ("C1B2", record.C1B2), ("C1B3", record.C1B3), ("C1C1", record.C1C1), ("C1C2", record.C1C2), ("C1C3", record.C1C3),
          ("C1D1", record.C1D1), ("C1D2", record.C1D2), ("C1D3", record.C1D3), ("C1E1", record.C1E1),
          ("C1E2", record.C1E2), ("C1E3", record.C1E3), ("C1F1", record.C1F1), ("C1F2", record.C1F2), ("C1F3", record.C1F3),
          ("T1_1", record.T11), ("T1_2", record.T12), ("T1_3", record.T13))

        val valuesListReport = valuesList.map({ case (name, value) => (name, value.isDefined) })

        VPGFieldMissing = valuesListReport.find({ case (name, condition) => !condition }).map(_._1)

        valuesListReport.forall(_._2)
      },
      message = ReportMessage(
        isAmmissibile = false,
        statusCode = COD_004,
        statusMessage = MANDATORY_ERROR + " (" + VPGFieldMissing.getOrElse("") + " non presente)"
      ))


  def ruleCheckVPGFieldsFormat: VPGRecordRule = VPGRecordRule(
    ruleName = "ruleCheckVPGFieldsFormat",
    isEnabled = Properties.isRuleCheckVPGFieldsFormatEnabled,
    condition = record => {
      val toDouble = (string: Option[String]) => Try(string.map(_.toDouble))

      val valuesList = List(("C1A1", record.C1A1), ("C1A2", record.C1A2), ("C2", record.C2), ("C4", record.C4), ("C1A3", record.C1A3),
        ("C1B1", record.C1B1), ("C1B2", record.C1B2), ("C1B3", record.C1B3), ("C1C1", record.C1C1), ("C1C2", record.C1C2), ("C1C3", record.C1C3),
        ("C1D1", record.C1D1), ("C1D2", record.C1D2), ("C1D3", record.C1D3), ("C1E1", record.C1E1),
        ("C1E2", record.C1E2), ("C1E3", record.C1E3), ("C1F1", record.C1F1), ("C1F2", record.C1F2), ("C1F3", record.C1F3))

      val valuesListReport = valuesList.map({ case (name, value) => (name, value, value.isDefined && value.get.contains(".") && toDouble(value).isSuccess) })

      VPGFieldIncorrect = valuesListReport.find({ case (name, value, condition) => !condition }).map(_._1)

      valuesListReport.forall(_._3)
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_004,
      statusMessage = MANDATORY_ERROR + " (" + VPGFieldIncorrect.getOrElse("") + " non corretto)"
    )
  )

  def ruleCheckVPGFieldsNumber: VPGRecordRule = VPGRecordRule(
    ruleName = "ruleCheckVPGFieldsNumber",
    isEnabled = Properties.isRuleCheckVPGFieldsNumberEnabled,
    condition = record => {
      val schema = VPGCsvSchema.getValues
      record.fields.length == schema.length
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_001,
      statusMessage = MOTIVAZIONE_FIELDS_NUMBER
    )
  )


}
