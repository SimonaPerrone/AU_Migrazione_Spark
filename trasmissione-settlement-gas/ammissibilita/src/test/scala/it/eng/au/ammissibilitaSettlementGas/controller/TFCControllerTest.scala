package it.eng.au.ammissibilitaSettlementGas.controller

import it.eng.au.ammissibilitaSettlementGas.controller
import it.eng.au.ammissibilitaSettlementGas.controller.TFCController.{getField, getTFCListFromFile}
import it.eng.au.ammissibilitaSettlementGas.dao.{PubblicazioneAmmissibilitaTFCDao, ReportAmmissibilitaTFCDao, TFCFileDao, TSGTFCDao}
import it.eng.au.ammissibilitaSettlementGas.model.{PubblicazioneAmmissibilitaTFC, ReportAmmissibilitaTFC, TFC}
import it.eng.au.ammissibilitaSettlementGas.model.rules.{TFCCsvRule, TFCRecordRule}
import it.eng.au.ammissibilitaSettlementGas.schema.{PubblicazioneAmmissibilitaTFCSchema, ReportAmmissibilitaTFCSchema, TFCFileSchema}
import it.eng.au.ammissibilitaSettlementGas.utility.Constants.CSV_DELIMITER
import it.eng.au.ammissibilitaSettlementGas.utility.file.FileUtility
import it.eng.au.ammissibilitaSettlementGas.utility.{Constants, EnvironmentSparkTest, Properties}
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.commons.io.FilenameUtils.removeExtension
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions.{col, corr, explode, hash, lit, when}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}

import java.io.{File, IOException}
import java.nio.file.{Files, Paths}
import java.nio.file.attribute.FileTime
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.reflect.io
import scala.util.matching.Regex
import java.text.SimpleDateFormat

class TFCControllerTest extends EnvironmentSparkTest {
  val path = "src/test/resources/input"//TSG2_10238291008"

  def testReadAndCheckCsvFiles():Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "10")

    val TFCMetadataRDD = TFCController.readCsvFiles()._1

    TFCMetadataRDD.foreach(x => println(x.file))

    val TFCCsvRules: List[TFCCsvRule] = List(
      //TFCController.ruleCheckTFCFileName, // 2
      TFCController.ruleCheckTFCPivaRdb, // 3
      TFCController.ruleCheckTFCFileExtension, // 4
      TFCController.ruleCheckTFCFileHeader // 5
    )
    val TFCRecordRules: List[TFCRecordRule] = List(
      TFCController.ruleCheckTFCDate, // 6
      TFCController.ruleCheckTFCDateFormat, // 7 & 8
      TFCController.ruleCheckTFCDateConsistence, // 9
      TFCController.ruleCheckTFCRegClimMandatory, // 10
      TFCController.ruleCheckTFCRegClimMismatch,  // 11
      TFCController.ruleCheckTFCWKRDefined, // 12
      TFCController.ruleCheckTFCWKRFormat, // 13
      TFCController.ruleCheckTFCFieldsNumber // 14
    )
    val readTFCFolder = TFCController.readCsvFiles()
    val TFCFilesWithMeta = readTFCFolder._1
    //TFCFilesWithMeta.persist(StorageLevel.MEMORY_AND_DISK)

    val checkedTFCFiles   = TFCFilesWithMeta.filter(tfcMeta => tfcMeta.isAmmissibile).map(tfcMeta => TFCController.checkTFCCsv(tfcMeta, TFCCsvRules))

    val correctTFCFiles   = checkedTFCFiles.filter(_.isAmmissibile)
    val incorrectTFCFiles = checkedTFCFiles.filter(!_.isAmmissibile) union TFCFilesWithMeta.filter(tfcMeta => !tfcMeta.isAmmissibile)
    println()
    println("CORRECT TFC FILES")
    correctTFCFiles.foreach(f => println(f.file.getName))
    println("INCORRECT TFC FILES")
    //incorrectTFCFiles.foreach(f => println(f.file.getName))
    incorrectTFCFiles.foreach(f => println(f))
    println()

    val TFCFilesAndRecords = correctTFCFiles.map(tfcMeta => tfcMeta.copy(csv = TFCController.getTFCListFromFile(tfcMeta)))
    val TFCRecords = TFCFilesAndRecords.flatMap(f => f.csv.getOrElse(List()))

    val TFCRecordsChecked = TFCRecords.map(tfcRec => TFCController.checkTFCRecord(tfcRec, TFCRecordRules))
    println(TFCFilesAndRecords.first().csv.get(1))
    println()
    println(TFCRecordsChecked.collect()(0))
    println(TFCRecordsChecked.collect()(1))
    println(TFCRecordsChecked.collect()(2))
    println(TFCRecordsChecked.collect()(3))
    println(TFCRecordsChecked.collect()(4))
    println(TFCRecordsChecked.collect()(5))
    println(TFCRecordsChecked.collect()(6))
    println(TFCRecordsChecked.collect()(19))
    println(TFCRecordsChecked.collect()(539))
    println(TFCRecordsChecked.collect()(19))

  }

  def testReadAndCheck2():Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "10")

    val TFCMetadataRDD = TFCController.readCsvFiles()._1

    TFCMetadataRDD.foreach(x => println(x.file))

    val TFCCsvRules: List[TFCCsvRule] = List(
      //TFCController.ruleCheckTFCFileName, // 2
      TFCController.ruleCheckTFCPivaRdb, // 3
      TFCController.ruleCheckTFCFileExtension, // 4
      TFCController.ruleCheckTFCFileHeader // 5
    )
    val TFCRecordRules: List[TFCRecordRule] = List(
      TFCController.ruleCheckTFCDate, // 6
      TFCController.ruleCheckTFCDateFormat, // 7 & 8
      TFCController.ruleCheckTFCDateConsistence, // 9
      TFCController.ruleCheckTFCRegClimMandatory, // 10
      TFCController.ruleCheckTFCRegClimMismatch,  // 11
      TFCController.ruleCheckTFCWKRDefined, // 12
      TFCController.ruleCheckTFCWKRFormat, // 13
      TFCController.ruleCheckTFCFieldsNumber // 14
    )
    val readTFCFolder = TFCController.readCsvFiles()
    val TFCFilesWithMeta = readTFCFolder._1
    //TFCFilesWithMeta.persist(StorageLevel.MEMORY_AND_DISK)

    val checkedTFCFiles   = TFCFilesWithMeta.filter(tfcMeta => tfcMeta.isAmmissibile).map(tfcMeta => TFCController.checkTFCCsv(tfcMeta, TFCCsvRules))

    val correctTFCFiles   = checkedTFCFiles.filter(_.isAmmissibile)
    val incorrectTFCFiles = checkedTFCFiles.filter(!_.isAmmissibile) union TFCFilesWithMeta.filter(tfcMeta => !tfcMeta.isAmmissibile)
    println()
    println("CORRECT TFC FILES")
    correctTFCFiles.foreach(f => println(f.file.getName))
    println("INCORRECT TFC FILES")
    //incorrectTFCFiles.foreach(f => println(f.file.getName))
    incorrectTFCFiles.foreach(f => println(f))
    println()

    val TFCFilesAndRecords = correctTFCFiles.map(tfcMeta => tfcMeta.copy(csv = TFCController.getTFCListFromFile(tfcMeta)))
    val TFCRecords = TFCFilesAndRecords.flatMap(f => f.csv.getOrElse(List()))

    val TFCRecordsChecked = TFCRecords.map(tfcRec => TFCController.checkTFCRecord(tfcRec, TFCRecordRules))
    println(TFCFilesAndRecords.first().csv.get(1))
    println()
  }

  def testTFCFileDaoGet():Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "10")

    val TFCFileDAO = new TFCFileDao()
    val readTFCFolder = TFCController.readCsvFiles()
    val TFCFilesWithMeta = readTFCFolder._1
    val sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    TFCFilesWithMeta.foreach(x => println(x.file))
    TFCFilesWithMeta.foreach(f => println(sdf.format(f.lastModified)))



    val tfcFile = TFCFileDAO.get(TFCFilesWithMeta)

    tfcFile.show(10)    // Sembra funzionare per bene.
  }


  def testReadCsvFiles():Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    val file = new File(path)
    println(file.getName)
    println()
    println(file.listFiles().foreach(println(_)))
    println()
    val settlement = file.listFiles.toList.filter(_.isDirectory)
    settlement.foreach(println(_))
    println()
    val years_rdd = Environment.spark.sparkContext.parallelize(settlement)

    years_rdd.foreach(println(_))
    val currentYear = "2021"
    val currentMonth = "05"
    println()
    val files_rdd = years_rdd.flatMap(_.listFiles().filter(f => f.isDirectory && f.getName == currentYear)) // && f.getName == "2020"))
      .flatMap(_.listFiles().filter(f => f.isDirectory && f.getName == currentMonth))
      .flatMap(_.listFiles().filter(f => f.isFile && f.getName.contains("TFC")))

    files_rdd.foreach(println(_))
    println()

    val files_rdd_2 = years_rdd.flatMap(_.listFiles().filter(f => f.isDirectory))
    files_rdd_2.foreach(println(_))
    println()
    //Environment.setProperty("year.month", "202210")
    Environment.setProperty("csv.input.path", path)
    Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.month", "05")

    val TFCMetadataRDD = TFCController.readCsvFiles()._1

    TFCMetadataRDD.foreach(x => println(x.file.getName))
  }



  def testRules(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    val TFCFileRegex: Regex = "^([A-Za-z0-9]+)_(TFC)_(\\d{6})_(\\d+)\\.(?i)(csv|xml)".r
    val TFCFileRegex2: Regex = "^([A-Za-z0-9]+)_(TFC)_(\\d{6})_(\\d+)\\.(?i)([A-Za-z0-9]+)".r
    val TFCFileRegexCorrect: Regex = "^([A-Za-z0-9]+)_(TFC)_(\\d{6})_(\\d+)\\.(?i)([A-Za-z0-9]+)".r
    val TFCFileRegexCorrect2: Regex = "^([A-Za-z0-9]+)_(TFC)_(\\d{6})_([A-Za-z0-9]+)\\.(?i)([A-Za-z0-9]+)".r
    val file = new File("src/test/resources/input/TSG2_10238291008/2021/07/10238291008_TFC_202106_1kj.csv")

    println(file.getName.toLowerCase.endsWith(".csv"))
    file.getName match {
      case TFCFileRegexCorrect(pivaRdb,tfc,annoMese,progressivo,estensione) => println(estensione)
      case TFCFileRegexCorrect2(pivaRdb,tfc,annoMese,progressivo,estensione) => println(estensione)
      case _ => println("niente non c'è verso k")
    }


  }

  def test2:Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.month", "05")

    //val TFCMetadataRDD = TFCController.readCsvFiles()

    //TFCMetadataRDD.foreach(x => println(x))

    val lista = List(1)
    println(lista)
  }

  def test_3:Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    Environment.setProperty("current.year", "2023")
    Environment.setProperty("current.month", "01")

    val TFCMetadataRDD = TFCController.readCsvFiles()._1

    println(TFCMetadataRDD.first())

    val tfcMetaWithRecords = TFCMetadataRDD.map(tfcMeta => tfcMeta.copy(csv = getTFCListFromFile(tfcMeta)))

    println(tfcMetaWithRecords.first().csv.get(0).fields.length)
    println(tfcMetaWithRecords.first().csv.get(0).fields(1))

    //val fields:Array[String] = List("1","2","3")
  }


  def test4:Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "10")

    val TFCMetadataRDD = TFCController.readCsvFiles()._1

    TFCMetadataRDD.foreach(x => println(x.file))

    val TFCCsvRules: List[TFCCsvRule] = List(
      //TFCController.ruleCheckTFCFileName, // 2
      TFCController.ruleCheckTFCPivaRdb, // 3
      TFCController.ruleCheckTFCFileExtension, // 4
      TFCController.ruleCheckTFCFileHeader // 5
    )
    val TFCRecordRules: List[TFCRecordRule] = List(
      TFCController.ruleCheckTFCDate, // 6
      TFCController.ruleCheckTFCDateFormat, // 7 & 8
      TFCController.ruleCheckTFCDateConsistence, // 9
      TFCController.ruleCheckTFCRegClimMandatory, // 10
      TFCController.ruleCheckTFCRegClimMismatch,  // 11
      TFCController.ruleCheckTFCWKRDefined, // 12
      TFCController.ruleCheckTFCWKRFormat, // 13
      TFCController.ruleCheckTFCFieldsNumber // 14
    )
    val readTFCFolder = TFCController.readCsvFiles()
    val TFCFilesWithMeta = readTFCFolder._1
    //TFCFilesWithMeta.persist(StorageLevel.MEMORY_AND_DISK)

    val checkedTFCFiles   = TFCFilesWithMeta.filter(tfcMeta => tfcMeta.isAmmissibile).map(tfcMeta => TFCController.checkTFCCsv(tfcMeta, TFCCsvRules))

    val correctTFCFiles   = checkedTFCFiles.filter(_.isAmmissibile)
    val incorrectTFCFiles = checkedTFCFiles.filter(!_.isAmmissibile) union TFCFilesWithMeta.filter(tfcMeta => !tfcMeta.isAmmissibile)
    println()
    println("CORRECT TFC FILES")
    correctTFCFiles.foreach(f => println(f.file.getName))
    println("INCORRECT TFC FILES")
    //incorrectTFCFiles.foreach(f => println(f.file.getName))
    incorrectTFCFiles.foreach(f => println(f))
    println()

    val TFCFilesAndRecords = correctTFCFiles.map(tfcMeta => tfcMeta.copy(csv = TFCController.getTFCListFromFile(tfcMeta)))
    val TFCRecords = TFCFilesAndRecords.flatMap(f => f.csv.getOrElse(List()))

    val TFCRecordsChecked = TFCRecords.map(tfcRec => TFCController.checkTFCRecord(tfcRec, TFCRecordRules))
    println(TFCFilesAndRecords.first().csv.get(1))
    println()

    // Ora tentiamo di unire le cose e valutare i record di un certo file direttamente nella struttura
    // TFCMetadata (c'è il campo csv che è una List[TSG]).
    val newTFCFilesAndRecords = correctTFCFiles.map(tfcMeta => tfcMeta.copy(csv = TFCController.getTFCListFromFile(tfcMeta)))

    val TFCFilesAndRecordsChecked = newTFCFilesAndRecords.
      map(tfcMeta => tfcMeta.copy(csv = Some(tfcMeta.csv.getOrElse(List()).map(tfcRec => TFCController.checkTFCRecord(tfcRec, TFCRecordRules)))))

    // println(TFCFilesAndRecordsChecked.collect()(0)) // Questo per vedere un esempio completo della cosa.
    // A questo punto ogni record corrispondente al particolare file avrà anche tutte le info riguardanti le sue righe e le relative valutazioni di ammissibilità.

    //println(TFCFilesAndRecordsChecked.collect()(0).csv.get(10))

    println(TFCFilesAndRecordsChecked.first().csv.get(0))
    println()
    println(TFCFilesAndRecordsChecked.first().csv.get(1))
    println()
    println(TFCFilesAndRecordsChecked.first().csv.get(2))
    println()
    println(TFCFilesAndRecordsChecked.first().csv.get(3))
    println()
    println(TFCFilesAndRecordsChecked.first().csv.get(19))
    println()
    println(TFCFilesAndRecordsChecked.first().csv.get(2))
    println()
    println(TFCFilesAndRecordsChecked.first().csv.get(3))
    println()
    println()
    println(TFCFilesAndRecordsChecked.first())
    println(TFCFilesAndRecordsChecked.first().csv.get(0))
  }


  def testCreationRepAmm:Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "10")

    val TFCMetadataRDD = TFCController.readCsvFiles()._1

    TFCMetadataRDD.foreach(x => println(x.file))

    val TFCCsvRules: List[TFCCsvRule] = List(
      //TFCController.ruleCheckTFCFileName, // 2
      TFCController.ruleCheckTFCPivaRdb, // 3
      TFCController.ruleCheckTFCFileExtension, // 4
      TFCController.ruleCheckTFCFileHeader // 5
    )
    val TFCRecordRules: List[TFCRecordRule] = List(
      TFCController.ruleCheckTFCDate, // 6
      TFCController.ruleCheckTFCDateFormat, // 7 & 8
      TFCController.ruleCheckTFCDateConsistence, // 9
      TFCController.ruleCheckTFCRegClimMandatory, // 10
      TFCController.ruleCheckTFCRegClimMismatch, // 11
      TFCController.ruleCheckTFCWKRDefined, // 12
      TFCController.ruleCheckTFCWKRFormat, // 13
      TFCController.ruleCheckTFCFieldsNumber // 14
    )
    val readTFCFolder = TFCController.readCsvFiles()
    val TFCFilesWithMeta = readTFCFolder._1
    //TFCFilesWithMeta.persist(StorageLevel.MEMORY_AND_DISK)

    val checkedTFCFiles = TFCFilesWithMeta.filter(tfcMeta => tfcMeta.isAmmissibile).map(tfcMeta => TFCController.checkTFCCsv(tfcMeta, TFCCsvRules))

    val correctTFCFiles = checkedTFCFiles.filter(_.isAmmissibile)
    val incorrectTFCFiles = checkedTFCFiles.filter(!_.isAmmissibile) union TFCFilesWithMeta.filter(tfcMeta => !tfcMeta.isAmmissibile)
    println()
    println("CORRECT TFC FILES")
    correctTFCFiles.foreach(f => println(f.file.getName))
    println("INCORRECT TFC FILES")
    //incorrectTFCFiles.foreach(f => println(f.file.getName))
    incorrectTFCFiles.foreach(f => println(f))
    println()

    val TFCFilesAndRecords = correctTFCFiles.map(tfcMeta => tfcMeta.copy(csv = TFCController.getTFCListFromFile(tfcMeta)))
    //val TFCRecords = TFCFilesAndRecords.flatMap(f => f.csv.getOrElse(List()))
    //val TFCRecords2 = TFCFilesAndRecords.flatMap(f => (f.csv.getOrElse(List()),"a"))

    //val TFCFilesAndRecordsExplode = TFCFilesAndRecords.flatMap(tfcMeta => tfcMeta.csv.map())

    val rdd = TFCFilesAndRecords.map(vpgMetaRow => {(vpgMetaRow.file.getName, vpgMetaRow.yearDir, vpgMetaRow.monthDir, vpgMetaRow.pivaRdb, vpgMetaRow.annoMese, vpgMetaRow.lastModified, vpgMetaRow.csv)})

    val df = Environment.spark.createDataFrame(rdd).toDF("nomeFile", "anno", "mese", TFCFileSchema.piva_rdb, TFCFileSchema.annomese, TFCFileSchema.data_creazione, "csv")
    val dfWithIDFile = df.withColumn("n_id_tsg2_file", hash(df.col("nomeFile"), df.col("anno"), df.col("mese")).cast(LongType) + Int.MaxValue)

    //dfWithIDFile.withColumn(TFCFileSchema.executionid, lit(Environment.executionId).cast(LongType))
      //.selectExpr(TFCFileSchema.getValues: _*)

    //dfWithIDFile.show(false)
    //dfWithIDFile.select(col("nomeFile"), col("anno"), col("mese"), TFCFileSchema.piva_rdb, TFCFileSchema.annomese,
      //TFCFileSchema.data_creazione, explode(col("csv")))

    /*
    dfWithIDFile.select(col("nomeFile"), col("anno"), explode(col("csv").alias("explode")))
      .withColumn("fileName", col("explode.fileName"))
      .drop("explode")
      .show(false) */

    val rdd2 = TFCFilesAndRecords.map(vpgMetaRow => {(vpgMetaRow.file.getName, vpgMetaRow.yearDir, vpgMetaRow.monthDir,
      vpgMetaRow.tipoFile, vpgMetaRow.pivaRdb, vpgMetaRow.annoMese, vpgMetaRow.isAmmissibile, vpgMetaRow.statusCode, vpgMetaRow.statusMessage,
      vpgMetaRow.csv)})

    val df2 = Environment.spark.createDataFrame(rdd2).toDF("nomeFile", "anno", "mese", ReportAmmissibilitaTFCSchema.tipo_file,
      ReportAmmissibilitaTFCSchema.piva_utente, ReportAmmissibilitaTFCSchema.annomese, ReportAmmissibilitaTFCSchema.verifica_amm,
      ReportAmmissibilitaTFCSchema.cod_causale, ReportAmmissibilitaTFCSchema.motivazione,"csv")

    val dfWithIDFile2 = df2.withColumn(ReportAmmissibilitaTFCSchema.n_id_tsg2_file, hash(df2.col("nomeFile"),
      df2.col("anno"), df2.col("mese")).cast(LongType) + Int.MaxValue)

    val dfresult = dfWithIDFile2.select(col(ReportAmmissibilitaTFCSchema.n_id_tsg2_file),
      explode(col("csv")).alias("explode"), col(ReportAmmissibilitaTFCSchema.tipo_file),
       col(ReportAmmissibilitaTFCSchema.piva_utente), col(ReportAmmissibilitaTFCSchema.annomese),
      col(ReportAmmissibilitaTFCSchema.verifica_amm),
      col(ReportAmmissibilitaTFCSchema.cod_causale), col(ReportAmmissibilitaTFCSchema.motivazione))
      .withColumn(ReportAmmissibilitaTFCSchema.wkr, col("explode.wkr"))
      .withColumn(ReportAmmissibilitaTFCSchema.id_reg_clim, col("explode.idRegClimatica"))
      .withColumn(ReportAmmissibilitaTFCSchema.numero_riga, col("explode.numeroRiga"))
      .withColumn("ReportAmmissibilitaTFCSchema.verifica_amm", col("explode.isAmmissibile"))
      .withColumn("ReportAmmissibilitaTFCSchema.cod_causale", col("explode.statusCode"))
      .withColumn("ReportAmmissibilitaTFCSchema.motivazione", col("explode.statusMessage"))
      .drop("explode")


    dfresult.show(10)

    // Produzione file TFC Amm relativo ad un particolare file di input.

    //TFCFilesAndRecordsChecked
  }

  def testReportAmmissibilitaTFCDaoGet():Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "10")

    val TFCMetadataRDD = TFCController.readCsvFiles()

    val TFCCsvRules: List[TFCCsvRule] = List(
      //TFCController.ruleCheckTFCFileName, // 2
      TFCController.ruleCheckTFCPivaRdb, // 3
      TFCController.ruleCheckTFCFileExtension, // 4
      TFCController.ruleCheckTFCFileHeader // 5
    )
    val TFCRecordRules: List[TFCRecordRule] = List(
      TFCController.ruleCheckTFCDate, // 6
      TFCController.ruleCheckTFCDateFormat, // 7 & 8
      TFCController.ruleCheckTFCDateConsistence, // 9
      TFCController.ruleCheckTFCRegClimMandatory, // 10
      TFCController.ruleCheckTFCRegClimMismatch, // 11
      TFCController.ruleCheckTFCWKRDefined, // 12
      TFCController.ruleCheckTFCWKRFormat, // 13
      TFCController.ruleCheckTFCFieldsNumber // 14
    )
    val readTFCFolder = TFCController.readCsvFiles()
    val TFCFilesWithMeta = readTFCFolder._1
    //TFCFilesWithMeta.persist(StorageLevel.MEMORY_AND_DISK)
    val checkedTFCFiles = TFCFilesWithMeta.filter(tfcMeta => tfcMeta.isAmmissibile).map(tfcMeta => TFCController.checkTFCCsv(tfcMeta, TFCCsvRules))
    val correctTFCFiles = checkedTFCFiles.filter(_.isAmmissibile)
    val incorrectTFCFiles = checkedTFCFiles.filter(!_.isAmmissibile) union TFCFilesWithMeta.filter(tfcMeta => !tfcMeta.isAmmissibile)

    val checkedTFCFiles2 = TFCFilesWithMeta.filter(tfcMeta => !tfcMeta.isAmmissibile) union TFCFilesWithMeta.filter(tfcMeta => tfcMeta.isAmmissibile).map(tfcMeta => TFCController.checkTFCCsv(tfcMeta, TFCCsvRules))

    val TFCFilesAndRecords = correctTFCFiles.map(tfcMeta => tfcMeta.copy(csv = TFCController.getTFCListFromFile(tfcMeta)))
    //val TFCRecords = TFCFilesAndRecords.flatMap(f => f.csv.getOrElse(List()))
    //val TFCRecords2 = TFCFilesAndRecords.flatMap(f => (f.csv.getOrElse(List()),"a"))

    //val TFCFilesAndRecordsExplode = TFCFilesAndRecords.flatMap(tfcMeta => tfcMeta.csv.map())

    checkedTFCFiles.foreach(println)
    println()
    println()
    val newTFCFilesAndRecords = checkedTFCFiles2.map(tfcMeta =>
      if (tfcMeta.isAmmissibile) tfcMeta.copy(csv = TFCController.getTFCListFromFile(tfcMeta))
      else  tfcMeta)

    //val TFCFilesAndRecordsChecked = newTFCFilesAndRecords.
    //map(tfcMeta => tfcMeta.copy(csv = Some(tfcMeta.csv.getOrElse(List()).map(tfcRec => TFCController.checkTFCRecord(tfcRec, TFCRecordRules)))))

    newTFCFilesAndRecords.foreach(println)

    val TFCFilesAndRecordsChecked = newTFCFilesAndRecords.
      map(tfcMeta => tfcMeta.copy(csv = Some(tfcMeta.csv.getOrElse(List()).map(tfcRec => TFCController.checkTFCRecord(tfcRec, TFCRecordRules)))))

    //TFCFilesAndRecordsChecked.foreach(println)

    val reportAmmissibilitaTFCDAO = new ReportAmmissibilitaTFCDao()

    val table = reportAmmissibilitaTFCDAO.get(TFCFilesAndRecordsChecked)
    println()
    table.show(false)
    println()
    val NotAmmissibleFiles = table.filter(col(ReportAmmissibilitaTFCSchema.wkr) === lit(""))

    val newTable = table.withColumn(ReportAmmissibilitaTFCSchema.wkr, col(ReportAmmissibilitaTFCSchema.wkr).cast(DoubleType))

    newTable.show(10, false)
    //val tsg2_pubblicazione_ammissibilita_vpg = table.select(col(ReportAmmissibilitaTFCSchema.n_id_tsg2_file), col(ReportAmmissibilitaTFCSchema.))
    /*
    // PROVIAMO ALTRE STRADE...

    val rdd = TFCFilesAndRecordsChecked.map(tfcMeta => {(tfcMeta.file.getName, tfcMeta.yearDir, tfcMeta.monthDir, tfcMeta.pivaRdb,
      tfcMeta.isAmmissibile, tfcMeta.statusCode, tfcMeta.statusMessage, tfcMeta.annoMese, tfcMeta.csv, tfcMeta.tipoFile)})

    val df = Environment.spark.createDataFrame(rdd).toDF(ReportAmmissibilitaTFCSchema.nome_file, "anno", "mese", ReportAmmissibilitaTFCSchema.piva_utente,
      ReportAmmissibilitaTFCSchema.verifica_amm, ReportAmmissibilitaTFCSchema.cod_causale, ReportAmmissibilitaTFCSchema.motivazione,
      ReportAmmissibilitaTFCSchema.annomese, "csv", ReportAmmissibilitaTFCSchema.tipo_file)

    val dfWithIDFile = df.withColumn(ReportAmmissibilitaTFCSchema.n_id_tsg2_file, hash(df.col("nomeFile"), df.col("anno"), df.col("mese")).cast(LongType) + Int.MaxValue)

    val df1 = dfWithIDFile.filter(col(ReportAmmissibilitaTFCSchema.verifica_amm) === "false").select(col(ReportAmmissibilitaTFCSchema.n_id_tsg2_file), col(ReportAmmissibilitaTFCSchema.nome_file),
      col(ReportAmmissibilitaTFCSchema.piva_utente), col(ReportAmmissibilitaTFCSchema.verifica_amm), col(ReportAmmissibilitaTFCSchema.cod_causale),
      col(ReportAmmissibilitaTFCSchema.motivazione), col(ReportAmmissibilitaTFCSchema.annomese), col(ReportAmmissibilitaTFCSchema.annomese_ricezione),
      explode(col("csv")).alias("explode"), col(ReportAmmissibilitaTFCSchema.tipo_file))
      .withColumn(ReportAmmissibilitaTFCSchema.wkr, lit(""))
      .withColumn(ReportAmmissibilitaTFCSchema.id_reg_clim, lit(""))
      .withColumn(ReportAmmissibilitaTFCSchema.data, lit(""))
      .withColumn(ReportAmmissibilitaTFCSchema.numero_riga, lit(""))
      .withColumn(ReportAmmissibilitaTFCSchema.executionid, lit(Environment.executionId).cast(LongType))
      .withColumn(ReportAmmissibilitaTFCSchema.data_amm, lit(Environment.executionId).cast(StringType))
      .drop("explode").selectExpr(ReportAmmissibilitaTFCSchema.getValues: _*)

    dfWithIDFile.show(false)

    val df2 = dfWithIDFile.filter(col(ReportAmmissibilitaTFCSchema.verifica_amm) === false).select(col(ReportAmmissibilitaTFCSchema.n_id_tsg2_file),
      col(ReportAmmissibilitaTFCSchema.nome_file), col(ReportAmmissibilitaTFCSchema.annomese_ricezione),
      col(ReportAmmissibilitaTFCSchema.piva_utente), col(ReportAmmissibilitaTFCSchema.verifica_amm), col(ReportAmmissibilitaTFCSchema.cod_causale),
      col(ReportAmmissibilitaTFCSchema.motivazione), col(ReportAmmissibilitaTFCSchema.annomese), col(ReportAmmissibilitaTFCSchema.tipo_file))

    //df1.show(false)

    table.show(false)

    val tfcAmmTable = table.withColumn("NUM_RIGA", col(ReportAmmissibilitaTFCSchema.numero_riga))
      .withColumn("COD_TIPO_FILE", lit("TFC"))
      .withColumn("PIVA_UTENTE", col(ReportAmmissibilitaTFCSchema.piva_utente))
      .withColumn("VERIFICA_AMM",  when(col(ReportAmmissibilitaTFCSchema.verifica_amm) === false,"N")
      .when(col(ReportAmmissibilitaTFCSchema.verifica_amm) === true,"Y"))
      .withColumn("COD_CAUSALE", col(ReportAmmissibilitaTFCSchema.cod_causale))
      .withColumn("MOTIVAZIONE", col(ReportAmmissibilitaTFCSchema.motivazione))
      .select(col("NUM_RIGA"), col("COD_TIPO_FILE"), col("PIVA_UTENTE"), col("VERIFICA_AMM"), col("COD_CAUSALE"), col("MOTIVAZIONE"))

    tfcAmmTable.show(false) */

  }


  def testCreationTFCAmmFile():Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "10")

    val TFCFileDAO = new TFCFileDao()
    val ReportAmmissibilitaTFCDao = new ReportAmmissibilitaTFCDao()

    val TFCCsvRules: List[TFCCsvRule] = List(
      TFCController.ruleCheckTFCPivaRdb, // 3
      TFCController.ruleCheckTFCFileExtension, // 4
      TFCController.ruleCheckTFCFileHeader // 5
    )

    val TFCRecordRules: List[TFCRecordRule] = List(
      TFCController.ruleCheckTFCDate, // 6
      TFCController.ruleCheckTFCDateFormat, // 7 & 8
      TFCController.ruleCheckTFCDateConsistence, // 9
      TFCController.ruleCheckTFCRegClimMandatory, // 10
      TFCController.ruleCheckTFCRegClimMismatch,  // 11
      TFCController.ruleCheckTFCWKRDefined, // 12
      TFCController.ruleCheckTFCWKRFormat, // 13
      TFCController.ruleCheckTFCFieldsNumber  // 14
    )

    val readTFCFolder = TFCController.readCsvFiles()
    val TFCFilesWithMeta = readTFCFolder._1

    val checkedTFCFiles   = TFCFilesWithMeta.filter(tfcMeta => !tfcMeta.isAmmissibile) union
      TFCFilesWithMeta.filter(tfcMeta => tfcMeta.isAmmissibile).map(tfcMeta => TFCController.checkTFCCsv(tfcMeta, TFCCsvRules))

    val correctTFCFiles   = checkedTFCFiles.filter(_.isAmmissibile)
    val incorrectTFCFiles = checkedTFCFiles.filter(!_.isAmmissibile)

    // Ora dovrei popolare le tabelle TSG2_TFC_FILE e TSG2_REPORT_AMMISSIBILITA_TFC

    // Popolamento tabella TSG2_TFC_FILE
    val tfcFile = TFCFileDAO.get(TFCFilesWithMeta)

    // Popolamento tabella TSG2_REPORT_AMMISSIBILITA_TFC

    val TFCFilesAndRecords = checkedTFCFiles.map(tfcMeta =>
      if (tfcMeta.isAmmissibile) tfcMeta.copy(csv = getTFCListFromFile(tfcMeta))
      else tfcMeta)

    val TFCFilesAndRecordsChecked = TFCFilesAndRecords.
      map(tfcMeta => tfcMeta.copy(csv = Some(tfcMeta.csv.getOrElse(List()).map(tfcRec => TFCController.checkTFCRecord(tfcRec, TFCRecordRules)))))

    val reportAmmissibilitaTFC = ReportAmmissibilitaTFCDao.get(TFCFilesAndRecordsChecked)

    //tfcFile.show(false)
    println("\n")
    //reportAmmissibilitaTFC.show(false)
    println("\n")

    val filteredRDD = TFCFilesAndRecordsChecked.filter(tfcMeta => (tfcMeta.file.getName == "10238291008_TFC_202109_1.csv" &&
      tfcMeta.yearDir == "2021" && tfcMeta.monthDir == "10"))

    //filteredRDD.foreach(println)

    val rdd = filteredRDD.map(tfcMeta => {(tfcMeta.file.getName, tfcMeta.pivaRdb, tfcMeta.isAmmissibile, tfcMeta.statusCode, tfcMeta.statusMessage, tfcMeta.csv)})

    val df = Environment.spark.createDataFrame(rdd).toDF("NOME_FILE", "PIVA_UTENTE", "VERIFICA_AMM", "COD_CAUSALE", "MOTIVAZIONE", "csv")

    val df1 = df.filter(col("VERIFICA_AMM") === "false" ||
      col("VERIFICA_AMM") === false).select(col("NOME_FILE"),
      col("PIVA_UTENTE"), col("VERIFICA_AMM"), col("COD_CAUSALE"), col("MOTIVAZIONE"))
      .withColumn("NUM_RIGA", lit(""))
      .withColumn("COD_TIPO_FILE", lit("TFC"))
      .select(col("NUM_RIGA"), col("COD_TIPO_FILE"), col("PIVA_UTENTE"), col("VERIFICA_AMM"), col("COD_CAUSALE"), col("MOTIVAZIONE"))

    val df2 = df.filter(col("VERIFICA_AMM") === true).select(col("NOME_FILE"), col("PIVA_UTENTE"),
       explode(col("csv")).alias("explode"))
      .withColumn("COD_TIPO_FILE", lit("TFC"))
      .withColumn("NUM_RIGA", col("explode.numeroRiga"))
      .withColumn("VERIFICA_AMM", col("explode.isAmmissibile"))
      .withColumn("COD_CAUSALE", col("explode.statusCode"))
      .withColumn("MOTIVAZIONE", col("explode.statusMessage"))
      .select(col("NUM_RIGA"), col("COD_TIPO_FILE"), col("PIVA_UTENTE"), col("VERIFICA_AMM"), col("COD_CAUSALE"), col("MOTIVAZIONE"))

    val resultDF = df1.union(df2).select(col("NUM_RIGA"), col("COD_TIPO_FILE"), col("PIVA_UTENTE"), col("VERIFICA_AMM"), col("COD_CAUSALE"), col("MOTIVAZIONE"))

    resultDF.show(false)
    println(resultDF.count())
    println()

    val header = List("NUM_RIGA", "COD_TIPO_FILE", "PIVA_UTENTE", "VERIFICA_AMM", "COD_CAUSALE", "MOTIVAZIONE").mkString(CSV_DELIMITER)


    resultDF.rdd.foreach(rddRow => {
      val numRiga = rddRow(0)
      val codTipoFile = rddRow(1)
      val pivaUtente = rddRow(2)
      val verificaAmm = if (rddRow(3) == true) "Y" else "N"
      val codCausale = rddRow(4)
      val motivazione = rddRow(5)

      val record = List(numRiga, codTipoFile, pivaUtente, verificaAmm, codCausale, motivazione).mkString(CSV_DELIMITER)

      //FileUtility.writeCsv("src/test/resources/input/TSG2_10238291008/2021/10/fileProdotto/we.csv", header, List(record), isTmpFolder = false, appendMode = true)

      println(record)
    })

  }

  def testCreationTFCAmmFile2():Unit = {
      val sc = Environment.sparkContext
      sc.setLogLevel("ERROR")

      Environment.setProperty("csv.input.path", path)
      //Environment.setProperty("current.year", "2021")
      Environment.setProperty("current.year", "2021")
      //Environment.setProperty("current.month", "05")
      Environment.setProperty("current.month", "10")

      val TFCFileDAO = new TFCFileDao()
      val ReportAmmissibilitaTFCDao = new ReportAmmissibilitaTFCDao()

      val TFCCsvRules: List[TFCCsvRule] = List(
        TFCController.ruleCheckTFCPivaRdb, // 3
        TFCController.ruleCheckTFCFileExtension, // 4
        TFCController.ruleCheckTFCFileHeader // 5
      )

      val TFCRecordRules: List[TFCRecordRule] = List(
        TFCController.ruleCheckTFCDate, // 6
        TFCController.ruleCheckTFCDateFormat, // 7 & 8
        TFCController.ruleCheckTFCDateConsistence, // 9
        TFCController.ruleCheckTFCRegClimMandatory, // 10
        TFCController.ruleCheckTFCRegClimMismatch,  // 11
        TFCController.ruleCheckTFCWKRDefined, // 12
        TFCController.ruleCheckTFCWKRFormat, // 13
        TFCController.ruleCheckTFCFieldsNumber  // 14
      )

      val readTFCFolder = TFCController.readCsvFiles()
    val TFCFilesWithMeta = readTFCFolder._1

      val checkedTFCFiles   = TFCFilesWithMeta.filter(tfcMeta => !tfcMeta.isAmmissibile) union
        TFCFilesWithMeta.filter(tfcMeta => tfcMeta.isAmmissibile).map(tfcMeta => TFCController.checkTFCCsv(tfcMeta, TFCCsvRules))

      val correctTFCFiles   = checkedTFCFiles.filter(_.isAmmissibile)
      val incorrectTFCFiles = checkedTFCFiles.filter(!_.isAmmissibile)

      // Ora dovrei popolare le tabelle TSG2_TFC_FILE e TSG2_REPORT_AMMISSIBILITA_TFC

      // Popolamento tabella TSG2_TFC_FILE
      val tfcFile = TFCFileDAO.get(TFCFilesWithMeta)

      // Popolamento tabella TSG2_REPORT_AMMISSIBILITA_TFC

      val TFCFilesAndRecords = checkedTFCFiles.map(tfcMeta =>
        if (tfcMeta.isAmmissibile) tfcMeta.copy(csv = getTFCListFromFile(tfcMeta))
        else tfcMeta)

      val TFCFilesAndRecordsChecked = TFCFilesAndRecords.
        map(tfcMeta => tfcMeta.copy(csv = Some(tfcMeta.csv.getOrElse(List()).map(tfcRec => TFCController.checkTFCRecord(tfcRec, TFCRecordRules)))))

      val reportAmmissibilitaTFC = ReportAmmissibilitaTFCDao.get(TFCFilesAndRecordsChecked)

      val filteredRDD = TFCFilesAndRecordsChecked.filter(tfcMeta => (tfcMeta.file.getName == "10238291008_TFC_202109_1.csv" &&
      tfcMeta.yearDir == "2021" && tfcMeta.monthDir == "10"))

    //filteredRDD.foreach(println)

    val rdd = TFCFilesAndRecordsChecked.map(tfcMeta => {(tfcMeta.file.getName, tfcMeta.pivaRdb, tfcMeta.isAmmissibile, tfcMeta.statusCode, tfcMeta.statusMessage, tfcMeta.csv)})

    val df = Environment.spark.createDataFrame(rdd).toDF("NOME_FILE", "PIVA_UTENTE", "VERIFICA_AMM", "COD_CAUSALE", "MOTIVAZIONE", "csv")

    val df1 = df.filter(col("VERIFICA_AMM") === "false" ||
      col("VERIFICA_AMM") === false).select(col("PIVA_UTENTE"), col("VERIFICA_AMM"), col("NOME_FILE"),
      col("COD_CAUSALE"), col("MOTIVAZIONE"))
      .withColumn("NUM_RIGA", lit(""))
      .withColumn("COD_TIPO_FILE", lit("TFC"))
      .select(col("NOME_FILE"), col("NUM_RIGA"), col("COD_TIPO_FILE"), col("PIVA_UTENTE"),
        col("VERIFICA_AMM"), col("COD_CAUSALE"), col("MOTIVAZIONE"))

    val df2 = df.filter(col("VERIFICA_AMM") === true).select( col("PIVA_UTENTE"), col("NOME_FILE"),
      explode(col("csv")).alias("explode"))
      .withColumn("COD_TIPO_FILE", lit("TFC"))
      .withColumn("NUM_RIGA", col("explode.numeroRiga"))
      .withColumn("VERIFICA_AMM", col("explode.isAmmissibile"))
      .withColumn("COD_CAUSALE", col("explode.statusCode"))
      .withColumn("MOTIVAZIONE", col("explode.statusMessage"))
      .select(col("NOME_FILE"), col("NUM_RIGA"), col("COD_TIPO_FILE"), col("PIVA_UTENTE"),
        col("VERIFICA_AMM"), col("COD_CAUSALE"), col("MOTIVAZIONE"))

    val resultDF = df1.union(df2).select(col("NOME_FILE"), col("NUM_RIGA"), col("COD_TIPO_FILE"),
      col("PIVA_UTENTE"), col("VERIFICA_AMM"), col("COD_CAUSALE"), col("MOTIVAZIONE"))

    resultDF.show(false)
    println(resultDF.count())
    println()

    val header = List("NUM_RIGA", "COD_TIPO_FILE", "PIVA_UTENTE", "VERIFICA_AMM", "COD_CAUSALE", "MOTIVAZIONE").mkString(CSV_DELIMITER)

    val outputPath = "src/test/resources/input/TSG2_10238291008/2021/10/fileProdotto/"
    resultDF.rdd.foreach(rddRow => {
      val nomeFile = rddRow(0)
      val numRiga = rddRow(1)
      val codTipoFile = rddRow(2)
      val pivaUtente = rddRow(3)
      val verificaAmm = if (rddRow(4) == true) "Y" else "N"
      val codCausale = rddRow(5)
      val motivazione = rddRow(6)

      val record = List(numRiga, codTipoFile, pivaUtente, verificaAmm, codCausale, motivazione).mkString(CSV_DELIMITER)

      //FileUtility.writeCsv(outputPath + removeExtension(nomeFile.toString) + "AMMISSIB.csv", header, List(record), isTmpFolder = false, appendMode = true)

      //println(getAmmissibilitaCsvNameTest(nomeFile.toString, Environment.startDateTime, "2021", "10"))
      println(record)
    })


  }

  def getAmmissibilitaCsvNameTest(inputFileName: String, daterun: LocalDateTime, year: String, month: String): (String, String) = {
    val timestamp = daterun.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    val inputFileNameWithoutExtension = inputFileName.replace(".csv", "")

    (s"/$year/$month/", s"/${inputFileNameWithoutExtension}_AMM_${timestamp}.csv")
  }

  def testwriteCSVTFCAmmFile() = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    Environment.setProperty("csvAmmissibilita.output.path", path + "/TSG2_10238291008")
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "11")


    val TFCFileDAO = new TFCFileDao()
    val ReportAmmissibilitaTFCDao = new ReportAmmissibilitaTFCDao()

    val TFCCsvRules: List[TFCCsvRule] = List(
      TFCController.ruleCheckTFCPivaRdb, // 3
      TFCController.ruleCheckTFCFileExtension, // 4
      TFCController.ruleCheckTFCFileHeader // 5
    )

    val TFCRecordRules: List[TFCRecordRule] = List(
      TFCController.ruleCheckTFCDate, // 6
      TFCController.ruleCheckTFCDateFormat, // 7 & 8
      TFCController.ruleCheckTFCDateConsistence, // 9
      TFCController.ruleCheckTFCRegClimMandatory, // 10
      TFCController.ruleCheckTFCRegClimMismatch,  // 11
      TFCController.ruleCheckTFCWKRDefined, // 12
      TFCController.ruleCheckTFCWKRFormat, // 13
      TFCController.ruleCheckTFCFieldsNumber  // 14
    )

    val readTFCFolder = TFCController.readCsvFiles()
    val TFCFilesWithMeta = readTFCFolder._1

    val checkedTFCFiles   = TFCFilesWithMeta.filter(tfcMeta => !tfcMeta.isAmmissibile) union
      TFCFilesWithMeta.filter(tfcMeta => tfcMeta.isAmmissibile).map(tfcMeta => TFCController.checkTFCCsv(tfcMeta, TFCCsvRules))

    val correctTFCFiles   = checkedTFCFiles.filter(_.isAmmissibile)
    val incorrectTFCFiles = checkedTFCFiles.filter(!_.isAmmissibile)

    // Ora dovrei popolare le tabelle TSG2_TFC_FILE e TSG2_REPORT_AMMISSIBILITA_TFC

    // Popolamento tabella TSG2_TFC_FILE
    val tfcFile = TFCFileDAO.get(TFCFilesWithMeta)
    println()
    tfcFile.show()
    println()
    // Popolamento tabella TSG2_REPORT_AMMISSIBILITA_TFC

    val TFCFilesAndRecords = checkedTFCFiles.map(tfcMeta =>
      if (tfcMeta.isAmmissibile) tfcMeta.copy(csv = getTFCListFromFile(tfcMeta))
      else tfcMeta)

    val TFCFilesAndRecordsChecked = TFCFilesAndRecords.
      map(tfcMeta => tfcMeta.copy(csv = Some(tfcMeta.csv.getOrElse(List()).map(tfcRec => TFCController.checkTFCRecord(tfcRec, TFCRecordRules)))))

    val tfcReportAmmissibilita = ReportAmmissibilitaTFCDao.get(TFCFilesAndRecordsChecked)
    tfcReportAmmissibilita.show()
    println()
    
    TFCController.writeCSVTFCAmmFile(TFCFilesAndRecordsChecked)
  }


  def testPubblicazioneAmmissibilitaTFCDaoGet():Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    Environment.setProperty("csvAmmissibilita.output.path", path + "/TSG2_10238291008")
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "07")


    val TFCFileDAO = new TFCFileDao()
    val ReportAmmissibilitaTFCDao = new ReportAmmissibilitaTFCDao()
    val PubblicazioneAmmissibilitaTFCDao = new PubblicazioneAmmissibilitaTFCDao()

    val TFCCsvRules: List[TFCCsvRule] = List(
      TFCController.ruleCheckTFCPivaRdb, // 3
      TFCController.ruleCheckTFCFileExtension, // 4
      TFCController.ruleCheckTFCFileHeader // 5
    )

    val TFCRecordRules: List[TFCRecordRule] = List(
      TFCController.ruleCheckTFCDate, // 6
      TFCController.ruleCheckTFCDateFormat, // 7 & 8
      TFCController.ruleCheckTFCDateConsistence, // 9
      TFCController.ruleCheckTFCRegClimMandatory, // 10
      TFCController.ruleCheckTFCRegClimMismatch,  // 11
      TFCController.ruleCheckTFCWKRDefined, // 12
      TFCController.ruleCheckTFCWKRFormat, // 13
      TFCController.ruleCheckTFCFieldsNumber  // 14
    )

    val readTFCFolder = TFCController.readCsvFiles()
    val TFCFilesWithMeta = readTFCFolder._1

    val checkedTFCFiles   = TFCFilesWithMeta.filter(tfcMeta => !tfcMeta.isAmmissibile) union
      TFCFilesWithMeta.filter(tfcMeta => tfcMeta.isAmmissibile).map(tfcMeta => TFCController.checkTFCCsv(tfcMeta, TFCCsvRules))

    val correctTFCFiles   = checkedTFCFiles.filter(_.isAmmissibile)
    val incorrectTFCFiles = checkedTFCFiles.filter(!_.isAmmissibile)

    // Ora dovrei popolare le tabelle TSG2_TFC_FILE e TSG2_REPORT_AMMISSIBILITA_TFC

    // Popolamento tabella TSG2_TFC_FILE
    val tfcFile = TFCFileDAO.get(TFCFilesWithMeta)
    println()
    tfcFile.show()
    println()
    // Popolamento tabella TSG2_REPORT_AMMISSIBILITA_TFC

    val TFCFilesAndRecords = checkedTFCFiles.map(tfcMeta =>
      if (tfcMeta.isAmmissibile) tfcMeta.copy(csv = getTFCListFromFile(tfcMeta))
      else tfcMeta)

    val TFCFilesAndRecordsChecked = TFCFilesAndRecords.
      map(tfcMeta => tfcMeta.copy(csv = Some(tfcMeta.csv.getOrElse(List()).map(tfcRec => TFCController.checkTFCRecord(tfcRec, TFCRecordRules)))))

    val dfReportAmmissibilitaTFC = ReportAmmissibilitaTFCDao.get(TFCFilesAndRecordsChecked)
    dfReportAmmissibilitaTFC.show()
    println()

    //tfcReportAmmissibilita.rdd.collect()(0).
    val reportPubblicazioneAmmissibilita = TFCController.writeCSVTFCAmmFile(TFCFilesAndRecordsChecked)
    val dfReportPubblicazioneAmmTFC = Environment.spark.createDataFrame(reportPubblicazioneAmmissibilita)
      .toDF(PubblicazioneAmmissibilitaTFCSchema.CARTELLA_CLOUD, PubblicazioneAmmissibilitaTFCSchema.CSV_FILE_NAME,
        PubblicazioneAmmissibilitaTFCSchema.LAST_MODIFIED, PubblicazioneAmmissibilitaTFCSchema.AMMISSIBILITA_FILE_NAME)
      .dropDuplicates()

    val resultDF = PubblicazioneAmmissibilitaTFCDao.get(dfReportAmmissibilitaTFC, dfReportPubblicazioneAmmTFC)

    val lista   = resultDF.collectAsList()
    val lista2  =  dfReportAmmissibilitaTFC.collectAsList()
    val lista3  = dfReportPubblicazioneAmmTFC.collectAsList()
    println(lista.size())
    println(lista2.size())
    println(lista3.size())

    dfReportPubblicazioneAmmTFC.select(col(PubblicazioneAmmissibilitaTFCSchema.CSV_FILE_NAME)).show(10, false)
    dfReportAmmissibilitaTFC.select(col(ReportAmmissibilitaTFCSchema.nome_file)).show(10, false)
    resultDF.show(10, false)
    /*
    reportPubblicazioneAmmissibilita.foreach(println)

    val DF = Environment.spark.createDataFrame(reportPubblicazioneAmmissibilita).toDF("cartella_cloud", "csv_file_name", "ammissibilita_file_name")
    val pubblicazioneAmmissibilitaTFC = tfcReportAmmissibilita.
      join(DF,tfcReportAmmissibilita(ReportAmmissibilitaTFCSchema.nome_file) ===  DF("csv_file_name"),"inner")
      //.show(false)

    pubblicazioneAmmissibilitaTFC.show(10, false) */
  }




  /*
  def testREADFileVuoto(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    val file = new File("src/test/resources/input/TFC/10238291008_TFC_201907_01 - Copia.csv")

    //val tfcRdd = getTFCListFromFile(file)

    // getTFCRDDFromFile source code

    val buffSourceFile = scala.io.Source.fromFile(file)
    //bufSourceFiles.map(file => file.getLines().toList.sp)

    val rowsList = buffSourceFile.getLines().map(_.split(CSV_DELIMITER)).toList
    println(rowsList.length)
    //rowsList.foreach(l => l.foreach(println))
    //rowsList.slice(1, rowsList.length).map(line => VPG(giornoRiferimento = line(0),C1A1 = line(1),C1A2 = line(2),C1A3 = line(3),C1B1 = line(4), C1B2 = line(5), C1C1 = line(6),C1C2=line(7),C1D1 = line(7),C1E1 = line(8),C1F1 = line(9),))
    //val header = buffSourceFile.getLines().collectFirst()
    //val filteredRows = rowsList.filter(line => line.length )
    //rowsList.foreach(line => println(line.length))

    val rowsListFiltered = rowsList.filter(line => line.length > 1)
    val TFCList = rowsListFiltered.slice(1, rowsList.length).map(line => TFC(fileName = file.getName, fields = line, data = getField(line,0),idRegClimatica = getField(line,1),wkr = getField(line,2)))
    val tfcRDD    = Environment.sparkContext.parallelize(TFCList)

    tfcRDD.foreach(println)
    println(tfcRDD.collect().toList.length)
    val lista = tfcRDD.collect().toList
    val listaReturn: Option[List[TFC]] = if (lista.length > 0) Some(lista) else None

    println(listaReturn)

    println()

    //println(getTFCRDDFromFile2(file))
    //  ------------------------------

    //val buffSourceFile2 = scala.io.Source.fromFile(file)
    //bufSourceFiles.map(file => file.getLines().toList.sp)
    //println("\n")
    //buffSourceFile2.getLines().filter(line => !line.equals("\n")).foreach(line => println(line))//.filter(s => s != "\n").toList//.map(_.split(CSV_DELIMITER)).toList
    //println(rowsList2.length)
    //println("ciao")
  } */

  def testTSGTFCDaoGet():Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    Environment.setProperty("csvAmmissibilita.output.path", path + "/TSG2_10238291008")
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "07")


    val TFCFileDAO = new TFCFileDao()
    val ReportAmmissibilitaTFCDao = new ReportAmmissibilitaTFCDao()
    val PubblicazioneAmmissibilitaTFCDao = new PubblicazioneAmmissibilitaTFCDao()

    val TFCCsvRules: List[TFCCsvRule] = List(
      TFCController.ruleCheckTFCPivaRdb, // 3
      TFCController.ruleCheckTFCFileExtension, // 4
      TFCController.ruleCheckTFCFileHeader, // 5
      TFCController.ruleCheckTFCFileIntegrity
    )

    val TFCRecordRules: List[TFCRecordRule] = List(
      TFCController.ruleCheckTFCDate, // 6
      TFCController.ruleCheckTFCDateFormat, // 7 & 8
      TFCController.ruleCheckTFCDateConsistence, // 9
      TFCController.ruleCheckTFCRegClimMandatory, // 10
      TFCController.ruleCheckTFCRegClimMismatch, // 11
      TFCController.ruleCheckTFCWKRDefined, // 12
      TFCController.ruleCheckTFCWKRFormat, // 13
      TFCController.ruleCheckTFCFieldsNumber // 14
    )

    val readTFCFolder = TFCController.readCsvFiles()
    val TFCFilesWithMeta = readTFCFolder._1

    val checkedTFCFiles = TFCFilesWithMeta.filter(tfcMeta => !tfcMeta.isAmmissibile) union
      TFCFilesWithMeta.filter(tfcMeta => tfcMeta.isAmmissibile).map(tfcMeta => TFCController.checkTFCCsv(tfcMeta, TFCCsvRules))

    val correctTFCFiles = checkedTFCFiles.filter(_.isAmmissibile)
    val incorrectTFCFiles = checkedTFCFiles.filter(!_.isAmmissibile)

    // Ora dovrei popolare le tabelle TSG2_TFC_FILE e TSG2_REPORT_AMMISSIBILITA_TFC

    // Popolamento tabella TSG2_TFC_FILE
    val tfcFile = TFCFileDAO.get(TFCFilesWithMeta)
    println()
    tfcFile.show()
    println()
    // Popolamento tabella TSG2_REPORT_AMMISSIBILITA_TFC

    val TFCFilesAndRecords = checkedTFCFiles.map(tfcMeta =>
      if (tfcMeta.isAmmissibile) tfcMeta.copy(csv = getTFCListFromFile(tfcMeta))
      else tfcMeta)

    val TFCFilesAndRecordsChecked = TFCFilesAndRecords.
      map(tfcMeta => tfcMeta.copy(csv = Some(tfcMeta.csv.getOrElse(List()).map(tfcRec => TFCController.checkTFCRecord(tfcRec, TFCRecordRules)))))

    val dfReportAmmissibilitaTFC = ReportAmmissibilitaTFCDao.get(TFCFilesAndRecordsChecked)
    dfReportAmmissibilitaTFC.show()
    println()

    val TSGTFCDao = new TSGTFCDao()
    val resultDf = TSGTFCDao.get(dfReportAmmissibilitaTFC).show(20, false)

  }

  def testProva(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    println("abba".hashCode())

    val file = new File("src/test/resources/input/TSG2_10238291008/2020/03/10238291008_TFC_202002_1.csv")
    println(file.getPath)
    println(file.getParentFile.getPath)
    println(file.getAbsolutePath)
    println(file.getCanonicalPath)

    println(getAmmissibilitaCsvNameTest(file.getName, Environment.startDateTime, "2021", "10"))

  }




  // Test regole a livello di RECORD
  /*
  def testRuleCheckTFCDate(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val tfcMetadata = TFCController.readCsv("src/test/resources/input/TFC/10238291008_TFC_202211_2.csv")
    println("\n\nSTAMPA\n\n")
    println(sc.parallelize(List(2, 3, 4)))
    val firstRecord = tfcMetadata.csv.first() //Campo data nel primo record appositamente non valorizzata nel file.
    println(firstRecord)
    println(TFCController.ruleCheckTFCDate.condition(firstRecord))
    println(Properties.isRuleCheckTFCFileHeaderEnabled)

    val records = tfcMetadata.csv.collect()
    println(records(2))
    println(TFCController.ruleCheckTFCDate.condition(records(2)))
    println(records(2).fileName)
    println(records(2).data.get.split("/").slice(1,3).mkString)//.flatMap(el => el))
  }

  def testRuleCheckTFCDateFormat(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val tfcMetadata = TFCController.readCsv("src/test/resources/input/TFC/10238291008_TFC_202210_1 - Copia.csv")

    val firstRecord = tfcMetadata.csv.first() //Campo data nel primo record appositamente non valorizzata nel file.
    println(firstRecord)
    println(TFCController.ruleCheckTFCDateFormat.condition(firstRecord))
    //println(Properties.isRuleCheckTFCFileHeaderEnabled)

    val records = tfcMetadata.csv.collect()
    println(records(1))
    println(TFCController.ruleCheckTFCDateFormat.condition(records(1)))

    println(TFCController.isDate(Option("11/1000/2011")))

    //println(Constants.ID_REG_CLIM_VALUES.foreach(println))//Constants.ID_REG_CLIM_VALUES.contains("11", _))

    println(Constants.ID_REG_CLIM_VALUES.contains(records(1).idRegClimatica.get))
    println(Constants.ID_REG_CLIM_VALUES.contains("10"))
    println(Constants.ID_REG_CLIM_VALUES.contains("14"))
    //println(Constants.ID_REG_CLIM_VALUES_2
    val a = ("key" -> "value","key2" -> "value2","key3" -> "value3")
    val l = List("key" -> "value","key2" -> "value2","key3" -> "value3")
    val lMap = l.toMap
    val b = ("key", "value")
    println(a.getClass, b.getClass)
    println(lMap.get("key2"))
    println(lMap.get("key3").getOrElse(null))
    println(lMap.get("key4").getOrElse(null))

    val ingredient = ("Sugar", 25)
    println(ingredient._1)
  }

  def testRules(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val tfcMetadata = TFCController.readCsv("src/test/resources/input/TFC/10238291008_TFC_202210_1 - Copia.csv")
    val yearMonth = tfcMetadata.csv.collect()(4).data.get.replace("/", "")//.slice(2,6)
    println(tfcMetadata.file.getName().split("_")(2) )
    println(tfcMetadata.csv.collect()(4))
    val tmpArr = tfcMetadata.csv.collect()(4).data.get.split("/")
    val yearMonth2 = tmpArr(2) + tmpArr(1)
    println(yearMonth2)
  }

  def testRuleCheckTFCDateConsistence(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val tfcMetadata = TFCController.readCsv("src/test/resources/input/TFC/10238291008_TFC_202210_1 - Copia.csv")

    //Campo data nel secondo record appositamente non valorizzato nel file.
    val csvarray = tfcMetadata.csv.collect()
    println(csvarray(2).data.get)
    println(TFCController.ruleCheckTFCDateConsistence.condition(csvarray(1)))
    println(TFCController.ruleCheckTFCDateConsistence.condition(csvarray(2)))
  }

  def testRuleCheckTFCRegClimDefined(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val tfcMetadata = TFCController.readCsv("src/test/resources/input/TFC/10238291008_TFC_202210_1 - Copia.csv")

    //Campo IdRegClim nel quarto record appositamente non valorizzato.
    val csvarray = tfcMetadata.csv.collect()
    println(csvarray(3))

    println(TFCController.ruleCheckTFCRegClimMandatory.condition(csvarray(3))) // Should be false.
    println(TFCController.ruleCheckTFCRegClimMandatory.condition(csvarray(1))) // Should be true.
    println(TFCController.ruleCheckTFCRegClimMandatory.condition(csvarray(2))) // Should be true.
  }

  def testRuleCheckTFCRegClimMismatch(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val tfcMetadata = TFCController.readCsv("src/test/resources/input/TFC/10238291008_TFC_202210_1 - Copia.csv")

    //Campo IdRegClim nel quinto record appositamente settato con un valore non coerente a quelli in lista.
    val csvarray = tfcMetadata.csv.collect()
    println(csvarray(3))

    println(TFCController.ruleCheckTFCRegClimMismatch.condition(csvarray(1))) // Should be true.
    println(TFCController.ruleCheckTFCRegClimMismatch.condition(csvarray(2))) // Should be true.
    println(TFCController.ruleCheckTFCRegClimMismatch.condition(csvarray(4))) // Should be false.
  }

  def testRuleCheckTFCWKRDefined(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val tfcMetadata = TFCController.readCsv("src/test/resources/input/TFC/10238291008_TFC_202210_1 - Copia.csv")

    //Campo WKR nel sesto record appositamente non valorizzato nel file.
    val csvarray = tfcMetadata.csv.collect()
    println(csvarray(5))

    println(TFCController.ruleCheckTFCWKRDefined.condition(csvarray(1))) // Should be true.
    println(TFCController.ruleCheckTFCWKRDefined.condition(csvarray(2))) // Should be true.
    println(TFCController.ruleCheckTFCWKRDefined.condition(csvarray(4))) // Should be true.
    println(TFCController.ruleCheckTFCWKRDefined.condition(csvarray(5))) // Should be false.
  }

  def testRuleCheckTFCWKRFormat(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val tfcMetadata = TFCController.readCsv("src/test/resources/input/TFC/10238291008_TFC_202210_1 - Copia.csv")

    //Campo WKR nel settimo record appositamente valorizzato con un valore non decimale.
    val csvarray = tfcMetadata.csv.collect()
    println(csvarray(6))

    println(TFCController.ruleCheckTFCWKRFormat.condition(csvarray(5))) // Should be false.
    println(TFCController.ruleCheckTFCWKRFormat.condition(csvarray(6))) // Should be false.
    println(TFCController.ruleCheckTFCWKRFormat.condition(csvarray(7))) // Should be true.

  }

  def testRuleCheckTFCFields(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val tfcMetadata = TFCController.readCsv("src/test/resources/input/TFC/TFC FILE INCORRECT.csv")

    println(tfcMetadata.csv.collect()(2))

    println("We")

  }

  def test(): Unit = {
    println("We")
  }

  def testRuleCheckRULES(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    println("Prova")
    println("Prova2")

    val rdd = sc.parallelize(List(1,2,3,4,5,6))

    //rdd.collect.foreach(println)


    Environment.sparkContext.parallelize(List(1,3,4)).collect.foreach(println)


  }
*/

}
