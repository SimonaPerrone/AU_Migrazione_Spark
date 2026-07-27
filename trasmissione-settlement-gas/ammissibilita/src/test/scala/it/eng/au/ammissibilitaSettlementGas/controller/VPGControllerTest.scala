package it.eng.au.ammissibilitaSettlementGas.controller

import it.eng.au.ammissibilitaSettlementGas.controller.VPGController._
import it.eng.au.ammissibilitaSettlementGas.dao.{PubblicazioneAmmissibilitaVPGDao, ReportAmmissibilitaVPGDao, TSGVPGDao, VPGFileDao}
import it.eng.au.ammissibilitaSettlementGas.schema.PubblicazioneAmmissibilitaVPGSchema
//import it.eng.au.ammissibilitaSettlementGas
import it.eng.au.ammissibilitaSettlementGas.model.rules.{VPGCsvRule, VPGRecordRule}
import it.eng.au.ammissibilitaSettlementGas.schema.VPGFileSchema
import it.eng.au.ammissibilitaSettlementGas.utility.EnvironmentSparkTest
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.commons.io.FilenameUtils.removeExtension
import org.apache.spark.sql.functions.hash
import org.apache.spark.sql.types.LongType

import java.io.File
import scala.util.matching.Regex

class VPGControllerTest extends EnvironmentSparkTest {
  val path = "src/test/resources/input/TSG2"

  def testReadAndCheckCsvFiles():Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    Environment.setProperty("current.year", "2023")
    Environment.setProperty("current.month", "04")

    val VPGFilesWithMeta = VPGController.readCsvFiles()

    VPGFilesWithMeta.foreach(x => println(x.file))

    val VPGCsvRules: List[VPGCsvRule] = List(
      //ruleCheckVPGFileName, // 2
      ruleCheckVPGPivaRdb, // 3
      ruleCheckVPGFileExtension, // 4
      ruleCheckVPGFileHeader // 5
    )

    val VPGRecordRules: List[VPGRecordRule] = List(
      ruleCheckVPGDate, // 6 & 7
      ruleCheckVPGDateConsistence, // 8
      ruleCheckVPGMandatoryFields, // 9
      ruleCheckVPGFieldsFormat  // 10
      //ruleCheckVPGFieldsNumber  // 11
    )

    val checkedVPGFiles   = VPGFilesWithMeta.filter(vpgMeta => vpgMeta.isAmmissibile).map(vpgMeta => VPGController.checkVPGCsv(vpgMeta, VPGCsvRules))
    val correctVPGFiles   = checkedVPGFiles.filter(_.isAmmissibile)
    val incorrectVPGFiles = checkedVPGFiles.filter(!_.isAmmissibile) union VPGFilesWithMeta.filter(vpgMeta => !vpgMeta.isAmmissibile)


    println("Files Testati")
    checkedVPGFiles.foreach(println(_))
    println("Files Corretti")
    correctVPGFiles.foreach(println(_))
    println("Files NON Corretti")
    incorrectVPGFiles.foreach(println(_))

    val VPGFilesAndRecords = correctVPGFiles.map(vpgMeta => vpgMeta.copy(csv = VPGController.getVPGListFromFile(vpgMeta.file)))
    val VPGRecords = VPGFilesAndRecords.flatMap(f => f.csv.getOrElse(List()))

    val VPGRecordsChecked = VPGRecords.map(vpgRec => VPGController.checkVPGRecord(vpgRec, VPGRecordRules))
    println()

    //println(VPGFilesAndRecords.first().csv.getOrElse(null)(0))
    println()
    println(VPGRecordsChecked.collect()(0))
    println(VPGRecordsChecked.collect()(1))
    println(VPGRecordsChecked.collect()(2))
    println(VPGRecordsChecked.collect()(3))
    println(VPGRecordsChecked.collect()(4))
    println(VPGRecordsChecked.collect()(5))
    println(VPGRecordsChecked.collect()(6))
    println(VPGRecordsChecked.collect()(19))

  }


  def testVPGFileDaoGet():Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "08")

    val VPGFileDAO = new VPGFileDao()
    val VPGFilesWithMeta = VPGController.readCsvFiles()

    VPGFilesWithMeta.foreach(x => println(x.file))
    val vpgFile = VPGFileDAO.get(VPGFilesWithMeta)

    vpgFile.show(10)    // Sembra funzionare per bene.
  }

  def testReportAmmissibilitaVPGDaoGet():Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "08")

    val VPGCsvRules: List[VPGCsvRule] = List(
      //ruleCheckVPGFileName, // 2
      ruleCheckVPGPivaRdb, // 3
      ruleCheckVPGFileExtension, // 4
      ruleCheckVPGFileHeader // 5
    )

    val VPGRecordRules: List[VPGRecordRule] = List(
      ruleCheckVPGDate, // 6 & 7
      ruleCheckVPGDateConsistence, // 8
      ruleCheckVPGMandatoryFields, // 9
      ruleCheckVPGFieldsFormat,  // 10
      ruleCheckVPGFieldsNumber  // 11
    )

    val ReportAmmissibilitaVPGDAO = new ReportAmmissibilitaVPGDao()
    val VPGFilesWithMeta = VPGController.readCsvFiles()

    val VPGFilesChecked = VPGFilesWithMeta.filter(vpgMeta => !vpgMeta.isAmmissibile) union
      VPGFilesWithMeta.filter(vpgMeta => vpgMeta.isAmmissibile).map(vpgMeta => checkVPGCsv(vpgMeta, VPGCsvRules))

    val VPGFilesAndRecords = VPGFilesChecked.map(vpgMeta =>
      if (vpgMeta.isAmmissibile) vpgMeta.copy(csv = getVPGListFromFile(vpgMeta.file))
      else vpgMeta)

    val VPGFilesAndRecordsChecked = VPGFilesAndRecords.
      map(vpgMeta => vpgMeta.copy(csv = Some(vpgMeta.csv.getOrElse(List()).map(vpgRec => VPGController.checkVPGRecord(vpgRec, VPGRecordRules)))))

    VPGFilesWithMeta.foreach(x => println(x.file))

   val resultDF =  ReportAmmissibilitaVPGDAO.get(VPGFilesAndRecordsChecked)

    resultDF.show(100)
    //foreach(println)

    //val vpgFile = VPGFileDAO.get(VPGFilesWithMeta)

    //vpgFile.show(10)    // Sembra funzionare per bene.
  }

  def testWriteCSVVPGAmmFile(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    Environment.setProperty("csvAmmissibilita.output.path", path + "/TSG2_10238291008")
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "09")

    val VPGCsvRules: List[VPGCsvRule] = List(
      //ruleCheckVPGFileName, // 2
      ruleCheckVPGPivaRdb, // 3
      ruleCheckVPGFileExtension, // 4
      ruleCheckVPGFileHeader // 5
    )

    val VPGRecordRules: List[VPGRecordRule] = List(
      ruleCheckVPGDate, // 6 & 7
      ruleCheckVPGDateConsistence, // 8
      ruleCheckVPGMandatoryFields, // 9
      ruleCheckVPGFieldsFormat,  // 10
      ruleCheckVPGFieldsNumber  // 11
    )


    val VPGFilesWithMeta = VPGController.readCsvFiles()

    val checkedVPGFiles = VPGFilesWithMeta.filter(vpgMeta => !vpgMeta.isAmmissibile).
      union(VPGFilesWithMeta.filter(vpgMeta => vpgMeta.isAmmissibile).map(vpgMeta => checkVPGCsv(vpgMeta, VPGCsvRules)))

    val incorrectVPGFiles = checkedVPGFiles.filter(!_.isAmmissibile)
    val correctVPGFiles   = checkedVPGFiles.filter(_.isAmmissibile)

    // ORA DOVREI POPOLARE LE TABELLE "VPG_FILE" & "REPORT_AMMISSIBILITA_VPG"
    val VPGFileDAO = new VPGFileDao()
    val ReportAmmissibilitaVPGDao = new ReportAmmissibilitaVPGDao()

    val vpgFile = VPGFileDAO.get(VPGFilesWithMeta)
    println()
    vpgFile.show()
    println()

    val VPGFilesAndRecords = checkedVPGFiles.map(vpgMeta =>
      if (vpgMeta.isAmmissibile) vpgMeta.copy(csv = getVPGListFromFile(vpgMeta.file))
      else vpgMeta)

    val VPGFilesAndRecordsChecked = VPGFilesAndRecords.
      map(vpgMeta => vpgMeta.copy(csv = Some(vpgMeta.csv.getOrElse(List()).map(vpgRec => VPGController.checkVPGRecord(vpgRec, VPGRecordRules)))))

    val vpgReportAmmissibilita = ReportAmmissibilitaVPGDao.get(VPGFilesAndRecordsChecked)
    vpgReportAmmissibilita.show()
    println()

    VPGController.writeCSVVPGAmmFile(VPGFilesAndRecordsChecked)
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
    val currentMonth = "08"
    println()
    val files_rdd = years_rdd.flatMap(_.listFiles().filter(f => f.isDirectory && f.getName == currentYear)) // && f.getName == "2020"))
      .flatMap(_.listFiles().filter(f => f.isDirectory && f.getName == currentMonth))
      .flatMap(_.listFiles().filter(f => f.isFile && f.getName.contains("VPG")))

    files_rdd.foreach(println(_))
    println()

    val files_rdd_2 = years_rdd.flatMap(_.listFiles().filter(f => f.isDirectory))
    files_rdd_2.foreach(println(_))
    println()
    //Environment.setProperty("year.month", "202210")
    Environment.setProperty("csv.input.path", path)
    Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.month", "08")

    val VPGMetadataRDD = VPGController.readCsvFiles()
    println("\nEccoli i files VPG")
    VPGMetadataRDD.foreach(x => println(x.file.getName))
  }


  def testVPGPubblicazioneAmmissibilitaDaoGet(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    Environment.setProperty("csvAmmissibilita.output.path", path + "/TSG2_10238291008")
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "09")

    val VPGCsvRules: List[VPGCsvRule] = List(
      //ruleCheckVPGFileName, // 2
      ruleCheckVPGPivaRdb, // 3
      ruleCheckVPGFileExtension, // 4
      ruleCheckVPGFileHeader // 5
    )

    val VPGRecordRules: List[VPGRecordRule] = List(
      ruleCheckVPGDate, // 6 & 7
      ruleCheckVPGDateConsistence, // 8
      ruleCheckVPGMandatoryFields, // 9
      ruleCheckVPGFieldsFormat,  // 10
      ruleCheckVPGFieldsNumber  // 11
    )

    val VPGFilesWithMeta = VPGController.readCsvFiles()

    val checkedVPGFiles = VPGFilesWithMeta.filter(vpgMeta => !vpgMeta.isAmmissibile).
      union(VPGFilesWithMeta.filter(vpgMeta => vpgMeta.isAmmissibile).map(vpgMeta => checkVPGCsv(vpgMeta, VPGCsvRules)))

    val incorrectVPGFiles = checkedVPGFiles.filter(!_.isAmmissibile)
    val correctVPGFiles   = checkedVPGFiles.filter(_.isAmmissibile)

    // ORA DOVREI POPOLARE LE TABELLE "VPG_FILE" & "REPORT_AMMISSIBILITA_VPG"
    val VPGFileDAO = new VPGFileDao()
    val ReportAmmissibilitaVPGDao = new ReportAmmissibilitaVPGDao()
    val pubblicazioneAmmissibilitaVPGDao = new PubblicazioneAmmissibilitaVPGDao()

    val vpgFile = VPGFileDAO.get(VPGFilesWithMeta)
    println()
    vpgFile.show(10,false)
    println()

    val VPGFilesAndRecords = checkedVPGFiles.map(vpgMeta =>
      if (vpgMeta.isAmmissibile) vpgMeta.copy(csv = getVPGListFromFile(vpgMeta.file))
      else vpgMeta)

    val VPGFilesAndRecordsChecked = VPGFilesAndRecords.
      map(vpgMeta => vpgMeta.copy(csv = Some(vpgMeta.csv.getOrElse(List()).map(vpgRec => VPGController.checkVPGRecord(vpgRec, VPGRecordRules)))))

    val dfReportAmmissibilitaVPG = ReportAmmissibilitaVPGDao.get(VPGFilesAndRecordsChecked)
    dfReportAmmissibilitaVPG.show(10, false)
    println()

    val reportPubblicazioneAmmissibilita = VPGController.writeCSVVPGAmmFile(VPGFilesAndRecordsChecked)
    val dfReportPubblicazioneAmmVPG = Environment.spark.createDataFrame(reportPubblicazioneAmmissibilita)
      .toDF(PubblicazioneAmmissibilitaVPGSchema.CARTELLA_CLOUD, PubblicazioneAmmissibilitaVPGSchema.CSV_FILE_NAME,
        PubblicazioneAmmissibilitaVPGSchema.AMMISSIBILITA_FILE_NAME).dropDuplicates()

    pubblicazioneAmmissibilitaVPGDao.get(dfReportAmmissibilitaVPG, dfReportPubblicazioneAmmVPG).show(10,false)

  }

  def testTSGVPGDaoGet(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    Environment.setProperty("csvAmmissibilita.output.path", path + "/TSG2_10238291008")
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "08")


    val VPGCsvRules: List[VPGCsvRule] = List(
      //ruleCheckVPGFileName, // 2
      ruleCheckVPGPivaRdb, // 3
      ruleCheckVPGFileExtension, // 4
      ruleCheckVPGFileHeader, // 5
      ruleCheckVPGFileIntegrity
    )

    val VPGRecordRules: List[VPGRecordRule] = List(
      ruleCheckVPGDate, // 6 & 7
      ruleCheckVPGDateConsistence, // 8
      ruleCheckVPGMandatoryFields, // 9
      ruleCheckVPGFieldsFormat,  // 10
      ruleCheckVPGFieldsNumber  // 11
    )

    val VPGFilesWithMeta = VPGController.readCsvFiles()

    val checkedVPGFiles = VPGFilesWithMeta.filter(vpgMeta => !vpgMeta.isAmmissibile).
      union(VPGFilesWithMeta.filter(vpgMeta => vpgMeta.isAmmissibile).map(vpgMeta => checkVPGCsv(vpgMeta, VPGCsvRules)))

    val incorrectVPGFiles = checkedVPGFiles.filter(!_.isAmmissibile)
    val correctVPGFiles   = checkedVPGFiles.filter(_.isAmmissibile)

    // ORA DOVREI POPOLARE LE TABELLE "VPG_FILE" & "REPORT_AMMISSIBILITA_VPG"
    val VPGFileDAO = new VPGFileDao()
    val ReportAmmissibilitaVPGDao = new ReportAmmissibilitaVPGDao()
    val pubblicazioneAmmissibilitaVPGDao = new PubblicazioneAmmissibilitaVPGDao()

    val vpgFile = VPGFileDAO.get(VPGFilesWithMeta)
    println()
    vpgFile.show(10,false)
    println()

    val VPGFilesAndRecords = checkedVPGFiles.map(vpgMeta =>
      if (vpgMeta.isAmmissibile) vpgMeta.copy(csv = getVPGListFromFile(vpgMeta.file))
      else vpgMeta)

    val VPGFilesAndRecordsChecked = VPGFilesAndRecords.
      map(vpgMeta => vpgMeta.copy(csv = Some(vpgMeta.csv.getOrElse(List()).map(vpgRec => VPGController.checkVPGRecord(vpgRec, VPGRecordRules)))))

    val dfReportAmmissibilitaVPG = ReportAmmissibilitaVPGDao.get(VPGFilesAndRecordsChecked)
    dfReportAmmissibilitaVPG.show(10, false)
    println()

    val TSGVPGDao = new TSGVPGDao()
    val dfResult = TSGVPGDao.get(dfReportAmmissibilitaVPG).show(20, false)

  }
  def testHashCodeImplementation():Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "08")


    val VPGFilesWithMeta = VPGController.readCsvFiles()

    VPGFilesWithMeta.foreach(x => println(x.file))

    val VPGCsvRules: List[VPGCsvRule] = List(
      //ruleCheckVPGFileName, // 2
      ruleCheckVPGPivaRdb, // 3
      ruleCheckVPGFileExtension, // 4
      ruleCheckVPGFileHeader // 5
    )

    val VPGRecordRules: List[VPGRecordRule] = List(
      ruleCheckVPGDate, // 6 & 7
      ruleCheckVPGDateConsistence, // 8
      ruleCheckVPGMandatoryFields, // 9
      ruleCheckVPGFieldsFormat  // 10
      //ruleCheckVPGFieldsNumber  // 11
    )

    val checkedVPGFiles   = VPGFilesWithMeta.filter(vpgMeta => vpgMeta.isAmmissibile).map(vpgMeta => VPGController.checkVPGCsv(vpgMeta, VPGCsvRules))
    val correctVPGFiles   = checkedVPGFiles.filter(_.isAmmissibile)
    val incorrectVPGFiles = checkedVPGFiles.filter(!_.isAmmissibile) union VPGFilesWithMeta.filter(vpgMeta => !vpgMeta.isAmmissibile)

    val rdd3 = checkedVPGFiles.map(vpgMetaRow => {(vpgMetaRow.file.getName, vpgMetaRow.yearDir, vpgMetaRow.monthDir, vpgMetaRow.annoTermico, vpgMetaRow.lastModified)})
    rdd3.foreach(println)

    val df = Environment.spark.createDataFrame(rdd3).toDF("nomeFile", "anno", "mese", VPGFileSchema.annotermico, VPGFileSchema.data_creazione)
    println(df.show(3))
    val newDf = df.withColumn("n_id_tsg2_file", hash(df.col("nomeFile"), df.col("anno"), df.col("mese")).cast(LongType) + Int.MaxValue)
    println(newDf.show(3))

    println(newDf.select("n_id_tsg2_file").show(3))
    println(newDf.select(newDf.col("n_id_tsg2_file")).show(3))

    //dfFiles.show(1)
    /*
    println("Files Testati")
    checkedVPGFiles.foreach(println(_))
    println("Files Corretti")
    correctVPGFiles.foreach(println(_))
    println("Files NON Corretti")
    incorrectVPGFiles.foreach(println(_))

    val VPGFilesAndRecords = correctVPGFiles.map(vpgMeta => vpgMeta.copy(csv = VPGController.getVPGRDDFromFile(vpgMeta.n_id_tsg2_file, vpgMeta.file)))
    val VPGRecords = VPGFilesAndRecords.flatMap(f => f.csv.getOrElse(List()))

    val VPGRecordsChecked = VPGRecords.map(vpgRec => VPGController.checkVPGRecord(vpgRec, VPGRecordRules))
    println() */



    // Proviamo con un esempio:
    val fileName = "10238291008_TFC_202001_01.csv"
    val year = "2002"
    //println(year.toInt)
    val month = "02"
    println(hashCode("10238291008_TFC_202001_01.csv" + year + month))
  }

  def hashCode(string: String): Long = {
    // Convertiamo ogni carattere in numerico tramite tabella ASCII per poi effettuare una somma.
    string.map(char => char.toLong).toList.sum
    /*
    val we = new File("")
    Environment.spark.emptyDataFrame
      .withColumn("hash", hash(lit(annoMese), col(path), col(name)))
     */

  }

  /*
  def testReadCsv(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val vpgMetadata = VPGController.readCsv("src/test/resources/input/VPG/10238291008_VPG_2018_1_v4.csv")
    vpgMetadata.csv.foreach(println)
  }

  def testRuleCheckVPGFileHeader(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val vpgMetadataCorrect = VPGController.readCsv("src/test/resources/input/VPG/10238291008_VPG_2018_1_v4.csv")
    val vpgMetadataIncorrect = VPGController.readCsv("src/test/resources/input/VPG/10238291008_VPG_2018_1_v4 - Copia - Errato FileHeader.csv")
    println(VPGController.ruleCheckVPGFileHeader.condition(vpgMetadataCorrect))   // Should be true
    println(VPGController.ruleCheckVPGFileHeader.condition(vpgMetadataIncorrect)) // Should be false
    //println("PRovsasodkfjh")
    //println(Properties.isRuleCheckTFCFileHeaderEnabled)
  }

  def testRuleCheckVPGFileExtension(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val vpgMetadata = VPGController.readCsv("src/test/resources/input/VPG/10238291008_VPG_2018_1_v4.csv")
    println(VPGController.ruleCheckVPGFileExtension.condition(vpgMetadata))
  } */

  def testRules(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    val VPGFileRegex: Regex = "^([A-Za-z0-9]+)_(VPG)_(\\d{8})_(\\d+)\\.(?i)([A-Za-z0-9]+)".r

    val name = "10238291008_VPG_20212022_01 - Copia.csv" //"10238291008_VPG_20192020_02 - Copia.csv"

    println(removeExtension(name))
    name match {
      case VPGFileRegex(pivaRdb, vpg, annoterm, progressivo, estenzione) => println(pivaRdb, vpg, annoterm, progressivo, estenzione)
      case _ => println("Errore")
    }
  }
/*
  def testRuleCheckVPGDate(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val vpgMetadata = VPGController.readCsv("src/test/resources/input/VPG/10238291008_VPG_20222023_01 - Copia.csv")

    val records = vpgMetadata.csv.collect()
    // Abbiamo appositamente modificato i primi tre records settando "giornoRiferimento" con una data non valida.
    println(VPGController.ruleCheckVPGDate.condition(vpgMetadata.csv.first()))  // Should be false
    println(VPGController.ruleCheckVPGDate.condition(records(1)))               // Should be false
    println(VPGController.ruleCheckVPGDate.condition(records(2))) // Should be false
    println(VPGController.ruleCheckVPGDate.condition(records(3))) // Should be true

    // In realtà questo controllo implementa anche il controllo sulla data valida (ID 07), come dimostrato di seguito:
    println(VPGController.ruleCheckVPGDate.condition(records(5))) // Should be false
    println(VPGController.ruleCheckVPGDate.condition(records(6))) // Should be false
    println(VPGController.ruleCheckVPGDate.condition(records(7))) // Should be false
    println(VPGController.ruleCheckVPGDate.condition(records(8))) // Should be false
    println(VPGController.ruleCheckVPGDate.condition(records(9))) // Should be false
  }

  def testRuleCheckVPGDateConsistence(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val vpgMetadata = VPGController.readCsv("src/test/resources/input/VPG/10238291008_VPG_20222023_01 - Copia.csv")

    val records = vpgMetadata.csv.collect()
    println(VPGController.ruleCheckVPGDateConsistence.condition(records(5))) // Should be true
    println(VPGController.ruleCheckVPGDateConsistence.condition(records(7))) // Should be false
    println(VPGController.ruleCheckVPGDateConsistence.condition(records(107))) // Should be true
  }

  def testRuleCheckVPGMandatoryFields(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val vpgMetadata = VPGController.readCsv("src/test/resources/input/VPG/10238291008_VPG_20222023_01 - Copia.csv")

    val records = vpgMetadata.csv.collect()
    println(VPGController.ruleCheckVPGMandatoryFields.condition(records(5))) // Should be true
    println(VPGController.ruleCheckVPGMandatoryFields.condition(records(7))) // Should be true
    println(VPGController.ruleCheckVPGMandatoryFields.condition(records(107))) // Should be true
    println(VPGController.ruleCheckVPGMandatoryFields.condition(records(8))) // Should be false
    println(VPGController.ruleCheckVPGMandatoryFields.message.statusMessage)
    println(VPGController.ruleCheckVPGMandatoryFields.condition(records(3))) // Should be false
    println(VPGController.ruleCheckVPGMandatoryFields.message.statusMessage)
  }

  def testRuleCheckVPGFieldsFormat(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    val vpgMetadata = VPGController.readCsv("src/test/resources/input/VPG/10238291008_VPG_20222023_01 - Copia.csv")

    val records = vpgMetadata.csv.collect()
    println(VPGController.ruleCheckVPGFieldsFormat.condition(records(5))) // Should be true
    println(VPGController.ruleCheckVPGFieldsFormat.condition(records(7))) // Should be true
    println(VPGController.ruleCheckVPGFieldsFormat.condition(records(107))) // Should be true
    println(VPGController.ruleCheckVPGFieldsFormat.condition(records(8))) // Should be false
    println(VPGController.ruleCheckVPGFieldsFormat.message.statusMessage)
    println(VPGController.ruleCheckVPGFieldsFormat.condition(records(11))) // Should be false
    println(VPGController.ruleCheckVPGFieldsFormat.message.statusMessage)
    println(VPGController.ruleCheckVPGFieldsFormat.condition(records(12))) // Should be false
    println(VPGController.ruleCheckVPGFieldsFormat.message.statusMessage)
  }
*/
  def testProva(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")
    val SQLContext = Environment.sqlContext

    println("Ciao")

    Environment.setProperty("csv.input.path", path)
    Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.month", "09")

    val VPGMetadataRDD = VPGController.readCsvFiles()
    println("\nEccoli i files VPG")
    VPGMetadataRDD.foreach(x => println(x.file.getName))


    val tfcMetaWithRecords = VPGMetadataRDD.map(tfcMeta => tfcMeta.copy(csv = getVPGListFromFile(tfcMeta.file)))

    println(tfcMetaWithRecords.first().csv.get(0).fields.length)
    println(tfcMetaWithRecords.first().csv.get(0))
    println(tfcMetaWithRecords.first().csv.get(1))
    println(tfcMetaWithRecords.first().csv.get(0).fields(1))

    println(ruleCheckVPGFieldsNumber.condition(tfcMetaWithRecords.first().csv.get(0)))
    println(ruleCheckVPGFieldsNumber.condition(tfcMetaWithRecords.first().csv.get(1)))
  }
}
