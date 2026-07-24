package it.au.misure.ingestionMisureGasUnico.validate

import java.io.File
import java.time.LocalDateTime
import com.typesafe.config.ConfigFactory
import it.au.misure.ingestionMisureGasUnico.model.{GasUnzipMetadata, GasXmlMetadata}
import it.au.misure.ingestionMisureGasUnico.utility.Constants.IGMG
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import it.au.misure.ingestionMisureGasUnico.utility.{Constants, EnvironmentSparkTest, FileUtility}
import junit.framework.TestCase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.Assert

import java.sql.Timestamp
import scala.collection.mutable

class TestValidateFile extends /*TestCase with SparkLocal*/ EnvironmentSparkTest {
  def testValidate():Unit={

    val srcPath = ConfigFactory.load.getString("tempRootPath")+"/xx_yy_01234567890"
    val subFoldersMask = "/*/*"

    deleteCsvs(srcPath)  //delete all the old ReportEsitoFile.csv files

    val inputRdd = if(System.getProperty("os.name").toLowerCase.contains("windows")) {
      //LOAD FILE ON WINDOWS TEST ENV
      Environment.getSpark.sparkContext.wholeTextFiles(srcPath + subFoldersMask)
        .map(textData => FileUtility.xmlToMetadata(new File(textData._1.replace("file:/", "")))) //replace() is needed only to test on windows
    } else {
      Environment.getSpark.sparkContext.wholeTextFiles(srcPath + subFoldersMask)
        .map(textData => FileUtility.xmlToMetadata(new File(textData._1)))
    }
    
    Assert.assertFalse(inputRdd.isEmpty()) //assuming test folder is populated

    val inputUnzipRdd = convertToGasUnzipMetadata(inputRdd)
    val inputUnzipRddStandard = inputUnzipRdd.filter(!_.flusso.equalsIgnoreCase(IGMG))
    val inputUnzipRddIGMG = inputUnzipRdd.filter(_.flusso.equalsIgnoreCase(IGMG))

    val udDActivePeriodsMap = mutable.Map("01234567890"->(LocalDateTime.MIN,LocalDateTime.MAX))
    inputUnzipRdd.foreach(meta=>{
      udDActivePeriodsMap(meta.pivaUtente) = (LocalDateTime.MIN,LocalDateTime.MAX)
    })

    val validateRdd = ValidateFileStandard.calcReportMessages(inputUnzipRddStandard, udDActivePeriodsMap.toMap)
    Assert.assertFalse(validateRdd.isEmpty())

    ValidateFileStandard.writeAmmissibilitaReportsCsv(validateRdd, Timestamp.valueOf(LocalDateTime.now()), isFileRecoveryEnabled = false)

    val validateRddIgmg = ValidateFileIGMG.calcReportMessages(inputUnzipRddIGMG,udDActivePeriodsMap.toMap)
    Assert.assertFalse(validateRddIgmg.isEmpty())
    ValidateFileIGMG.writeAmmissibilitaReportsCsv(validateRddIgmg, Timestamp.valueOf(LocalDateTime.now()), isFileRecoveryEnabled = false)


    //Check if the reports file exists
    val reportFileName = "ReportAmmissibilitàFileGAS.txt"
    val reportFileNameIgmg = "ReportAmmissibilitàFileIGMG.txt"
    val srcDir = new File(srcPath)
    Assert.assertTrue(srcDir.exists()) //check if the file exist
    Assert.assertTrue(srcDir.isDirectory) //check if the file is a dir
    Assert.assertEquals("xx_yy_01234567890",srcDir.getName) //check if the dir has the proper name

    val reportDirs = srcDir.listFiles().filter(_.isDirectory).flatMap(_.listFiles().filter(_.isDirectory))  //Assuming they can not be empty
    reportDirs.foreach(dir=>{ //check if a report has been created for each input dir
      Assert.assertTrue(dir.isDirectory)
      val reportEsitoFileCSV = new File(dir.getPath+"/"+reportFileName)
      val reportEsitoFileIgmgCSV = new File(dir.getPath+"/"+reportFileNameIgmg)
      Assert.assertTrue(reportEsitoFileCSV.exists()|| reportEsitoFileIgmgCSV.exists() || reportEsitoFileCSV.getParentFile.listFiles().length.equals(0)) //check all the subdirs have the .csv Report or they have no file to analyze
    })

    /*
    *     TESTING CSV REPORTS
    *
    * */
    val reportEsitoFileCsvAll = reportDirs.flatMap(dir=>dir.listFiles().filter(_.getName.equals(reportFileName)))
    reportEsitoFileCsvAll.foreach(file=>{ Assert.assertTrue(file.exists()) }) //check they exist all

    val extensionMask = "/*.txt"
    val reportFileSchema = StructType(Array(
      StructField("CartellaCloud", StringType)
      , StructField("Nomefile", StringType)
      , StructField("Ammissibilità", StringType)
      , StructField("Bloccante", StringType)
      , StructField("Codice_Inammissibilità", StringType)
      , StructField("Descrizione", StringType)
    ))
    //read all the reports into a dataframe
    val reportEsitoFileCsvAllDf_read = Environment.getSpark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("delimiter",Constants.CSV_REPORT_SEPARATOR)
      .schema(reportFileSchema)
      .load(srcPath+subFoldersMask+extensionMask)

    Assert.assertEquals(reportEsitoFileCsvAllDf_read.schema, reportFileSchema) //check dataframe schema matches the specified schema

    val reportEsitoFileCsvAllDf = reportEsitoFileCsvAllDf_read.na.fill("")  /* Fix the problem of blank values
            in csv mapped as null instead of empty strings "" */

    println(reportEsitoFileCsvAllDf.show(10))
    //check no field of any column is null
    reportEsitoFileCsvAllDf.columns.foreach(column=>{
      Assert.assertEquals( 0,
        reportEsitoFileCsvAllDf
          .select(col(column))
          .where(col(column).isNull)
          .count()
      )
    })
    //check all the folders respect unix-like or window-like basic pattern
    Assert.assertEquals( reportEsitoFileCsvAllDf.count(),
      reportEsitoFileCsvAllDf.select(col("CartellaCloud"))
        .filter(col("CartellaCloud") rlike ".*\\\\.*|.*\\/.*")
        .count()
    )

    //Check column has only legal values
    Assert.assertEquals(4,
      reportEsitoFileCsvAllDf.select(col("Codice_Inammissibilità"))
        .where( col("Codice_Inammissibilità").notEqual(Constants.COD904)  && col("Codice_Inammissibilità").notEqual(Constants.COD919)  )
        .count()
    )

    reportEsitoFileCsvAll.foreach(reportFile=>{
      val folderName = reportFile.getParentFile.getAbsolutePath
      Assert.assertTrue( reportEsitoFileCsvAllDf.where(  col("CartellaCloud") === folderName ).count() > 0 )

    })
  }

  def deleteCsvs(rootPath:String):Unit = {
    val srcDir = new File(rootPath)
    val reportDirs = srcDir.listFiles().filter(_.isDirectory).flatMap(_.listFiles().filter(_.isDirectory))  //Assuming they can not be empty
    reportDirs.foreach(dir=>{ //check if a report has been created for each input dir
      Assert.assertTrue(dir.isDirectory)
      val reportEsitoFileCSV = new File(dir.getPath+"/ReportAmmissibilitàFileGAS.csv")
      reportEsitoFileCSV.delete() //check all the subdirs have the .csv Report
      val reportEsitoFileOldCSV = new File(dir.getPath+"/ReportEsitoFILE.csv")
      reportEsitoFileOldCSV.delete() //check all the subdirs have the .csv Report
      val reportEsitoFileIgmgCSV = new File(dir.getPath+"/ReportAmmissibilitàFileIGMG.csv")
      reportEsitoFileIgmgCSV.delete() //check all the subdirs have the .csv Report
      val reportEsitoFileTXT = new File(dir.getPath+"/ReportAmmissibilitàFileGAS.txt")
      reportEsitoFileTXT.delete() //check all the subdirs have the .csv Report
      val reportEsitoFileOldTXT = new File(dir.getPath+"/ReportEsitoFILE.txt")
      reportEsitoFileOldTXT.delete() //check all the subdirs have the .csv Report
      val reportEsitoFileIgmgTXT = new File(dir.getPath+"/ReportAmmissibilitàFileIGMG.txt")
      reportEsitoFileIgmgTXT.delete() //check all the subdirs have the .csv Report
    })
  }

  def convertToGasUnzipMetadata(rddGasXmlMetadata:RDD[GasXmlMetadata]):RDD[GasUnzipMetadata]={
    rddGasXmlMetadata.map(xmlMeta=>
      GasUnzipMetadata(file = xmlMeta.file
        , xmlNode = xmlMeta.xmlNode
        , outputFilePath = "prova.xml"
        , statusCode = null
        , pivaDistributore = xmlMeta.pivaDistributore
        , pivaUtente = xmlMeta.pivaUtente
        , anno = xmlMeta.anno
        , annoRiferimento = xmlMeta.annoRiferimento
        , mese = xmlMeta.mese
        , meseRiferimento = xmlMeta.meseRiferimento
        , giorno = xmlMeta.giorno
        , flusso = xmlMeta.flusso
        , timestamp = xmlMeta.timestamp
        , progressivo = xmlMeta.progressivo
        , tS = xmlMeta.tS ) //NB. xsd validators are instantiated inside unzip.validate()
    )
  }


  def testGetFolderFilesMap(): Unit = {

    val srcPath = new File(ConfigFactory.load.getString("tempRootPath")+"/xx_yy_01234567890").getAbsolutePath
    val subFolder1 = "/2019/1020"
    val subFolder2= "/2020/1020"

    val inputMetaList = List(
      GasUnzipMetadata(
        file = new  File(srcPath+subFolder1+"/01234567890_12345678901_201907_FDD_20190808130645_1_M.xml")
        , pivaDistributore = ""
        , pivaUtente = ""
        , anno = ""
        , annoRiferimento = ""
        , mese = ""
        , meseRiferimento = ""
        , giorno = ""
        , flusso = ""
        , timestamp = ""
        , progressivo = ""
        , tS = ""
      )
      , GasUnzipMetadata(
        file = new  File(srcPath+subFolder2+"/01234567890_12345678901_202006_RGL_20200810154656_1_R.XML")
        , pivaDistributore = ""
        , pivaUtente = ""
        , anno = ""
        , annoRiferimento = ""
        , mese = ""
        , meseRiferimento = ""
        , giorno = ""
        , flusso = ""
        , timestamp = ""
        , progressivo = ""
        , tS = ""
      )
    )

    val inputMetaListZip = List(
      GasUnzipMetadata(
        file = new  File(srcPath+subFolder1+"/01234567890_12345678901_2000000.zip")
        , outputFilePath = srcPath+subFolder1+"/01234567890_12345678901_201907_XXX_20190808130645_1_M.xml"
        , pivaDistributore = ""
        , pivaUtente = ""
        , anno = ""
        , annoRiferimento = ""
        , mese = ""
        , meseRiferimento = ""
        , giorno = ""
        , flusso = ""
        , timestamp = ""
        , progressivo = ""
        , tS = ""
      )
      , GasUnzipMetadata(
        file = new  File(srcPath+subFolder1+"/01234567890_12345678901_201907.zip")
        , outputFilePath = srcPath+subFolder1+"/01234567890_12345678901_201907_XXX_20190808130645_1_M.xml"
        , pivaDistributore = ""
        , pivaUtente = ""
        , anno = ""
        , annoRiferimento = ""
        , mese = ""
        , meseRiferimento = ""
        , giorno = ""
        , flusso = ""
        , timestamp = ""
        , progressivo = ""
        , tS = ""
      )
    )



    val inputUnzipRdd = Environment.getSpark.sparkContext.parallelize(inputMetaList:::inputMetaListZip)

    val pathMap = ValidateFileStandard.getFolderFilesMap(inputUnzipRdd).collectAsMap().toMap
    val testPath1 = new File(srcPath+subFolder1).getAbsolutePath
    val testPath2 = new File(srcPath+subFolder2).getAbsolutePath

    Assert.assertTrue(pathMap.contains(testPath1))
    val fileMap = pathMap.apply(testPath1)
    Assert.assertTrue(fileMap.contains("01234567890_12345678901_201907_FDD_20190808130645_1_M.xml"))

    //different .zip  with same .xml test
    Assert.assertTrue(fileMap.contains("01234567890_12345678901_201907_XXX_20190808130645_1_M.xml"))
    Assert.assertEquals(2, fileMap("01234567890_12345678901_201907_XXX_20190808130645_1_M.xml").size)

    val fileMap2 = pathMap.apply(testPath2)
    Assert.assertTrue(fileMap2.contains("01234567890_12345678901_202006_RGL_20200810154656_1_R.xml"))

  }

}
