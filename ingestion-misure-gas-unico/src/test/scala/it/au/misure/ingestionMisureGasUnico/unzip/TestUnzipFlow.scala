package it.au.misure.ingestionMisureGasUnico.unzip

import com.typesafe.config.ConfigFactory
import it.au.misure.ingestionMisureGasUnico.args.UnzipArgsConfig
import it.au.misure.ingestionMisureGasUnico.model.GasUnzipMetadata
import it.au.misure.ingestionMisureGasUnico.model.schema.DecompressioneLogSchema._
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.MisuraXMLSchema._
import it.au.misure.ingestionMisureGasUnico.utility.{Constants, EnvironmentSparkTest}
import junit.framework.TestCase
import org.junit.Assert

import java.io.File
import java.time.{LocalDate, LocalDateTime}
import it.au.misure.ingestionMisureGasUnico.utility.Constants._
import it.au.misure.ingestionMisureGasUnico.validate.ValidateFileStandard

import scala.collection.mutable

class TestUnzipFlow extends /*TestCase with SparkLocal*/ EnvironmentSparkTest {
  def testGetFilesWithMetadata(): Unit = {
    val rootPath = ConfigFactory.load.getString("rootPath") + "/input/unzip/getFilesWithMeta/"
    val unzipArgs = UnzipArgsConfig(
      fromDate = Some(LocalDate.parse("2020-10-29")),
      toDate = Some(LocalDate.parse("2021-02-12"))
    )

    val rdd = UnzipFlow.getFilesWithMetadata(unzipArgs, rootPath).cache

    rdd.foreach(println)

    Assert.assertEquals(13, rdd.count())

    Assert.assertEquals("A01", rdd.filter(_.file.getName == "01234567890_12345678901_202010_A01_20201017123456_1_M.zip").first().flusso)
    Assert.assertEquals("2020", rdd.filter(_.file.getName == "01234567890_12345678901_202010_A01_20201017123456_1_M.zip").first().anno)
    Assert.assertEquals("10", rdd.filter(_.file.getName == "01234567890_12345678901_202010_A01_20201017123456_1_M.zip").first().mese)
    Assert.assertEquals("29", rdd.filter(_.file.getName == "01234567890_12345678901_202010_A01_20201017123456_1_M.zip").first().giorno)
    Assert.assertEquals("01234567890", rdd.filter(_.file.getName == "01234567890_12345678901_202010_A01_20201017123456_1_M.zip").first().pivaDistributore)
    Assert.assertEquals("12345678901", rdd.filter(_.file.getName == "01234567890_12345678901_202010_A01_20201017123456_1_M.zip").first().pivaUtente)
    Assert.assertEquals("20201017123456", rdd.filter(_.file.getName == "01234567890_12345678901_202010_A01_20201017123456_1_M.zip").first().timestamp)
    Assert.assertEquals("1", rdd.filter(_.file.getName == "01234567890_12345678901_202010_A01_20201017123456_1_M.zip").first().progressivo)
    Assert.assertEquals("M", rdd.filter(_.file.getName == "01234567890_12345678901_202010_A01_20201017123456_1_M.zip").first().tS)
    Assert.assertEquals("zip", rdd.filter(_.file.getName == "01234567890_12345678901_202010_A01_20201017123456_1_M.zip").first().file.getName.takeRight(3).toLowerCase)

    Assert.assertEquals("A01", rdd.filter(_.file.getName == "01234567890_12345678901_202010_A01_20201017123456_1.zip").first().flusso)
    Assert.assertEquals("2020", rdd.filter(_.file.getName == "01234567890_12345678901_202010_A01_20201017123456_1.zip").first().anno)
    Assert.assertEquals("10", rdd.filter(_.file.getName == "01234567890_12345678901_202010_A01_20201017123456_1.zip").first().mese)
    Assert.assertEquals("29", rdd.filter(_.file.getName == "01234567890_12345678901_202010_A01_20201017123456_1.zip").first().giorno)
    Assert.assertEquals("", rdd.filter(_.file.getName == "FILESBAGLIATO_01234567890_12345678901_202010_CCC_20201017123456_1_M.xml").first().flusso)

    Assert.assertEquals("A01", rdd.filter(_.file.getName == "03752430961_03237330232_A010150_20201016144805.xml").first().flusso)
    Assert.assertEquals("", rdd.filter(_.file.getName == "03752430961_03237330232_A010150_20201016144805.xml").first().pivaDistributore)
    Assert.assertEquals("2020", rdd.filter(_.file.getName == "03752430961_03237330232_A010150_20201016144805.xml").first().anno)
    Assert.assertEquals("10", rdd.filter(_.file.getName == "03752430961_03237330232_A010150_20201016144805.xml").first().mese)
    Assert.assertEquals("29", rdd.filter(_.file.getName == "03752430961_03237330232_A010150_20201016144805.xml").first().giorno)
    Assert.assertEquals("xml", rdd.filter(_.file.getName == "03752430961_03237330232_A010150_20201016144805.xml").first().file.getName.takeRight(3).toLowerCase)


    Assert.assertEquals(STD_NAMING_UNMATCH, rdd.filter(_.file.getName == "13171830787_03679070787_202011_TAL_20201208184801_M_100.zip").first().fileError)
    Assert.assertEquals(STD_NAMING_UNMATCH, rdd.filter(_.file.getName == "13171830787_03679070787_202011_TAL_20201208184801_100_M.xml").first().fileError)
    Assert.assertEquals(STD_NAMING_UNMATCH, rdd.filter(_.file.getName == "01234567890_12345678901_202005__SWG1_20200530101533_1.zip").first().fileError)
    Assert.assertEquals(IGMG_NAMING_UNMATCH, rdd.filter(_.file.getName == "13171830787_03679070787_202011_IGMG101.xml").first().fileError)
    Assert.assertEquals(IGMG_NAMING_UNMATCH, rdd.filter(_.file.getName == "13171830787_03679070787_202011_IGMG101.zip").first().fileError)
    Assert.assertEquals(IGMR_NAMING_UNMATCH, rdd.filter(_.file.getName == "01234567890_12345678901_202012_IGMR_20201212091234_2.xml").first().fileError)
    Assert.assertEquals(IGMR_NAMING_UNMATCH_BUT_STD_MATCH, rdd.filter(_.file.getName == "01234567890_12345678901_202012_IGMR_20201212091234_1_M.zip").first().fileError)
  }

  def testUnzipXml(): Unit = {
    val rootPath = ConfigFactory.load.getString("rootPath") + "/input/unzip/unzipXml/"

    val xmlFile = GasUnzipMetadata(
      file = new File(rootPath + "01234567890_12345678901_202010_A01_20201017123456_1_M.xml")
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
      , tS = "M"
    )

    val xmlFileResult = UnzipFlow.unzipXml(xmlFile, "dest", "destOld")
    Assert.assertTrue(xmlFileResult.head.outputFilePath.contains("01234567890_12345678901_202010_A01_20201017123456_1_M.xml"))
    Assert.assertEquals(1, xmlFileResult.length)
    Assert.assertEquals(STATUS_0.toString, xmlFileResult.head.statusCode)
    Assert.assertEquals(Constants.OK, xmlFileResult.head.statusMessage)
    Assert.assertEquals(PHASE_C.toString, xmlFileResult.head.statusType)
    Assert.assertTrue((xmlFileResult.head.xmlNode \ IdentificativiFlusso).nonEmpty)


    val xmlZip = GasUnzipMetadata(
      file = new File(rootPath + "03752430961_03237330232_A010150_20201016144805.xml.zip")
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "A01"
      , timestamp = ""
      , progressivo = ""
      , tS = ""
      , trackType = OLD_PRESTAZIONALE
    )

    val xmlZipResult = UnzipFlow.unzipXml(xmlZip, "dest", "destOld")
    Assert.assertEquals(3, xmlZipResult.length)

    Assert.assertTrue(xmlZipResult.head.outputFilePath.contains("01234567890_12345678901_202010_A01_20201017123456_1_M.xml"))
    Assert.assertEquals(STATUS_4.toString, xmlZipResult.head.statusCode)
    Assert.assertEquals(PHASE_C.toString, xmlZipResult.head.statusType)

    Assert.assertTrue(xmlZipResult(1).outputFilePath.contains("03752430961_03237330232_A010150_20201016144805.xml"))
    Assert.assertEquals(STATUS_0.toString, xmlZipResult(1).statusCode)
    Assert.assertEquals(Constants.OK, xmlZipResult(1).statusMessage)
    Assert.assertEquals(PHASE_U.toString, xmlZipResult(1).statusType)
    Assert.assertTrue((xmlZipResult(1).xmlNode \ IdentificativiFlusso).nonEmpty)

    Assert.assertTrue(xmlZipResult(2).outputFilePath.contains("FILESBAGLIATO_01234567890_12345678901_202010_A01_20201017123456_1_M.csv"))
    Assert.assertEquals(STATUS_3.toString, xmlZipResult(2).statusCode)
    Assert.assertNotEquals(Constants.OK, xmlZipResult(2).statusMessage)
    Assert.assertEquals(PHASE_C.toString, xmlZipResult(2).statusType)
    Assert.assertNull(xmlZipResult(2).xmlNode)


    val zipCorruptedFile = GasUnzipMetadata(
      file = new File(rootPath + "FILESBAGLIATO.zip")
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

    val zipCorruptedFileResult = UnzipFlow.unzipXml(zipCorruptedFile, "dest", "destOld")
    Assert.assertEquals("", zipCorruptedFileResult.head.outputFilePath)
    Assert.assertEquals(1, zipCorruptedFileResult.length)
    Assert.assertEquals(STATUS_5.toString, zipCorruptedFileResult.head.statusCode)
    Assert.assertNotEquals(Constants.OK, zipCorruptedFileResult.head.statusMessage)
    Assert.assertEquals(PHASE_E.toString, zipCorruptedFileResult.head.statusType)
    Assert.assertNull(zipCorruptedFileResult.head.xmlNode)


    val xmlFile2 = GasUnzipMetadata(
      file = new File(rootPath + "00930530324_06655971007_202012_TAL_20210104191301_1_M.ZIP")
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
      , tS = "M"
      , trackType = STD
    )

    val xmlFileResult2 = UnzipFlow.unzipXml(xmlFile2, "dest", "destOld")
    println(xmlFileResult2.head.fileError)
    Assert.assertEquals(Constants.OK, xmlFileResult2.head.statusMessage)

    val xmlFile3 = GasUnzipMetadata(
      file = new File(rootPath + "00930530324_06655971007_202012_TAL_20210104191301_1_M.XmL.zIP")
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
      , tS = "M"
      , trackType = STD
    )

    val xmlFileResult3 = UnzipFlow.unzipXml(xmlFile3, "dest", "destOld")
    Assert.assertEquals(Constants.OK, xmlFileResult3.head.statusMessage)

    ////////////////////////////////////////////////////////////////////////

    val stdWithNoXml = GasUnzipMetadata(
      file = new File(rootPath + "13171830787_03679070787_202011_TAL_20201208184801_102_M.zip")
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "TAL"
      , timestamp = ""
      , progressivo = ""
      , tS = ""
      , trackType = STD
    )
    val stdWithNoXmlResult = UnzipFlow.unzipXml(stdWithNoXml, "dest", "destOld")
    Assert.assertEquals(FILE_XML_NOT_PRESENT, stdWithNoXmlResult.head.fileError)

    val emptyZip0 = GasUnzipMetadata(
      file = new File(rootPath + "13171830787_03679070787_202011_TAL_20201208184801_104_M.zip")
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "TAL"
      , timestamp = ""
      , progressivo = ""
      , tS = ""
      , trackType = STD
    )
    val emptyZipResult0 = UnzipFlow.unzipXml(emptyZip0, "dest", "destOld")
    Assert.assertEquals(FILE_XML_NOT_PRESENT, emptyZipResult0.head.fileError)

    val emptyZip1 = GasUnzipMetadata(
      file = new File(rootPath + "13171830787_03679070787_202011_TAL_20201208184801_103_M.zip")
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "TAL"
      , timestamp = ""
      , progressivo = ""
      , tS = ""
      , trackType = STD
    )
    val emptyZipResult1 = UnzipFlow.unzipXml(emptyZip1, "dest", "destOld")
    Assert.assertEquals(FILE_XML_NOT_PRESENT, emptyZipResult1.head.fileError)

    val emptyZip2 = GasUnzipMetadata(
      file = new File(rootPath + "00517310421_02106730415_202103_A01_20210303111334_1_M.zip")
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "TAL"
      , timestamp = ""
      , progressivo = ""
      , tS = ""
      , trackType = STD
    )
    val emptyZipResult2 = UnzipFlow.unzipXml(emptyZip2, "dest", "destOld")
    Assert.assertEquals(CORRUPTED_ZIP, emptyZipResult2.head.fileError)

    val stdZipXmlNamingUnmatch = GasUnzipMetadata(
      file = new File(rootPath + "00489490011_01531350229_202010_TML_20201009123456_4_M.zip")
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "TAL"
      , timestamp = ""
      , progressivo = ""
      , tS = ""
      , trackType = STD
    )
    val stdZipXmlNamingUnmatchResult = UnzipFlow.unzipXml(stdZipXmlNamingUnmatch, "dest", "destOld")
    Assert.assertEquals(ZIP_XML_NAMING_UNMATCH, stdZipXmlNamingUnmatchResult.head.fileError)

    // single .xml.xml file inside zip, same name as zip file
    val singleXmlXmlFile = GasUnzipMetadata(
      file = new File(rootPath + "08549940016_12883420155_202008_RGL_20211104114217_1_R.xml.zip")
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "A01"
      , timestamp = ""
      , progressivo = ""
      , tS = ""
      , trackType = STD
    )
    val singleXmlXmlFileResult = UnzipFlow.unzipXml(singleXmlXmlFile, "dest", "destOld")
    Assert.assertEquals(ZIP_XML_NAMING_UNMATCH, singleXmlXmlFileResult.head.fileError)

    // single .xml file inside zip, same name as zip file
    val sinlgeXmlFile = GasUnzipMetadata(
      file = new File(rootPath + "08549940016_12883420155_202008_RGL_20211104114217_2_R.zip")
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "A01"
      , timestamp = ""
      , progressivo = ""
      , tS = ""
      , trackType = STD
    )
    val sinlgeXmlFileResult = UnzipFlow.unzipXml(sinlgeXmlFile, "dest", "destOld")
    Assert.assertEquals(Constants.OK, sinlgeXmlFileResult.head.statusMessage)

    // two .xml.xml files inside zip, the first one has the same name as the zip file
    val multipleXmlXmlFiles = GasUnzipMetadata(
      file = new File(rootPath + "08549940016_12883420155_202008_RGL_20211104114217_3_R.xml.zip")
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "A01"
      , timestamp = ""
      , progressivo = ""
      , tS = ""
      , trackType = STD
    )
    val multipleXmlXmlFilesResult = UnzipFlow.unzipXml(multipleXmlXmlFiles, "dest", "destOld")
    Assert.assertEquals(FILE_XML_NOT_PRESENT, multipleXmlXmlFilesResult.head.fileError)

    // two .xml files inside zip, the first one has the same name as the zip file
    val multipleXmlFiles = GasUnzipMetadata(
      file = new File(rootPath + "08549940016_12883420155_202008_RGL_20211104114217_4_R.zip")
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "A01"
      , timestamp = ""
      , progressivo = ""
      , tS = ""
      , trackType = STD
    )

    val multipleXmlFilesResult = UnzipFlow.unzipXml(multipleXmlFiles, "dest", "destOld")
    Assert.assertEquals(FILE_XML_NOT_PRESENT, multipleXmlFilesResult.head.fileError)

    // file with inside a folder containing a valid XML and an invalid XML (corrupted)
    val oldZipWithFolder = GasUnzipMetadata(
      file = new File(rootPath + "03479071000_07670380000_V01_0150.zip")
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "V01"
      , timestamp = ""
      , progressivo = ""
      , tS = ""
      , trackType = OLD_PRESTAZIONALE
    )

    val oldZipWithFolderResult = UnzipFlow.unzipXml(oldZipWithFolder, "dest", "destOld")
    Assert.assertEquals(3, oldZipWithFolderResult.length)
    Assert.assertEquals(1, oldZipWithFolderResult.count(file => file.statusCode.equals(STATUS_0.toString)))
    Assert.assertEquals(1, oldZipWithFolderResult.count(file => file.statusCode.equals(STATUS_3.toString)))
    Assert.assertEquals(1, oldZipWithFolderResult.count(file => file.statusCode.equals(STATUS_6.toString)))


    val zipStdCorrotto = GasUnzipMetadata(
      file = new File(rootPath + "13171830787_03679070787_202011_TAL_20201208184801_100_M.zip")
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
      , tS = "M"
      , trackType = STD
    )

    val zipStdCorrottoResult = UnzipFlow.unzipXml(zipStdCorrotto, "dest", "destOld")
    Assert.assertTrue(zipStdCorrottoResult.head.statusCode.equals(STATUS_5.toString) && zipStdCorrottoResult.head.fileError.equals(CORRUPTED_ZIP))


    val xmlStdCorrotto = GasUnzipMetadata(
      file = new File(rootPath + "13171830787_03679070787_202011_TAL_20201208184801_101_M.zip")
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
      , tS = "M"
      , trackType = STD
    )

    val xmlStdCorrottoResult = UnzipFlow.unzipXml(xmlStdCorrotto, "dest", "destOld")
    Assert.assertTrue(xmlStdCorrottoResult.head.statusCode.equals(STATUS_6.toString) && xmlStdCorrottoResult.head.fileError.equals(CORRUPTED_XML))

    val zipStdNamingUnmatch = GasUnzipMetadata(
      file = new File(rootPath + "13171830787_03679070787_202011_TAL_20201208184801_101_M.zip")
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
      , tS = "M"
      , statusCode = STATUS_3
      , statusMessage = "Nomenclatura tracciato non rispettata."
      , statusType = PHASE_U
      , trackType = STD
      , fileError = STD_NAMING_UNMATCH
    )

    val zipStdNamingUnmatchResult = UnzipFlow.unzipXml(zipStdNamingUnmatch, "dest", "destOld")
    Assert.assertTrue(zipStdNamingUnmatchResult.head.statusCode.equals(STATUS_3.toString) && zipStdNamingUnmatchResult.head.fileError.equals(STD_NAMING_UNMATCH))


    val zipIGMGCorrotto = GasUnzipMetadata(
      file = new File(rootPath + "01217720779_06655971777_202101_IGMG_20210107125344_1.zip")
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
      , trackType = IGMG
    )

    val zipIGMGCorrottoResult = UnzipFlow.unzipXml(zipIGMGCorrotto, "dest", "destOld")
    Assert.assertTrue(zipIGMGCorrottoResult.head.statusCode.equals(STATUS_5.toString) && zipIGMGCorrottoResult.head.fileError.equals(CORRUPTED_ZIP))


    val xmlIGMGCorrotto = GasUnzipMetadata(
      file = new File(rootPath + "01217720779_06655971777_202101_IGMG_20210107125344_2.zip")
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
      , trackType = IGMG
    )

    val xmlIGMGCorrottoResult = UnzipFlow.unzipXml(xmlIGMGCorrotto, "dest", "destOld")
    Assert.assertTrue(xmlIGMGCorrottoResult.head.statusCode.equals(STATUS_6.toString) && xmlIGMGCorrottoResult.head.fileError.equals(CORRUPTED_XML))

    val xmlIGMRCorrotto = GasUnzipMetadata(
      file = new File(rootPath + "01217720779_06655971777_202101_IGMR_20210107125344_2.zip")
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
      , trackType = IGMR
    )

    val xmlIGMRCorrottoResult = UnzipFlow.unzipXml(xmlIGMRCorrotto, "dest", "destOld")
    Assert.assertTrue(xmlIGMRCorrottoResult.head.statusCode.equals(STATUS_6.toString) && xmlIGMRCorrottoResult.head.fileError.equals(CORRUPTED_XML))

    val zipIGMRCorrotto = GasUnzipMetadata(
      file = new File(rootPath + "01217720779_06655971777_202101_IGMR_20210107125344_1.zip")
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
      , trackType = IGMR
    )

    val zipIGMRCorrottoResult = UnzipFlow.unzipXml(zipIGMRCorrotto, "dest", "destOld")
    Assert.assertTrue(zipIGMRCorrottoResult.head.statusCode.equals(STATUS_5.toString) && zipIGMRCorrottoResult.head.fileError.equals(CORRUPTED_ZIP))

    val IgmrZipXmlNamingUnmatch = GasUnzipMetadata(
      file = new File(rootPath + "01234567890_12345678901_202012_IGMR_20201212091234_3.zip")
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
      , trackType = IGMR
    )

    val IgmrZipXmlNamingUnmatchResult = UnzipFlow.unzipXml(IgmrZipXmlNamingUnmatch, "dest", "destOld")
    Assert.assertEquals(ZIP_XML_NAMING_UNMATCH, IgmrZipXmlNamingUnmatchResult.head.fileError)

    val xmlIGMRNotPresent = GasUnzipMetadata(
      file = new File(rootPath + "01234567890_12345678901_202012_IGMR_20201212091234_1.zip")
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
      , trackType = IGMR
    )

    val xmlIGMRNotPresentResult = UnzipFlow.unzipXml(xmlIGMRNotPresent, "dest", "destOld")
    Assert.assertTrue(xmlIGMRNotPresentResult.head.statusCode.equals(STATUS_4.toString) && xmlIGMRNotPresentResult.head.fileError.equals(FILE_XML_NOT_PRESENT))

    val fileNotXml = GasUnzipMetadata(
      file = new File(rootPath + "01234567890_12345678901_202012_IGMR_20201212091234_2.zip")
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
      , trackType = IGMR
    )

    val fileNotXmlResult = UnzipFlow.unzipXml(fileNotXml, "dest", "destOld")
    Assert.assertTrue(fileNotXmlResult.head.statusCode.equals(STATUS_4.toString) && fileNotXmlResult.head.fileError.equals(FILE_NOT_XML))
  }

}
