package it.au.misure.ingestionMisureGasUnico.utility

import java.io.File

import it.au.misure.ingestionMisureGasUnico.model.GasXmlMetadata
import junit.framework.TestCase
import org.apache.spark.sql.Row
import org.junit.Assert

import scala.xml.XML

class TestParseUtility extends TestCase {
  def testParseXmlMisuraNonGiornaliero(): Unit = {
    val tempRootPath = PropertyUtility.getTmpOutputFolder
    val rootPath = PropertyUtility.getUnzipInputPath
    val inputPath = s"$tempRootPath/Standard/A01/2020/10/17/01234567890_12345678901_202010_A01_20201017123456_1_M.xml"
    val inputMetadata = GasXmlMetadata(
      xmlNode = XML.load(inputPath)
      , file = new File(inputPath)
      , pivaDistributore = "01234567890"
      , pivaUtente = "12345678901"
      , anno = "2020"
      , annoRiferimento = "2020"
      , mese = "10"
      , meseRiferimento = "10"
      , giorno = "17"
      , flusso = "A01"
      , timestamp = "20201017123456"
      , progressivo = "1"
      , tS = "M"
      , ammissibilita = Map("11122233345678" -> "OK")
    )

    val expectedRows = List(Row(
      "A01"
      , "12345678901"
      , "01234567890"
      , "11122233345678"
      , null
      , null
      , null
      , "M"
      , null
      , null
      , "9"
      , "9"
      , "0.9"
      , null
      , null
      , "T"
      , null
      , null
      , null
      , null
      , null
      , null
      , null
      , "G6"
      , "SI"
      , "NO"
      , "S"
      , null
      , null
      , null
      , null
      , "000123456"
      , "000123466"
      , "01/05/2019"
      , null
      , null
      , null
      , null
      , null
      , "OK"
      , s"$rootPath/TMG_01234567890/DISTRIBUTORE/TMG_01234567890_12345678901/2020/1017/01234567890_12345678901_202010_A01_20201017123456_1_M.zip"
      , "2020"
      , "2020"
      , "10"
      , "10"
      , "17"
      , "01234567890_12345678901_202010_A01_20201017123456_1_M.zip"
    ))

    Assert.assertEquals(expectedRows, ParseUtility.parseXmlMisura(inputMetadata))
  }

  def testParseXmlMisuraGiornaliero(): Unit = {
    val tempRootPath = PropertyUtility.getTmpOutputFolder
    val rootPath = PropertyUtility.getUnzipInputPath
    val inputPath = s"$tempRootPath/Standard/TGL/2020/07/11/CCC01234567890_AAA12345678901_202006_TGL_20200701130133_1_M.xml"
    val inputMetadata = GasXmlMetadata(
      xmlNode = XML.load(inputPath)
      , file = new File(inputPath)
      , pivaDistributore = "CCC01234567890"
      , pivaUtente = "AAA12345678901"
      , anno = "2020"
      , annoRiferimento = "2020"
      , mese = "07"
      , meseRiferimento = "06"
      , giorno = "11"
      , flusso = "TGL"
      , timestamp = "20200701130133"
      , progressivo = "1"
      , tS = "M"
      , ammissibilita = Map("11122233345678" -> "OK")
    )

    val expectedFirstRow = Row(
      "TGL"
      , "AAA12345678901"
      , "CCCC01234567890"
      , "11122233345678"
      , "06/2020"
      , null
      , null
      , "G"
      , "qqqqqwwwwwzzzzz11111"
      , null
      , null
      , null
      , "1"
      , null
      , null
      , "P"
      , "P"
      , null
      , null
      , null
      , null
      , null
      , null
      , null
      , null
      , null
      , "E"
      , null
      , null
      , null
      , null
      , "000000033"
      , null
      , null
      , null
      , null
      , null
      , null
      , "01/06/2020"
      , "OK"
      , s"$rootPath/TMG_CCC01234567890/DISTRIBUTORE/TMG_CCC01234567890_AAA12345678901/2020/0711/CCC01234567890_AAA12345678901_202006_TGL_20200701130133_1_M.zip"
      , "2020"
      , "2020"
      , "07"
      , "06"
      , "11"
      , "CCC01234567890_AAA12345678901_202006_TGL_20200701130133_1_M.zip"
    )

    val expectedRows = ParseUtility.parseXmlMisura(inputMetadata)
    Assert.assertEquals((inputMetadata.xmlNode \\ "LettureGiornaliere").size, expectedRows.size)
    Assert.assertEquals(expectedFirstRow, expectedRows.head)
  }

  def testParseXmlRettificaNonGiornaliero(): Unit = {
    val tempRootPath = PropertyUtility.getTmpOutputFolder
    val rootPath = PropertyUtility.getUnzipInputPath
    val inputPath = s"$tempRootPath/Standard/D01R/2020/09/09/01234567890_12345678901_202009_D01R_20200909123456_1_R.xml"
    val inputMetadata = GasXmlMetadata(
      xmlNode = XML.load(inputPath)
      , file = new File(inputPath)
      , pivaDistributore = "01234567890"
      , pivaUtente = "12345678901"
      , anno = "2020"
      , annoRiferimento = "2020"
      , mese = "09"
      , meseRiferimento = "09"
      , giorno = "09"
      , flusso = "D01R"
      , timestamp = "20200909123456"
      , progressivo = "1"
      , tS = "R"
      , ammissibilita = Map("12345677889988" -> "OK")
    )

    val expectedRows = List(Row(
      "D01R"
      , "01234567890"
      , "11122233344"
      , "12345677889988"
      , null
      , null
      , "T"
      , "2"
      , null
      , null
      , null
      , "M"
      , "QQQQQEEEEEZZZZZ99999"
      , "AAAAAXXXXXVVVVV44444"
      , "1"
      , null
      , null
      , null
      , "000000111"
      , "000000123"
      , null
      , null
      , null
      , null
      , null
      , null
      , "OK"
      , s"$rootPath/TMG_01234567890/DISTRIBUTORE/TMG_01234567890_12345678901/2020/0909/01234567890_12345678901_202009_D01R_20200909123456_1_R.zip"
      , "2020"
      , "2020"
      , "09"
      , "09"
      , "09"
      , "01234567890_12345678901_202009_D01R_20200909123456_1_R.zip"
    ))

    Assert.assertEquals(expectedRows, ParseUtility.parseXmlRettifica(inputMetadata))
  }

  def testParseXmlRettificaNonGiornalieroWithCausaOstativa(): Unit = {
    val tempRootPath = PropertyUtility.getTmpOutputFolder
    val rootPath = PropertyUtility.getUnzipInputPath
    val inputPath = s"$tempRootPath/Standard/D01R/2020/09/09/01234567890_12345678901_202009_D01R_20200909123456_2_R.xml"
    val inputMetadata = GasXmlMetadata(
      xmlNode = XML.load(inputPath)
      , file = new File(inputPath)
      , pivaDistributore = "01234567890"
      , pivaUtente = "12345678901"
      , anno = "2020"
      , annoRiferimento = "2020"
      , mese = "09"
      , meseRiferimento = "09"
      , giorno = "09"
      , flusso = "D01R"
      , timestamp = "20200909123456"
      , progressivo = "1"
      , tS = "R"
      , ammissibilita = Map("12345677889988" -> "OK")
    )

    val expectedRows = List(Row(
      "D01R"
      , "01234567890"
      , "11122233344"
      , "12345677889988"
      , null
      , null
      , "T"
      , "2"
      , "SI"
      , null
      , null
      , "M"
      , "QQQQQEEEEEZZZZZ99999"
      , "AAAAAXXXXXVVVVV44444"
      , "1"
      , null
      , null
      , null
      , "000000111"
      , "000000123"
      , null
      , null
      , null
      , null
      , null
      , null
      , "OK"
      , s"$rootPath/TMG_01234567890/DISTRIBUTORE/TMG_01234567890_12345678901/2020/0909/01234567890_12345678901_202009_D01R_20200909123456_1_R.zip"
      , "2020"
      , "2020"
      , "09"
      , "09"
      , "09"
      , "01234567890_12345678901_202009_D01R_20200909123456_1_R.zip"
    ))

    Assert.assertEquals(expectedRows, ParseUtility.parseXmlRettifica(inputMetadata))
  }

  def testParseXmlRettificaGiornaliero(): Unit = {
    val tempRootPath = PropertyUtility.getTmpOutputFolder
    val rootPath = PropertyUtility.getUnzipInputPath
    val inputPath = s"$tempRootPath/Standard/RGL/2020/08/10/01234567890_12345678901_202006_RGL_20200810154656_1_R.xml"
    val inputMetadata = GasXmlMetadata(
      xmlNode = XML.load(inputPath)
      , file = new File(inputPath)
      , pivaDistributore = "01234567890"
      , pivaUtente = "12345678901"
      , anno = "2020"
      , annoRiferimento = "2020"
      , mese = "08"
      , meseRiferimento = "06"
      , giorno = "10"
      , flusso = "RGL"
      , timestamp = "20200810154656"
      , progressivo = "1"
      , tS = "R"
      , ammissibilita = Map("11122233345678" -> "OK")
    )

    val expectedFirstRow = Row(
      "RGL"
      , "12345678901"
      , "01234567890"
      , "11122233345678"
      , "06/2020"
      , null
      , "T"
      , "1"
      , null
      , null
      , null
      , "G"
      , null
      , null
      , "0.9"
      , null
      , null
      , "12/06/2020"
      , "000000123"
      , "000000127"
      , null
      , null
      , null
      , "1123334444qqqqqeeerr"
      , "zzzzz111112222233333"
      , null
      , "OK"
      , s"$rootPath/TMG_01234567890/DISTRIBUTORE/TMG_01234567890_12345678901/2020/0810/01234567890_12345678901_202006_RGL_20200810154656_1_R.zip"
      , "2020"
      , "2020"
      , "08"
      , "06"
      , "10"
      , "01234567890_12345678901_202006_RGL_20200810154656_1_R.zip"
    )

    val expectedRows = ParseUtility.parseXmlRettifica(inputMetadata)
    Assert.assertEquals((inputMetadata.xmlNode \\ "LettureGiornaliereRett").size, expectedRows.size)
    Assert.assertEquals(expectedFirstRow, expectedRows.head)
  }

  def testMotRetLettParse(): Unit = { //defined after bug notified by AU
    val tempRootPath = PropertyUtility.getTmpOutputFolder
    val rootPath = PropertyUtility.getUnzipInputPath
    val inputPath = s"$tempRootPath/Standard/RML/2020/12/23/00489490011_12300020158_201905_RML_20190603101533_30_R.xml"
    val inputMetadata = GasXmlMetadata(
      xmlNode = XML.load(inputPath)
      , file = new File(inputPath)
      , pivaDistributore = "00489490011"
      , pivaUtente = "12300020158"
      , anno = "2020"
      , annoRiferimento = "2019"
      , mese = "12"
      , meseRiferimento = "05"
      , giorno = "23"
      , flusso = "RML"
      , timestamp = "20190603101533"
      , progressivo = "30"
      , tS = "R"
      , ammissibilita = Map("11122233345678" -> "OK")
    )

    val expectedFirstRow = Row(
      "RML"
      , "12300020158"
      , "00489490011"
      , "11122233345678"
      , null
      , "31/05/2019"
      , "P"
      , "4"
      , null
      , null
      , null
      , "M"
      , "1123334444qqqqqeeerr"
      , "zzzzz111112222233333"
      , "0.9"
      , "1"
      , null
      , "21/05/2019"
      , null
      , null
      , null
      , null
      , null
      , null
      , null
      , null
      , "OK"
      , s"$rootPath/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12300020158/2020/1223/00489490011_12300020158_201905_RML_20190603101533_30_R.zip"
      , "2020"
      , "2019"
      , "12"
      , "05"
      , "23"
      , "00489490011_12300020158_201905_RML_20190603101533_30_R.zip"
    )

    val expectedRows = ParseUtility.parseXmlRettifica(inputMetadata)
    Assert.assertEquals(expectedFirstRow, expectedRows.head)
  }
}
