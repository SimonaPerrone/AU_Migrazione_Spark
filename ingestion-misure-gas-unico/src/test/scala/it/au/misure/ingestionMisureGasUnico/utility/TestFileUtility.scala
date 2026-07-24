package it.au.misure.ingestionMisureGasUnico.utility

import java.io._

import it.au.misure.ingestionMisureGasUnico.model.GasXmlMetadata
import junit.framework.TestCase
import org.junit.Assert

import scala.xml._

class TestFileUtility extends TestCase {
  def testExtractNodeOrNull(): Unit = {
    val testXml =
      <rootNode>
        <existingNode>1</existingNode>
      </rootNode>

    Assert.assertEquals("1", FileUtility.extractNodeOrNull(testXml \ "existingNode"))
    Assert.assertNull(FileUtility.extractNodeOrNull(testXml \ "nonExistentNode"))
  }

  def testXmlToMetadataStandard(): Unit = {
    val inputPath = s"${PropertyUtility.getTmpOutputFolder}/Standard/A01/2020/10/17/01234567890_12345678901_202010_A01_20201017123456_1_M.xml"
    val inputFile = new File(inputPath)
    val expectedMetadata = GasXmlMetadata(
      xmlNode = XML.load(inputPath)
      , file = inputFile
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
    )

    Assert.assertEquals(expectedMetadata, FileUtility.xmlToMetadata(inputFile))
  }

  def testXmlToMetadataIGMG(): Unit = {
    val inputPath = s"${PropertyUtility.getTmpOutputFolder}/IGMG/IGMG/2020/12/12/01234567890_12345678901_202012_IGMG_20201212091234_1.xml"
    val inputFile = new File(inputPath)

    val expectedMetadata = GasXmlMetadata(
      xmlNode = XML.load(inputPath)
      , file = inputFile
      , pivaDistributore = "01234567890"
      , pivaUtente = "12345678901"
      , anno = "2020"
      , annoRiferimento = "2020"
      , mese = "12"
      , meseRiferimento = "12"
      , giorno = "12"
      , flusso = "IGMG"
      , timestamp = "20201212091234"
      , progressivo = "1"
      , tS = ""
    )

    Assert.assertEquals(expectedMetadata, FileUtility.xmlToMetadata(inputFile))
  }
}
