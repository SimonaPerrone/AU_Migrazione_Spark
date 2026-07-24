package it.au.misure.ingestionMisureGasUnico.validate

import it.au.misure.ingestionMisureGasUnico.model.{ExternalInfo, GasUnzipMetadata}
import it.au.misure.ingestionMisureGasUnico.model.validate.ReportEsitoFILEMessage
import it.au.misure.ingestionMisureGasUnico.utility.{Constants, PropertyUtility}
import it.au.misure.ingestionMisureGasUnico.utility.Constants.ERROR_FILE_STRUCTURE
import junit.framework.TestCase
import org.junit.Assert

import java.io.File
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory
import scala.xml.XML

class TestBloccantePriority extends TestCase {
  /**
   * The goal of this test is to verify that the messages with greater priority are the ones with code BLOCCANTE.
   * This means htat if my file fails for two rule one marked as BLOCCANTE one as NON_BLOCCANTE the message logged is
   * the BLOCCANTE's one.
   *
   * In this test case the file triggers rules id  6 NON_BLOCCANTE and id 7 BLOCCANTE, the logged one is the rule with
   * id 7
   * */
  def testBloccantePriority(): Unit = {
    val filePath = "src/test/resources/test/bloccantePriority/00489490011_12300020158_202006_TfL_20200701130133_11_G.xml"
    val checkAmm = new CheckAmmissibilitaFileRules
    val file = new File(filePath)
    val xmlFile = XML.loadFile(file)
    val factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
    val flusso1XSD = factory.newSchema(new StreamSource(PropertyUtility.getXsdPeriodicoPath)).newValidator()
    val flusso2XSD = factory.newSchema(new StreamSource(PropertyUtility.getXsdRettificaPath)).newValidator()

    val fileMetadata = GasUnzipMetadata(
      file = file
      , outputFilePath = ""
      , pivaDistributore = "00489490011"
      , pivaUtente = "12300020158"
      , anno = "2020"
      , annoRiferimento = "2020"
      , mese = "06"
      , meseRiferimento = "06"
      , giorno = "21"
      , flusso = "TfL"
      , timestamp = "20200701130133"
      , progressivo = "11"
      , tS = "G"
      , externalInfo = ExternalInfo(flusso1XSD = flusso1XSD, flusso2XSD = flusso2XSD)
    )
    val message: ReportEsitoFILEMessage = checkAmm.check(xmlFile, fileMetadata)

    Assert.assertEquals(Constants.BLOCCANTE, message.bloccante)
    Assert.assertEquals(ERROR_FILE_STRUCTURE, message.descrizione)

  }

  def testReplaceAll(): Unit = {
    val xmlFileName = "00489490011_12300020158_202006_TfL_20200701130133_11_G.xml.xml"
    val xmlFileName2 = "00489490011_12300020158_202006_TfL_20200701130133_11_G.xml"
    println(xmlFileName.replaceAll("(?i)\\.xml", ""))
    println(xmlFileName2.replaceAll("(?i)\\.xml", ""))
  }

}
