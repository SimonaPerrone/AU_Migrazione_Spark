package it.au.misure.ingestionMisureGasUnico.validate

import java.io.File
import java.time.LocalDateTime
import it.au.misure.ingestionMisureGasUnico.model.validate.RuleParameters
import it.au.misure.ingestionMisureGasUnico.model.{ExternalInfo, GasUnzipMetadata, GasXmlMetadata}
import it.au.misure.ingestionMisureGasUnico.utility.Constants.{CORRUPTED_XML, CORRUPTED_ZIP, FILE_XML_NOT_PRESENT, GENERIC_ERROR, IGMG_NAMING_UNMATCH, ZIP_XML_NAMING_UNMATCH}
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import it.au.misure.ingestionMisureGasUnico.utility.{EnvironmentSparkTest, PropertyUtility}

import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory
import junit.framework.TestCase
import org.junit.Assert

import scala.xml.XML

class TestCheckAmmissibilitaFileRulesIGMG extends /*TestCase with SparkLocal*/ EnvironmentSparkTest {

  val  checkAmm = new CheckAmmissibilitaFileRulesIGMG

  def testRulePivaDistributore():Unit = { //ID 1
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "01234567890",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val xmlMetaBadValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "12345678901",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val xmlFile = <FlussoIGMG xmlns:xs="http://www.w3.org/2001/XMLSchema" CodFlusso="IGMG">
      <IdentificativiFlusso>
        <piva_utente>12345678901</piva_utente>
        <piva_distr>01234567890</piva_distr>
      </IdentificativiFlusso>
      <DatiPdR></DatiPdR>
    </FlussoIGMG>


    Assert.assertTrue(checkAmm.rulePivaDistributore.condition(xmlFile, xmlMetaBadValued, None))
    Assert.assertFalse(checkAmm.rulePivaDistributore.condition(xmlFile, xmlMetaGoodValued, None))
  }

  def testRulePivaUDDCloud():Unit = { // id 2
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = null,
      file = new File("XXX_XXXXXXXXXXXX_12345678901/2020/1010/file.xml"),
      pivaDistributore = "",
      pivaUtente = "12345678901",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val xmlMetaBadValued = GasXmlMetadata(
      xmlNode = null,
      file = new File("XXX_XXXXXXXXXXXX_12345678901/2020/1010/file.xml"),
      pivaDistributore = "",
      pivaUtente = "01234567890",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val xmlFile = null

    Assert.assertTrue(checkAmm.rulePivaUDDCloud.condition(xmlFile, xmlMetaBadValued, None))
    Assert.assertFalse(checkAmm.rulePivaUDDCloud.condition(xmlFile, xmlMetaGoodValued, None))
  }

  def testRulePivaUdDFile():Unit = { // ID 3
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "12345678901",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val xmlMetaBadValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "01234567890",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val xmlFile = <FlussoIGMG xmlns:xs="http://www.w3.org/2001/XMLSchema" CodFlusso="IGMG">
      <IdentificativiFlusso>
        <piva_utente>12345678901</piva_utente>
      </IdentificativiFlusso>
      <DatiPdR></DatiPdR>
    </FlussoIGMG>

    Assert.assertTrue(checkAmm.rulePivaUdDFile.condition(xmlFile, xmlMetaBadValued, None))
    Assert.assertFalse(checkAmm.rulePivaUdDFile.condition(xmlFile, xmlMetaGoodValued, None))
  }

  def testRulePIVAUDDRCU():Unit = { // id 4
    val pIvaUdD = "01234567890"
    val uDDActivePeriodsMap = Environment.getSpark.sparkContext.broadcast(
      Map( pIvaUdD ->
        ( LocalDateTime.of(2019,1,1,0,0),LocalDateTime.of(2020,12,30,23,59))
      )
    )

    val xmlMetaGoodValued = GasUnzipMetadata(
      file=new File("")
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = pIvaUdD
      , anno = ""
      , annoRiferimento = "2020"
      , mese = ""
      , meseRiferimento = "10"
      , giorno = ""
      , flusso = ""
      , timestamp= ""
      , progressivo = ""
      , tS = ""
      , externalInfo = ExternalInfo(uDDActivePeriodsMap = uDDActivePeriodsMap)
    )

    val xmlMetaBadValued = GasUnzipMetadata(
      file=new File("")
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = pIvaUdD
      , anno = ""
      , annoRiferimento = "2018"
      , mese = ""
      , meseRiferimento = "10"
      , giorno = ""
      , flusso = ""
      , timestamp= ""
      , progressivo = ""
      , tS = ""
      , externalInfo = ExternalInfo(uDDActivePeriodsMap = uDDActivePeriodsMap)
    )

    val xmlFile = null
    val prms = Option( RuleParameters(parameters = Map("XGiorni"->"10"), isActive = true, bloccante = true) )


    Assert.assertTrue(checkAmm.rulePIVAUDDRCU.condition(xmlFile, xmlMetaBadValued, prms))
    Assert.assertFalse(checkAmm.rulePIVAUDDRCU.condition(xmlFile, xmlMetaGoodValued, prms))
  }

  def testRuleTimestamp():Unit = { //id 5
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = "",
      timestamp = "20201028145000",
      progressivo = "",
      tS = ""
    )

    val xmlMetaBadValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = "",
      timestamp = "10202028145000", //MMyyyyddHHmmss
      progressivo = "",
      tS = ""
    )

    val xmlFile = null

    Assert.assertTrue(checkAmm.ruleTimestamp.condition(xmlFile, xmlMetaBadValued, None))
    Assert.assertFalse(checkAmm.ruleTimestamp.condition(xmlFile, xmlMetaGoodValued, None))
  }

  def testRuleFlussoCodFlusso():Unit = { // id 6
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = "IGMG",
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val xmlMetaBadValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = "TGL",
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val xmlFile = <FlussoIGMG xmlns:xs="http://www.w3.org/2001/XMLSchema" CodFlusso="IGMG"></FlussoIGMG>

    Assert.assertTrue(checkAmm.ruleFlussoCodFlusso.condition(xmlFile, xmlMetaBadValued, None))
    Assert.assertFalse(checkAmm.ruleFlussoCodFlusso.condition(xmlFile, xmlMetaGoodValued, None))
  }

  def testRuleFileAlreadyTransmitted():Unit = { //id 7
    //case1: two xml in the same folder but different extension

    val xmlMetaTrue = GasUnzipMetadata(
      file =null
      , outputFilePath = ""
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
      , alreadyTransmitted = true
    )

    val xmlMetaFalse =GasUnzipMetadata(
      file =null
      , outputFilePath = ""
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
      , alreadyTransmitted = false
    )


    Assert.assertFalse(checkAmm.ruleFileAlreadyTransmitted.condition(null,xmlMetaFalse,None ) )
    Assert.assertTrue(checkAmm.ruleFileAlreadyTransmitted.condition(null,xmlMetaTrue,None ) )//at least one pass

  }

  def testRuleXMLValidate():Unit = { //id 8
    val factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
    val flussoIGMGXSDBroad = factory.newSchema(new StreamSource(PropertyUtility.getXsdIgmgPath)).newValidator()

    val sampleFile = "src/test/resources/tempRootPath/IGMG/IGMG/2020/12/12/01234567890_12345678901_202012_IGMG_20201212091234_2.xml"

    val xmlMeta = GasUnzipMetadata(
      file=new File(sampleFile)
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = ""
      , timestamp= ""
      , progressivo = ""
      , tS = ""
      , externalInfo = ExternalInfo(flussoIGMGXSD = flussoIGMGXSDBroad )

    )
    Assert.assertFalse(checkAmm.ruleXMLValidate.condition(XML.load(sampleFile), xmlMeta, None))
  }

  def testRuleAnnoMeseRiferimentoNomeFile(): Unit = {
    val xmlMetaGoodValued = GasUnzipMetadata(
      file=new File("")
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = "2020"
      , mese = ""
      , meseRiferimento = "10"
      , giorno = ""
      , flusso = ""
      , timestamp= ""
      , progressivo = ""
      , tS = ""
    )

    val xmlMetaBadValued1 = GasUnzipMetadata(
      file=new File("")
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = "2020"
      , mese = ""
      , meseRiferimento = "20"
      , giorno = ""
      , flusso = ""
      , timestamp= ""
      , progressivo = ""
      , tS = ""
    )

    val xmlMetaBadValued2 = GasUnzipMetadata(
      file=new File("")
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = "1800"
      , mese = ""
      , meseRiferimento = "10"
      , giorno = ""
      , flusso = ""
      , timestamp= ""
      , progressivo = ""
      , tS = ""
    )

    val xmlMetaBadValued3 = GasUnzipMetadata(
      file=new File("")
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = "0100"
      , mese = ""
      , meseRiferimento = "10"
      , giorno = ""
      , flusso = ""
      , timestamp= ""
      , progressivo = ""
      , tS = ""
    )

    Assert.assertFalse(checkAmm.ruleAnnoMeseRiferimentoNomeFile.condition(null, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleAnnoMeseRiferimentoNomeFile.condition(null, xmlMetaBadValued1, None))
    Assert.assertTrue(checkAmm.ruleAnnoMeseRiferimentoNomeFile.condition(null, xmlMetaBadValued2, None))
    Assert.assertTrue(checkAmm.ruleAnnoMeseRiferimentoNomeFile.condition(null, xmlMetaBadValued3, None))
  }

  def testRuleNamingUnmatch(): Unit = {

    val namingUnmatchFile = GasUnzipMetadata(
      file = null
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "IGMG"
      , timestamp= ""
      , progressivo = ""
      , tS = ""
      , fileError = IGMG_NAMING_UNMATCH
    )
    Assert.assertTrue(checkAmm.ruleNamingUnmatch.condition(null, namingUnmatchFile, None))

  }

  def testRuleXmlNotPresentFile(): Unit = {

    val XmlNotPresentFile = GasUnzipMetadata(
      file = null
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "IGMG"
      , timestamp= ""
      , progressivo = ""
      , tS = ""
      , fileError = FILE_XML_NOT_PRESENT
    )
    Assert.assertTrue(checkAmm.ruleXmlNotPresent.condition(null, XmlNotPresentFile, None))

  }

  def testRuleZipXmlNamingUnmatch(): Unit = {

    val zipXmlNamingUnmatchFile = GasUnzipMetadata(
      file = null
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "IGMG"
      , timestamp= ""
      , progressivo = ""
      , tS = ""
      , fileError = ZIP_XML_NAMING_UNMATCH
    )
    Assert.assertTrue(checkAmm.ruleZipXmlNamingUnmatch.condition(null, zipXmlNamingUnmatchFile, None))

  }

  def testRuleCorruptedZip(): Unit = {

    val CorruptedZipFile = GasUnzipMetadata(
      file = null
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "IGMG"
      , timestamp= ""
      , progressivo = ""
      , tS = ""
      , fileError = CORRUPTED_ZIP
    )
    Assert.assertTrue(checkAmm.ruleZipError.condition(null, CorruptedZipFile, None))

  }

  def testRuleCorruptedXml(): Unit = {

    val CorruptedXmlFile = GasUnzipMetadata(
      file = null
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "IGMG"
      , timestamp= ""
      , progressivo = ""
      , tS = ""
      , fileError = CORRUPTED_XML
    )
    Assert.assertTrue(checkAmm.ruleXmlError.condition(null, CorruptedXmlFile, None))

  }

  def testRuleGenericError(): Unit = {

    val genericErrorFile = GasUnzipMetadata(
      file = null
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "IGMG"
      , timestamp= ""
      , progressivo = ""
      , tS = ""
      , fileError = GENERIC_ERROR
    )
    Assert.assertTrue(checkAmm.ruleGenericError.condition(null, genericErrorFile, None))

  }

}
