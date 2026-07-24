package it.au.misure.ingestionMisureGasUnico.validate

import java.io.File
import java.time.LocalDateTime
import it.au.misure.ingestionMisureGasUnico.model.validate.RuleParameters
import it.au.misure.ingestionMisureGasUnico.model.{ExternalInfo, GasUnzipMetadata}
import it.au.misure.ingestionMisureGasUnico.utility.Constants.{CORRUPTED_XML, CORRUPTED_ZIP, FILE_XML_NOT_PRESENT, GENERIC_ERROR, STD_NAMING_UNMATCH, ZIP_XML_NAMING_UNMATCH, flusso1List, flusso2List}
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import it.au.misure.ingestionMisureGasUnico.utility.{EnvironmentSparkTest, PropertyUtility}

import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory
import junit.framework.TestCase
import org.junit.Assert

import scala.xml.XML

class TestCheckAmmissibilitaFIleRules extends /*TestCase with SparkLocal*/ EnvironmentSparkTest {
  val checkAmm = new CheckAmmissibilitaFileRules
  
  def testRulePIVADistributore(): Unit = {
    val xmlMetaGoodValued = GasUnzipMetadata(
      file=new File("")
      , xmlNode = null
      , pivaDistributore = "05724831002"
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
    )
    val fileGoodValued = <FlussoMisure xmlns:xs="http://www.w3.org/2001/XMLSchema" cod_flusso="FDD">
      <IdentificativiFlusso>
        <piva_utente>12345678901</piva_utente>
        <piva_distr>05724831002</piva_distr>
      </IdentificativiFlusso>
      <DatiPdr></DatiPdr>
    </FlussoMisure>
    val fileBadValued = <FlussoMisure xmlns:xs="http://www.w3.org/2001/XMLSchema" cod_flusso="FDD">
      <IdentificativiFlusso>
        <piva_utente>12345678901</piva_utente>
        <piva_distr>01234567890</piva_distr>
      </IdentificativiFlusso>
      <DatiPdr></DatiPdr>
    </FlussoMisure>

    Assert.assertFalse(checkAmm.rulePIVADistributore.condition(fileGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.rulePIVADistributore.condition(fileBadValued, xmlMetaGoodValued, None))

  }

  def testRulePIVAUDD(): Unit = {
    val xmlMetaGoodValued = GasUnzipMetadata(
      file=new File("XXX_XXXXXXXXXXXX_12345678901/2020/1010/file.xml")
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = "12345678901"
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = ""
      , timestamp= ""
      , progressivo = ""
      , tS = ""
    )

    val xmlMetaBadValued = GasUnzipMetadata(
      file=new File("XXX_XXXXXXXXXXXX_12345678901/2020/1010/file.xml")
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = "01123456789"
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = ""
      , timestamp= ""
      , progressivo = ""
      , tS = ""
    )


    Assert.assertFalse(checkAmm.rulePIVAUDD.condition(null, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.rulePIVAUDD.condition(null, xmlMetaBadValued, None))

  }

  def testRulePIVAUDDPIVAUtente(): Unit = {
    val xmlMetaGoodValued = GasUnzipMetadata(
      file=new File("")
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = "12345678901"
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = ""
      , timestamp= ""
      , progressivo = ""
      , tS = ""
    )
    val fileGoodValued = <FlussoMisure xmlns:xs="http://www.w3.org/2001/XMLSchema" cod_flusso="FDD">
      <IdentificativiFlusso>
        <piva_utente>12345678901</piva_utente>
        <piva_distr>12345678901</piva_distr>
      </IdentificativiFlusso>
      <DatiPdr></DatiPdr>
    </FlussoMisure>
    val fileBadValued = <FlussoMisure xmlns:xs="http://www.w3.org/2001/XMLSchema" cod_flusso="FDD">
      <IdentificativiFlusso>
        <piva_utente>05724831002</piva_utente>
        <piva_distr>05724831002</piva_distr>
      </IdentificativiFlusso>
      <DatiPdr></DatiPdr>
    </FlussoMisure>

    Assert.assertFalse(checkAmm.rulePIVAUDDPIVAUtente.condition(fileGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.rulePIVAUDDPIVAUtente.condition(fileBadValued, xmlMetaGoodValued, None))

  }

  def testRulePIVAUDDRCU(): Unit = {

    val pIvaUdD = "01234567890"
    val uDDActivePeriodsMap = Environment.getSpark.sparkContext.broadcast( Map( pIvaUdD ->
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
   val prms = RuleParameters(parameters = Map("XGiorni"->"10"), isActive = true, bloccante = true)

    Assert.assertFalse(checkAmm.rulePIVAUDDRCU.condition( null, xmlMetaGoodValued,Option(prms) ))
    Assert.assertTrue(checkAmm.rulePIVAUDDRCU.condition( null, xmlMetaBadValued,Option(prms) ))

  }

  def testRuleTimestamp(): Unit = {
    val xmlMetaGoodValued = GasUnzipMetadata(
      file=new File("")
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = ""
      , timestamp= "20201028145000" //yyyyMMddHHmmss
      , progressivo = ""
      , tS = ""
    )
    val xmlMetaBadValued = GasUnzipMetadata(
      file=new File("")
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = ""
      , timestamp= "10202028145000" //MMyyyyddHHmmss
      , progressivo = ""
      , tS = ""
    )

    Assert.assertFalse(checkAmm.ruleTimestamp.condition(null, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleTimestamp.condition(null, xmlMetaBadValued, None))

  }

  def testRuleCodFlusso(): Unit = {
    val xmlMetaGoodValued = GasUnzipMetadata(
      file=new File("")
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = "12345678901"
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "FDD"
      , timestamp= ""
      , progressivo = ""
      , tS = ""
    )
    val fileGoodValued = <FlussoMisure xmlns:xs="http://www.w3.org/2001/XMLSchema" cod_flusso="FDD">
      <IdentificativiFlusso>
        <piva_utente>12345678901</piva_utente>
        <piva_distr>12345678901</piva_distr>
      </IdentificativiFlusso>
      <DatiPdr></DatiPdr>
    </FlussoMisure>
    val fileBadValued = <FlussoMisure xmlns:xs="http://www.w3.org/2001/XMLSchema" cod_flusso="TGL">
      <IdentificativiFlusso>
        <piva_utente>05724831002</piva_utente>
        <piva_distr>05724831002</piva_distr>
      </IdentificativiFlusso>
      <DatiPdr></DatiPdr>
    </FlussoMisure>

    Assert.assertFalse(checkAmm.ruleCodFlusso.condition(fileGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlusso.condition(fileBadValued, xmlMetaGoodValued, None))
  }

  def testRuleTraccaitoStandard(): Unit = {
    val xmlMetaGoodValued = GasUnzipMetadata(
      file=new File("")
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = "12345678901"
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = ""
      , timestamp= ""
      , progressivo = ""
      , tS = "R"
    )

    val xmlMetaBadValued = GasUnzipMetadata(
      file=new File("")
      , xmlNode = null
      , pivaDistributore = ""
      , pivaUtente = "12345678901"
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = "FDD"
      , timestamp= ""
      , progressivo = ""
      , tS = "A"
    )

    Assert.assertFalse(checkAmm.ruleTraccaitoStandard.condition(null, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleTraccaitoStandard.condition(null, xmlMetaBadValued, None))
  }

  def testRuleAlreadyTransmitted(): Unit = {
    //case1: two xml in the same folder but different extension
   /* val srcPath = "/home/development/isilonshare_gas/TMG_01234567890/DISTRIBUTORE/TMG_02747290126_03649070269"
    val subFolder1 = "/2020/1029"
    val fileXml1 = "/01234567890_12345678901_201907_FDD_20190808130645_1_M.xml"
    val fileXml2 = "/01234567890_12345678901_201907_FDD_20190808130645_1_M.XML"

    val inputMetaList = List(
      GasUnzipMetadata(
        file = new  File(srcPath+subFolder1+fileXml1)
        , fileType = "xml"
        , outputFilePath = "TempRootPath/Standard/TMG_02747290126_03649070269"+fileXml1
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
        file = new  File(srcPath+subFolder1+fileXml2)
        , fileType = "xml"
        , outputFilePath = "TempRootPath/Standard/TMG_02747290126_03649070269"+fileXml2
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

    val map = sc.broadcast(ValidateFileStandard.getFolderFilesMap(sc.parallelize(inputMetaList)).collectAsMap().toMap )
    val file1Error = checkAmm.ruleAlreadyTransmitted.condition(null,inputMetaList.head.copy(externalInfo = ExternalInfo(mapFilesName = map)), None)
    val file2Error = checkAmm.ruleAlreadyTransmitted.condition(null,inputMetaList(1).copy(externalInfo = ExternalInfo(mapFilesName = map)), None)

    Assert.assertFalse(file1Error && file2Error ) //only one file pass
    Assert.assertTrue(file1Error || file2Error) //at least one pass

    // case2: two different file zip with the same .xml file
    val fileZip1 = "/01234567890_12345678901_201907_FDD_201908081306450.zip"
    val fileZip2 = "/01234567890_12345678901_201907_FDD2_201908081306450.zip"
    val fileZipXml1 = "/01234567890_12345678901_201907_FDD_20190808130645_1_M.xml"

    val inputZipMetaList = List(
      GasUnzipMetadata(
        file = new  File(srcPath+subFolder1+fileZip1)
        , fileType = "zip"
        , outputFilePath = "TempRootPath/Standard/TMG_02747290126_03649070269"+fileZipXml1
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
        file = new  File(srcPath+subFolder1+fileZip2)
        , fileType = "zip"
        , outputFilePath = "TempRootPath/Standard/TMG_02747290126_03649070269"+fileZipXml1
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

    val zipMap = sc.broadcast(ValidateFileStandard.getFolderFilesMap(sc.parallelize(inputZipMetaList)).collectAsMap().toMap )
    val zipFile1Error = checkAmm.ruleAlreadyTransmitted.condition(null,inputZipMetaList.head.copy(externalInfo = ExternalInfo(mapFilesName = zipMap)), None)
    val zipFile2Error = checkAmm.ruleAlreadyTransmitted.condition(null,inputZipMetaList(1).copy(externalInfo = ExternalInfo(mapFilesName = zipMap)), None)*/
    val xmlMetaTrue = GasUnzipMetadata(
      file = null
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
    val xmlMetaFalse = GasUnzipMetadata(
      file = null
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
    Assert.assertFalse(checkAmm.ruleAlreadyTransmitted.condition(null,xmlMetaFalse,None) )
    Assert.assertTrue(checkAmm.ruleAlreadyTransmitted.condition(null,xmlMetaTrue,None) )

  }

  def testRuleFlusso1ValidateXML(): Unit = {

    val factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
    val flusso1XSDBroad = factory.newSchema(new StreamSource(PropertyUtility.getXsdPeriodicoPath)).newValidator()
    val flusso2XSDBroad = factory.newSchema(new StreamSource(PropertyUtility.getXsdRettificaPath)).newValidator()
    val sampleFile = "src/test/resources/tempRootPath/Standard/TML/2020/10/09/01234567890_12345678901_202010_TML_20201009123456_1_M.xml"
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
      , flusso = "TML"
      , timestamp= ""
      , progressivo = ""
      , tS = ""
      , externalInfo = ExternalInfo(
        flusso1XSD = flusso1XSDBroad
        , flusso2XSD = flusso2XSDBroad
      )

    )
    Assert.assertTrue(checkAmm.isFlusso(xmlMeta.flusso.toLowerCase(),flusso1List))
    Assert.assertFalse(checkAmm.ruleFlusso1ValidateXML.condition(XML.load(sampleFile), xmlMeta, None))
    Assert.assertFalse(checkAmm.ruleFlusso2ValidateXML.condition(XML.load(sampleFile), xmlMeta, None))
  }

  def testRuleFlusso2ValidateXML(): Unit = {

    val factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
    val flusso1XSDBroad = factory.newSchema(new StreamSource(PropertyUtility.getXsdPeriodicoPath)).newValidator()
    val flusso2XSDBroad = factory.newSchema(new StreamSource(PropertyUtility.getXsdRettificaPath)).newValidator()

    val sampleFile = "src/test/resources/tempRootPath/Standard/S40R/2020/06/05/01234567890_12345678901_202005_S40R_20200605092345_1_R.xml"
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
      , flusso = "RML"
      , timestamp= ""
      , progressivo = ""
      , tS = ""
      , externalInfo = ExternalInfo(
        flusso1XSD = flusso1XSDBroad
        , flusso2XSD = flusso2XSDBroad
      )
    )

    Assert.assertTrue(checkAmm.isFlusso(xmlMeta.flusso.toLowerCase(),flusso2List))
    Assert.assertFalse(checkAmm.ruleFlusso1ValidateXML.condition(XML.load(sampleFile), xmlMeta, None))
    Assert.assertFalse(checkAmm.ruleFlusso2ValidateXML.condition(XML.load(sampleFile), xmlMeta, None)) //NOT PASSING SINCE IT MIGHT HAVE ERRORS
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
      , flusso = "RML"
      , timestamp= ""
      , progressivo = ""
      , tS = "R"
      , fileError = STD_NAMING_UNMATCH
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
      , flusso = "RML"
      , timestamp= ""
      , progressivo = "R"
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
      , flusso = "RML"
      , timestamp= ""
      , progressivo = "R"
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
      , flusso = "RML"
      , timestamp= ""
      , progressivo = ""
      , tS = "R"
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
      , flusso = "RML"
      , timestamp= ""
      , progressivo = ""
      , tS = "R"
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
      , flusso = "RML"
      , timestamp= ""
      , progressivo = ""
      , tS = "R"
      , fileError = GENERIC_ERROR
    )
    Assert.assertTrue(checkAmm.ruleGenericError.condition(null, genericErrorFile, None))

  }


}
