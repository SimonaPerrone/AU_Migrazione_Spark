package it.eng.au.eng.ammissibilita.file

import it.eng.au.ammissibilita.file.CheckAmmissibilitaFileRules
import it.eng.au.args.AmmissibilitaParameters
import it.eng.au.eng.utility.{EnvironmentSparkTest, SparkLocal}
import it.eng.au.model.XMLMetadata
import it.eng.au.utility.environment.Environment
import it.eng.au.utility.{PropertyUtility, SystemUtility}
import junit.framework.TestCase
import org.junit.Assert

import java.io.File
import java.time.LocalDateTime
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory
import scala.xml.XML

class TestCheckAmmissibilitaFileRules extends EnvironmentSparkTest {
  SystemUtility.setLocalLaunch()

  def testRuleDatiFile(): Unit = {

    val datiPod2018OK = <FlussoMisure>
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <MeseAnno>10/2018</MeseAnno>
      </DatiPod>
    </FlussoMisure>

    //if flusso is SNM or SNM2G must not trigger an error otherwise it should
    val datiPod2018minus1Month = <FlussoMisure>
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <MeseAnno>09/2018</MeseAnno>
      </DatiPod>
    </FlussoMisure>

    //if flusso is SNM or SNM2G must not trigger an error otherwise it should
    val datiPod2018DataMisura = <FlussoMisure>
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <MeseAnno>10/2018</MeseAnno>
        <DataMisura>01/09/2018</DataMisura>
      </DatiPod>
    </FlussoMisure>

    val datiPod2018MultipleNodesOK = <FlussoMisure>
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataMisura>01/10/2018</DataMisura>
      </DatiPod>
      <DatiPod>
        <Pod>IT001E49161678</Pod>
        <MeseAnno>10/2018</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>IT001E49161677</Pod>
        <MeseAnno>10/2018</MeseAnno>
        <DataMisura>01/10/2018</DataMisura>
      </DatiPod>
    </FlussoMisure>

    val datiPod2018MultipleNodesError = <FlussoMisure>
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <MeseAnno>10/2018</MeseAnno>
        <DataMisura>01/10/2018</DataMisura>
      </DatiPod>
      <DatiPod>
        <Pod>IT001E49161678</Pod>
        <MeseAnno>10/2018</MeseAnno>
        <DataMisura>01/10/2018</DataMisura>
      </DatiPod>
      <DatiPod>
        <Pod>IT001E49161677</Pod>
        <MeseAnno>10/2018</MeseAnno>
        <DataMisura>01/06/2018</DataMisura>
      </DatiPod>
    </FlussoMisure>

    val datiPod2018MultipleNodesBadFormat = <FlussoMisure>
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataMisura>10/2018</DataMisura>
      </DatiPod>
      <DatiPod>
        <Pod>IT001E49161678</Pod>
        <MeseAnno>10/2018</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>IT001E49161677</Pod>
        <MeseAnno>10/2018</MeseAnno>
        <DataMisura>01/10/2018</DataMisura>
      </DatiPod>
    </FlussoMisure>

    val rfo1gSpecialCaseOK = <FlussoMisure>
      <DatiPod>
        <Pod>pod1</Pod>
        <MeseAnno>10/2018</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>pod1</Pod>
        <MeseAnno>09/2018</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>pod1</Pod>
        <MeseAnno>08/2018</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>pod2</Pod>
        <MeseAnno>10/2018</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>pod2</Pod>
        <MeseAnno>09/2017</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>pod2</Pod>
        <MeseAnno>08/2018</MeseAnno>
      </DatiPod>
    </FlussoMisure>

    val rfo1gSpecialCaseBad = <FlussoMisure>
      <DatiPod>
        <Pod>pod1</Pod>
        <MeseAnno>10/2018</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>pod1</Pod>
        <MeseAnno>09/2018</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>pod1</Pod>
        <MeseAnno>01/2019</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>pod2</Pod>
        <MeseAnno>10/2012</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>pod2</Pod>
        <MeseAnno>09/2012</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>pod2</Pod>
        <DataMisura>10/08/2019</DataMisura>
      </DatiPod>
    </FlussoMisure>

    val rfo1gSpecialCaseOK2 = <FlussoMisure>
      <DatiPod>
        <Pod>pod1</Pod>
        <MeseAnno>11/2018</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>pod1</Pod>
        <MeseAnno>01/2019</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>pod1</Pod>
        <MeseAnno>10/2018</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>pod2</Pod>
        <MeseAnno>10/2020</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>pod2</Pod>
        <MeseAnno>09/2020</MeseAnno>
      </DatiPod>
      <DatiPod>
        <Pod>pod2</Pod>
        <DataMisura>08/10/2018</DataMisura>
      </DatiPod>
    </FlussoMisure>

    val xmlWithMeta2018SNM = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "201810",
      flusso = "SM",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "SM"
    )
    val xmlWithMeta2018RSN = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "201810",
      flusso = "RSN",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "RSN"
    )
    val xmlWithMeta2018RSN2G = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "201810",
      flusso = "RSN2G",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "RSN2G"
    )

    val xmlWithMeta2018 = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "201810",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "PDO"
    )

    val rfo1gWithMeta2018 = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "201810",
      flusso = "RFO",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "RFO"
    )

    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleDatiFile.condition(datiPod2018OK, xmlWithMeta2018, None))

    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleDatiFile.condition(datiPod2018minus1Month, xmlWithMeta2018, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleDatiFile.condition(datiPod2018minus1Month, xmlWithMeta2018SNM, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleDatiFile.condition(datiPod2018minus1Month, xmlWithMeta2018RSN, None))

    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleDatiFile.condition(datiPod2018DataMisura, xmlWithMeta2018, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleDatiFile.condition(datiPod2018DataMisura, xmlWithMeta2018SNM, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleDatiFile.condition(datiPod2018DataMisura, xmlWithMeta2018RSN, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleDatiFile.condition(datiPod2018DataMisura, xmlWithMeta2018RSN2G, None))

    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleDatiFile.condition(datiPod2018MultipleNodesOK, xmlWithMeta2018, None))
    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleDatiFile.condition(datiPod2018MultipleNodesError, xmlWithMeta2018, None))

    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleDatiFile.condition(datiPod2018MultipleNodesBadFormat, xmlWithMeta2018, None))
    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleDatiFile.condition(datiPod2018MultipleNodesBadFormat, xmlWithMeta2018SNM, None))
    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleDatiFile.condition(datiPod2018MultipleNodesBadFormat, xmlWithMeta2018RSN, None))
    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleDatiFile.condition(datiPod2018MultipleNodesBadFormat, xmlWithMeta2018RSN2G, None))

    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleDatiFile.condition(rfo1gSpecialCaseOK, rfo1gWithMeta2018, None))
    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleDatiFile.condition(rfo1gSpecialCaseBad, rfo1gWithMeta2018, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleDatiFile.condition(rfo1gSpecialCaseOK2, rfo1gWithMeta2018, None))
  }

  def testRulePIVADistributore(): Unit = {

    val xmlWithMetaGood = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "05724831002",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "PDO"
    )

    val xmlWithMetaBad = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "05724831001",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "PDO"
    )

    val xmlFile = <FlussoMisure>
      <IdentificativiFlusso>
        <PIvaUtente></PIvaUtente>
        <PIvaDistributore>05724831002</PIvaDistributore>
        <CodContrDisp></CodContrDisp>
      </IdentificativiFlusso>
    </FlussoMisure>

    Assert.assertTrue(CheckAmmissibilitaFileRules.rulePIVADistributore.condition(xmlFile, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.rulePIVADistributore.condition(xmlFile, xmlWithMetaGood, None))

  }

  def testRulePIVADistributorePratica(): Unit = {
    val xmlWithMetaGood = new XMLMetadata(
      file = new File("/TME_09876543210_12345678901/2017/0502/file.xml"),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = ""
    )

    val xmlWithMetaBad = new XMLMetadata(
      file = new File("/TME_00987654321_12345678901/2017/0502/file.xml"),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = ""
    )

    val xmlFile = <FlussoMisure>
      <IdentificativiFlusso>
        <PIvaUtente>12345678901</PIvaUtente>
        <PIvaDistributore>09876543210</PIvaDistributore>
        <CodContrDisp></CodContrDisp>
      </IdentificativiFlusso>
    </FlussoMisure>

    Assert.assertTrue(CheckAmmissibilitaFileRules.rulePIVADistributorePratica.condition(xmlFile, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.rulePIVADistributorePratica.condition(xmlFile, xmlWithMetaGood, None))

  }


  def testRulePIVAUdd(): Unit = {

    val xmlWithMetaGood = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "05724831002",
      annoMese = "202010",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "PDO"
    )

    val xmlWithMetaBad = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "05724831001",
      annoMese = "202010",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "PDO"
    )

    val xmlFile = <FlussoMisure>
      <IdentificativiFlusso>
        <PIvaUtente>05724831002</PIvaUtente>
        <PIvaDistributore></PIvaDistributore>
        <CodContrDisp></CodContrDisp>
      </IdentificativiFlusso>
    </FlussoMisure>

    Assert.assertTrue(CheckAmmissibilitaFileRules.rulePIVAUdd.condition(xmlFile, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.rulePIVAUdd.condition(xmlFile, xmlWithMetaGood, None))

  }

  def testRulePIVAUddRcu(): Unit = {
    val mapPivaRCU = Map(
      "01234567890" -> (
        LocalDateTime.of(2020, 11, 1, 0, 0, 0),
        LocalDateTime.of(2020, 11, 1, 0, 0, 0)
      )
    )
    val xmlWithMetaGood = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "01234567890",
      annoMese = "202011",
      mapPivaRcu = Environment.getSpark.sparkContext.broadcast(mapPivaRCU),
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = ""
    )

    val xmlWithMetaBad = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "01234567890",
      annoMese = "202012",
      mapPivaRcu = Environment.getSpark.sparkContext.broadcast(mapPivaRCU),
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = ""
    )

    val xmlWithMetaSkippingRule = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      mapPivaRcu = null,
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "DP0426",
      sm = ""
    )

    Assert.assertTrue(CheckAmmissibilitaFileRules.rulePIVAUddRcu.condition(null, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.rulePIVAUddRcu.condition(null, xmlWithMetaGood, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.rulePIVAUddRcu.condition(null, xmlWithMetaSkippingRule, None))

  }

  def testRuleCodDp(): Unit = {
    val xmlWithMetaGood = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "05724831002",
      annoMese = "202010",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "DP0001",
      sm = "",
      codFlusso = "PDO"
    )

    val xmlWithMetaBad = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "05724831001",
      annoMese = "202010",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "DP0011",
      sm = "",
      codFlusso = "PDO"
    )

    val xmlFile = <FlussoMisure>
      <IdentificativiFlusso>
        <PIvaUtente>12345678901</PIvaUtente>
        <PIvaDistributore>01234567890</PIvaDistributore>
        <CodContrDisp>DP0001</CodContrDisp>
      </IdentificativiFlusso>
    </FlussoMisure>

    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleCodDp.condition(xmlFile, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleCodDp.condition(xmlFile, xmlWithMetaGood, None))

  }

  def testRuleCodDpPIVA(): Unit = {
    val mapCodDPPivaUDD = Map("DP0001" -> Set("01234567890"))

    val xmlWithMetaBad = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "12345678901",
      annoMese = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "DP0001",
      mapCodDPRcuPivaUdd = Environment.getSpark.sparkContext.broadcast(mapCodDPPivaUDD),
      sm = ""
    )
    val xmlWithMetaGood = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "01234567890",
      annoMese = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "DP0001",
      mapCodDPRcuPivaUdd = Environment.getSpark.sparkContext.broadcast(mapCodDPPivaUDD),
      sm = ""
    )

    val xmlWithMetaSkippingRule = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "DP0426",
      mapCodDPRcuPivaUdd = Environment.getSpark.sparkContext.broadcast(mapCodDPPivaUDD),
      sm = ""
    )
    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleCodDpPIVA.condition(null, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleCodDpPIVA.condition(null, xmlWithMetaGood, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleCodDpPIVA.condition(null, xmlWithMetaSkippingRule, None))

  }

  def testRuleTimestamp(): Unit = {

    val xmlWithMetaBad = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "",
      timestamp = "201003002010",
      progressivo = "",
      codDp = "",
      sm = ""
    )
    val xmlWithMetaGood = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "",
      timestamp = "20201003003023",
      progressivo = "",
      codDp = "",
      sm = ""
    )

    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleTimestamp.condition(null, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleTimestamp.condition(null, xmlWithMetaGood, None))

  }

  def testRuleCodFlusso(): Unit = {

    val xmlWithMetaGood = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "")

    val xmlWithMetaBad = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "PDA",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "")

    val xmlFile = <FlussoMisure CodFlusso="PDO">
      <IdentificativiFlusso>
        <PIvaUtente>12345678901</PIvaUtente>
        <PIvaDistributore>01234567890</PIvaDistributore>
        <CodContrDisp>DP0001</CodContrDisp>
      </IdentificativiFlusso>
    </FlussoMisure>

    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleCodFlusso.condition(xmlFile, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleCodFlusso.condition(xmlFile, xmlWithMetaGood, None))


  }

  def testRuleStatoMisuratore(): Unit = {
    val xmlWithMetaGood = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "R"
    )

    val xmlWithMetaBad = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "RN"
    )


    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleStatoMisuratore.condition(null, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleStatoMisuratore.condition(null, xmlWithMetaGood, None))

  }


  def testRuleStatoMisuratoreFlusso(): Unit = {

    val xmlWithMetaGood = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "PDO2G",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "N"
    )

    val xmlWithMetaBad = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "PDO2G",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "NR"
    )

    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleStatoMisuratoreFlusso.condition(null, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleStatoMisuratoreFlusso.condition(null, xmlWithMetaGood, None))

  }

  def testRuleAlreadyTransmitted(): Unit = {

    val xmlMetaTrue = new XMLMetadata(
      file = null,
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      alreadyTransmitted = true)

    val xmlMetaFalse = new XMLMetadata(
      file = null,
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      alreadyTransmitted = false)

    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleAlreadyTransmitted.condition(null, xmlMetaFalse, None)) //only one file pass
    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleAlreadyTransmitted.condition(null, xmlMetaTrue, None)) //at least one pass
  }

  def testRuleFlusso1XsdVal(): Unit = {

    val factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
    val flusso1XSDBroad = factory.newSchema(new StreamSource(PropertyUtility.getXsdPeriodicoPath)).newValidator()
    val flusso2XSDBroad = factory.newSchema(new StreamSource(PropertyUtility.getXsdRettificaPath)).newValidator()

    val file = new File("src/test/resources/input/checkAmmissibilitaFile/flusso1Good.xml")
    val xmlMeta = new XMLMetadata(
      file = file,
      pivaDistributore = "01234567890",
      pivaUDD = "12345678901",
      annoMese = "201301",
      flusso = "SM",
      timestamp = "20130218060523",
      progressivo = "",
      codDp = "1DP0001",
      sm = "R"
    ).copy(flusso1XSDBroad = flusso1XSDBroad, flusso2XSDBroad = flusso2XSDBroad)

    val fileBad = new File("src/test/resources/input/checkAmmissibilitaFile/flusso1Bad.xml")
    val xmlMetaBad = new XMLMetadata(
      file = fileBad,
      pivaDistributore = "01234567890",
      pivaUDD = "12345678901",
      annoMese = "201301",
      flusso = "SM",
      timestamp = "20130218060523",
      progressivo = "",
      codDp = "1DP0001",
      sm = "BAD"
    ).copy(flusso1XSDBroad = flusso1XSDBroad, flusso2XSDBroad = flusso2XSDBroad)

    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleFlusso1XsdVal.condition(XML.load(file.getAbsolutePath), xmlMeta, None))
    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleFlusso1XsdVal.condition(XML.load(fileBad.getAbsolutePath), xmlMetaBad, None))


  }

  def testRuleFlusso2XsdVal(): Unit = {
    val factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
    val flusso1XSDBroad = factory.newSchema(new StreamSource(PropertyUtility.getXsdPeriodicoPath)).newValidator()
    val flusso2XSDBroad = factory.newSchema(new StreamSource(PropertyUtility.getXsdRettificaPath)).newValidator()

    val file = new File("src/test/resources/input/checkAmmissibilitaFile/flusso2Good.xml")
    val xmlMeta = new XMLMetadata(
      file = file,
      pivaDistributore = "01234567890",
      pivaUDD = "12345678901",
      annoMese = "201301",
      flusso = "RTR",
      timestamp = "20130218060523",
      progressivo = "",
      codDp = "1DP0001",
      sm = "R"
    ).copy(flusso1XSDBroad = flusso1XSDBroad, flusso2XSDBroad = flusso2XSDBroad)

    val fileBad = new File("src/test/resources/input/checkAmmissibilitaFile/flusso2Bad.xml")
    val xmlMetaBad = new XMLMetadata(
      file = fileBad,
      pivaDistributore = "01234567890",
      pivaUDD = "12345678901",
      annoMese = "201301",
      flusso = "RTR",
      timestamp = "20130218060523",
      progressivo = "",
      codDp = "1DP0001",
      sm = "BAD"
    ).copy(flusso1XSDBroad = flusso1XSDBroad, flusso2XSDBroad = flusso2XSDBroad)

    Assert.assertFalse(CheckAmmissibilitaFileRules.ruleFlusso2XsdVal.condition(XML.load(file.getAbsolutePath), xmlMeta, None))
    Assert.assertTrue(CheckAmmissibilitaFileRules.ruleFlusso2XsdVal.condition(XML.load(fileBad.getAbsolutePath), xmlMetaBad, None))


  }

  def testRulePastFutureMonth(): Unit = {
    // set the process Now to 11st-10-2020. Past and future rules are tested wrt this date (process Now), obtained as metadata
    val processParams = AmmissibilitaParameters(
      year = "2020",
      month = "10",
      day = "11"
    )

    val xmlWithMetaGood = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202009",
      flusso = "PDO2G",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "N",
      params = processParams
    )

    val xmlWithMetaBadFuture = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202011",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      params = processParams
    )
    val xmlWithMetaBadPast = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "201410",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      params = processParams
    )

    val xmlWithMetaBadPast69thMonth = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "201501",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      params = processParams
    )

    val xmlWithMetaGoodPast67thMonth = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "201503",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      params = processParams
    )

    Assert.assertTrue(CheckAmmissibilitaFileRules.rulePastFutureMonth.condition(null, xmlWithMetaBadPast, None))
    Assert.assertTrue(CheckAmmissibilitaFileRules.rulePastFutureMonth.condition(null, xmlWithMetaBadFuture, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.rulePastFutureMonth.condition(null, xmlWithMetaGood, None))
    Assert.assertFalse(CheckAmmissibilitaFileRules.rulePastFutureMonth.condition(null, xmlWithMetaGoodPast67thMonth, None))
    Assert.assertTrue(CheckAmmissibilitaFileRules.rulePastFutureMonth.condition(null, xmlWithMetaBadPast69thMonth, None))

  }
}
