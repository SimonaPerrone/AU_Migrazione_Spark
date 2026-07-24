package it.au.misure.eng.ammissibilita.file

import it.au.misure.eng.args.AmmissibilitaParameters
import it.au.misure.eng.model.{RuleParameters, XMLMetadata}
import it.au.misure.eng.utility.{PropertyUtility, SparkLocal, SystemUtility}
import junit.framework.TestCase
import org.junit.Assert

import java.io.File
import java.time.LocalDateTime
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory
import scala.xml.XML
import scala.util.{Failure, Success, Try}

class TestCheckAmmissibilitaFileRulesSMIS extends TestCase with SparkLocal {
  SystemUtility.setLocalLaunch()

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

    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.ruleAlreadyTransmitted.condition(null, xmlMetaFalse, None)) //only one file pass
    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.ruleAlreadyTransmitted.condition(null, xmlMetaTrue, None)) //at least one pass
  }

  def testRulePIVARcuDistr(): Unit = {
    val listPivaRcuDistr = List("01234567890")
    val xmlWithMetaGood = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "01234567890",
      pivaUDD = "",
      annoMese = "202011",
      listPivaRcuDistr = sc.broadcast(listPivaRcuDistr),
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = ""
    )

    val xmlWithMetaBad = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "99999999999",
      pivaUDD = "",
      annoMese = "202012",
      listPivaRcuDistr = sc.broadcast(listPivaRcuDistr),
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = ""
    )

    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.rulePIVARcuDistr.condition(null, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.rulePIVARcuDistr.condition(null, xmlWithMetaGood, None))
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

    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.rulePIVADistributorePratica.condition(xmlFile, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.rulePIVADistributorePratica.condition(xmlFile, xmlWithMetaGood, None))

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
      mapPivaRcu = sc.broadcast(mapPivaRCU),
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
      mapPivaRcu = sc.broadcast(mapPivaRCU),
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = ""
    )

    val xmlWithMetaGood2 = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "01234567890",
      annoMese = "202012",
      mapPivaRcu = sc.broadcast(mapPivaRCU),
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "DP7001",
      sm = ""
    )

    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.rulePIVAUddRcu.condition(null, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.rulePIVAUddRcu.condition(null, xmlWithMetaGood, None))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.rulePIVAUddRcu.condition(null, xmlWithMetaGood2, None))
  }

  def testRuleCodDpPIVA():Unit = {
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
      mapCodDPRcuPivaUdd = sc.broadcast(mapCodDPPivaUDD) ,
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
      mapCodDPRcuPivaUdd = sc.broadcast(mapCodDPPivaUDD) ,
      sm = ""
    )

    val xmlWithMetaGood2 = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "01234567890",
      annoMese = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "DP0426",
      mapCodDPRcuPivaUdd = sc.broadcast(mapCodDPPivaUDD) ,
      sm = ""
    )

    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.ruleCodDpPIVA.condition(null,xmlWithMetaBad,None))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.ruleCodDpPIVA.condition(null,xmlWithMetaGood,None))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.ruleCodDpPIVA.condition(null,xmlWithMetaGood2,None))

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

    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.ruleTimestamp.condition(null, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.ruleTimestamp.condition(null, xmlWithMetaGood, None))

  }

  def testRulePastFutureMonth():Unit = {
    val rulesParameters = PropertyUtility.getParametersMap(CheckAmmissibilitaFileRulesSMIS.ammissibilitaType)
    val ruleParameter = rulesParameters.get("rulePastFutureMonth")

    // set the process Now to 11st-10-2020. Past and future rules are tested wrt this date (process Now), obtained as metadata
    val processParams = AmmissibilitaParameters(
      year = "2020",
      month = "10",
      day = "11",
      isSmis = true
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

    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.rulePastFutureMonth.condition(null,xmlWithMetaBadPast, ruleParameter))
    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.rulePastFutureMonth.condition(null,xmlWithMetaBadFuture, ruleParameter))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.rulePastFutureMonth.condition(null,xmlWithMetaGood, ruleParameter))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.rulePastFutureMonth.condition(null,xmlWithMetaGoodPast67thMonth, ruleParameter))
    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.rulePastFutureMonth.condition(null,xmlWithMetaBadPast69thMonth, ruleParameter))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.rulePastFutureMonth.condition(null,xmlWithMetaGood, rulesParameters.get("")))
    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.rulePastFutureMonth.condition(null,xmlWithMetaGoodPast67thMonth, Option(RuleParameters(true,true,Map("x"->"50")))))

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

    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.rulePIVADistributore.condition(xmlFile, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.rulePIVADistributore.condition(xmlFile, xmlWithMetaGood, None))

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

    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.rulePIVAUdd.condition(xmlFile, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.rulePIVAUdd.condition(xmlFile, xmlWithMetaGood, None))

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

    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.ruleCodDp.condition(xmlFile, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.ruleCodDp.condition(xmlFile, xmlWithMetaGood, None))

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

    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.ruleCodFlusso.condition(xmlFile, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.ruleCodFlusso.condition(xmlFile, xmlWithMetaGood, None))


  }


  def testRuleCodContrDispStruct(): Unit = {

    val xmlFileGod = <FlussoMisure CodFlusso="PDO">
      <IdentificativiFlusso>
        <PIvaUtente>12345678901</PIvaUtente>
        <PIvaDistributore>01234567890</PIvaDistributore>
        <CodContrDisp>DP0060</CodContrDisp>
      </IdentificativiFlusso>
    </FlussoMisure>

    val xmlFileBad = <FlussoMisure CodFlusso="PDO">
      <IdentificativiFlusso>
        <PIvaUtente>12345678901</PIvaUtente>
        <PIvaDistributore>01234567890</PIvaDistributore>
        <CodContrDisp>DP0000</CodContrDisp>
      </IdentificativiFlusso>
    </FlussoMisure>

    val xmlFileGod2 = <FlussoMisure CodFlusso="PDO">
      <IdentificativiFlusso>
        <PIvaUtente>12345678901</PIvaUtente>
        <PIvaDistributore>01234567890</PIvaDistributore>
        <CodContrDisp>dp0001</CodContrDisp>
      </IdentificativiFlusso>
    </FlussoMisure>

    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.ruleCodContrDispStruct.condition(xmlFileGod, null, None))
    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.ruleCodContrDispStruct.condition(xmlFileBad, null, None))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.ruleCodContrDispStruct.condition(xmlFileGod2, null, None))

  }

  def testRulePIVARcuEmtCodContrDisp(): Unit = {
    val listPivaRcuEmt = List("01234567890")

    val xmlFileGod = <FlussoMisure CodFlusso="PDO">
      <IdentificativiFlusso>
        <PIvaUtente>01234567890</PIvaUtente>
        <PIvaDistributore>01234567890</PIvaDistributore>
        <CodContrDisp>DP0426</CodContrDisp>
      </IdentificativiFlusso>
    </FlussoMisure>

    val xmlFileBad = <FlussoMisure CodFlusso="PDO">
      <IdentificativiFlusso>
        <PIvaUtente>01234567890</PIvaUtente>
        <PIvaDistributore>01234567890</PIvaDistributore>
        <CodContrDisp>DP0001</CodContrDisp>
      </IdentificativiFlusso>
    </FlussoMisure>

    val xmlWithMetaGood = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "01234567890",
      annoMese = "202011",
      listPivaRcuEmt = sc.broadcast(listPivaRcuEmt),
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = ""
    )

    val xmlWithMetaBad = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "99999999999",
      annoMese = "202012",
      listPivaRcuEmt = sc.broadcast(listPivaRcuEmt),
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = ""
    )

    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.rulePIVARcuEmtCodContrDisp.condition(xmlFileGod, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.rulePIVARcuEmtCodContrDisp.condition(xmlFileGod, xmlWithMetaGood, None))
    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.rulePIVARcuEmtCodContrDisp.condition(xmlFileBad, xmlWithMetaGood, None))

  }

  def testRuleFlusso1XsdVal(): Unit = {

    val factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
    val flussoSMISXSDBroad = factory.newSchema(new StreamSource(PropertyUtility.getXsdSMISPath)).newValidator()

    val file = new File("src/test/resources/input/checkAmmissibilitaFileSMIS/flusso1Good.xml")
    val xmlMeta = new XMLMetadata(
      file = file,
      pivaDistributore = "01234567890",
      pivaUDD = "12345678901",
      annoMese = "201811",
      flusso = "SMIS",
      timestamp = "20181203101533",
      progressivo = "",
      codDp = "1DP0001",
      sm = null
    ).copy(flussoSMISXSDBroad = flussoSMISXSDBroad)

    val fileBad = new File("src/test/resources/input/checkAmmissibilitaFileSMIS/flusso1Bad.xml")
    val xmlMetaBad = new XMLMetadata(
      file = file,
      pivaDistributore = "12345678901",
      pivaUDD = "01234567890",
      annoMese = "201811",
      flusso = "SMIS",
      timestamp = "20181203101533",
      progressivo = "",
      codDp = "1DP0001",
      sm = null
    ).copy(flusso1XSDBroad = flussoSMISXSDBroad)

    Assert.assertFalse(CheckAmmissibilitaFileRulesSMIS.ruleFlussoXsdVal.condition(XML.load(file.getAbsolutePath), xmlMeta, None))
    Assert.assertTrue(CheckAmmissibilitaFileRulesSMIS.ruleFlussoXsdVal.condition(XML.load(fileBad.getAbsolutePath), xmlMetaBad, None))


  }
  def test(): Unit = {
    import sqlContext.implicits._
//    val rowsRdd = sc.parallelize(
//      List(
//        ("first", 2.0, 7.0),
//        ("second", 3.5, 2.5),
//        ("third", 7.0, 5.9)
//      )
//    ).toDF("t_piva", "a", "b")
    val rowsRdd = sc.parallelize(
      List(
        ("first", (2.0, 7.0)),
        ("second", (3.5, 2.5)),
        ("third", (7.0, 5.9))
      )
    )
    val rowsRdd2 = sc.parallelize(
      List(
        ("first", (2.0, 7.0)),
        ("second", (3.5, 2.5)),
        ("third", (7.0, 5.9))
      )
    )
     val res=  rowsRdd.union(rowsRdd2).reduceByKey{(a,b) => a}

    res.collect().foreach(println)
  }
}