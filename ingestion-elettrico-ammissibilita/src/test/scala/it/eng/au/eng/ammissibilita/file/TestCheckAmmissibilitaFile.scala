package it.eng.au.eng.ammissibilita.file

//import it.eng.au.misure.cli.FlussoMisureTool
import it.eng.au.ammissibilita.CheckAmmissibilitaRules
import it.eng.au.ammissibilita.file.CheckAmmissibilitaFile.{filenameRegex, getXMLMetadata}
import it.eng.au.ammissibilita.file.{CheckAmmissibilitaFileRules, CheckAmmissibilitaFileRulesSMIS}
import it.eng.au.args.AmmissibilitaParameters
import it.eng.au.eng.utility.EnvironmentSparkTest
import it.eng.au.model.ReportEsitoFILEMessage
import it.eng.au.utility.environment.Environment
import it.eng.au.utility.{Constants, SystemUtility}
import org.junit.{Assert, Ignore}

import java.io.File
import java.time.LocalDateTime
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.{SchemaFactory, Validator}
import scala.xml.XML

@Ignore
class TestCheckAmmissibilitaFile extends EnvironmentSparkTest {
  def testCheckAmmissibilitaFileFlow(): Unit = {
    val file = new File("src/test/resources/debug/TMP_1G/isilonshare1G/TME_05779711000/DISTRIBUTORE/TME_05779711000_12300020158/2020/1215/05779711000_12300020158_202011_RNO_20201113181140_015DP7001_RBB.xml")
    val fileWithMatches = Seq(file).map(file => (file, filenameRegex.findFirstMatchIn(file.getName)))
    val params = AmmissibilitaParameters(
      g = Constants._1G,
      year = "2020",
      month = "12",
      day = "15",
      isSmis = false
    )
    val fileWithMeta = fileWithMatches.map({ case (file, matches) =>
      val factory: SchemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
      val flusso1XSDBroad: Validator = factory.newSchema(new StreamSource("src/main/resources/deploy/XSD_EE/FlussiDatiMisuraPrelievoEE-Flusso1-Periodico.xsd")).newValidator()
      val flusso2XSDBroad: Validator = factory.newSchema(new StreamSource("src/main/resources/deploy/XSD_EE/FlussiDatiMisuraPrelievoEE-Flusso2-Rettifica.xsd")).newValidator()

      getXMLMetadata(file, matches, params).copy(
        flusso1XSDBroad = flusso1XSDBroad,
        flusso2XSDBroad = flusso2XSDBroad,
        mapFileNames = null,
        mapCodDPRcuPivaUdd = Environment.getSpark.sparkContext.broadcast(Map("" -> Set(""))),
        mapPivaRcu = Environment.getSpark.sparkContext.broadcast(Map("12300020158" -> (LocalDateTime.MIN, LocalDateTime.MAX)))
      )
    }
    )

    val checker: CheckAmmissibilitaRules[ReportEsitoFILEMessage] = if (params.isSmis) CheckAmmissibilitaFileRulesSMIS else CheckAmmissibilitaFileRules
    val fileWithMessages = checker.check(XML.loadFile(fileWithMeta.head.file), fileWithMeta.head)
    Assert.assertEquals(Constants.COD904, fileWithMessages.codiceInamissibilita)
  }
}
