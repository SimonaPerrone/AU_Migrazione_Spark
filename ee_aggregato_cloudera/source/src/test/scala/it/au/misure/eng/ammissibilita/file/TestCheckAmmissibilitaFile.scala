package it.au.misure.eng.ammissibilita.file

import it.au.misure.cli.FlussoMisureTool
import it.au.misure.eng.ammissibilita.CheckAmmissibilitaRules
import it.au.misure.eng.ammissibilita.file.CheckAmmissibilitaFile.{filenameRegex, getXMLMetadata}
import it.au.misure.eng.args.AmmissibilitaParameters
import it.au.misure.eng.model.{ReportEsitoFILEMessage, XMLMetadata}
import it.au.misure.eng.utility.{Constants, SystemUtility}
import junit.framework.TestCase
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Assert, Ignore}

import java.io.File
import java.time.LocalDateTime
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.{SchemaFactory, Validator}
import scala.xml.XML

@Ignore
class TestCheckAmmissibilitaFile extends TestCase {
  SystemUtility.setLocalLaunch()

  def conf = new SparkConf()
    .setAppName("TEST")
    .setMaster("local[*]")

  def sc = SparkContext.getOrCreate(conf)

  def testRun(): Unit = {
    val arguments: Array[String] = Array(
      "-ia",
      "-g",
      "-y", "2019",
      "-m", "09",
      "-s", "16"
    )

    FlussoMisureTool.main(arguments)
  }

  def testCheckAmmissibilitaFileFlow(): Unit = {
    SystemUtility.setLocalLaunch()

    val file = new File("src/test/resources/debug/TMP_1G/isilonshare1G/TME_05779711000/DISTRIBUTORE/TME_05779711000_12300020158/2020/1215/05779711000_12300020158_202011_RNO_20201113181140_015DP7001_R.xml")
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
        mapCodDPRcuPivaUdd = sc.broadcast(Map("" -> Set(""))),
        mapPivaRcu = sc.broadcast(Map("12300020158" -> (LocalDateTime.MIN, LocalDateTime.MAX)))
      )
    }
    )

    val checker: CheckAmmissibilitaRules[ReportEsitoFILEMessage] = if (params.isSmis) CheckAmmissibilitaFileRulesSMIS else CheckAmmissibilitaFileRules
    val fileWithMessages = checker.check(XML.loadFile(fileWithMeta.head.file), fileWithMeta.head)
    Assert.assertEquals(Constants.COD906, fileWithMessages.codiceInamissibilita)

  }

  def testRddFilter(): Unit = {
    val words = sc.parallelize(Seq(null, "quick", "brown", "fox", "the"))

    println(words.filter(f => f == null).count())
  }

/*  def testFileWithMatches(): Unit = {
    val file = new File("src/test/resources/TMP_1G/isilonshare1G/TME_01234567890/DISTRIBUTORE/TME_01234567890_12345678901/2018/1203/01234567890_12345678901_201811_SMIS_20181203101533_1DP0001.xml")
    val file2 = new File("src/test/resources/TMP_1G/isilonshare1G/TME_01234567890/DISTRIBUTORE/TME_01234567890_12345678901/2018/1203/01234567890_12345678901_201811_SMIS_20181205101533_1DP0002.xml")
    val file3 = new File("src/test/resources/TMP_1G/isilonshare1G/TME_01234567890/DISTRIBUTORE/TME_01234567890_12345678901/2018/1203/01234567890_12345678901_201811_SMIS_20181205101533_1DP0003.xml")
    val file4 = new File("src/test/resources/TMP_1G/isilonshare1G/TME_01234567890/DISTRIBUTORE/TME_01234567890_12345678901/2018/1203/01234567890_12345678901_201910_SMIS_20191103101533_1DP0234.xml")
    val file5 = new File("src/test/resources/TMP_1G/isilonshare1G/TME_01234567890/DISTRIBUTORE/TME_01234567890_12345678901/2018/1203/01234567890_AAA01234567890BB_201911_SMIS_20191203101533_1DP1234.xml")
    val params = AmmissibilitaParameters(
      g = Constants._1G,
      year = "2018",
      month = "12",
      day = "03",
      isSmis = true
    )
    //    sc.parallelize(List(file, file2, file3, file4, file5).map(file => (file, filenameRegexSMIS.findFirstMatchIn(file.getName))))
    val xm1 = CheckAmmissibilitaFile.recoverPodAndDataMisura(XML.loadFile(file), new XMLMetadata(file = file, pivaDistributore = "01234567890", pivaUDD = "12345678901", annoMese = "201811", flusso = "SMIS", timestamp = "20181203101533", progressivo = "1", codDp = "DP0001", sm = null, params = params))
    val xm2 = CheckAmmissibilitaFile.recoverPodAndDataMisura(XML.loadFile(file2), new XMLMetadata(file = file2, pivaDistributore = "01234567890", pivaUDD = "12345678901", annoMese = "201811", flusso = "SMIS", timestamp = "20181205101533", progressivo = "1", codDp = "DP0002", sm = null, params = params))
    val xm3 = CheckAmmissibilitaFile.recoverPodAndDataMisura(XML.loadFile(file3), new XMLMetadata(file = file3, pivaDistributore = "01234567890", pivaUDD = "12345678901", annoMese = "201811", flusso = "SMIS", timestamp = "20181205101533", progressivo = "1", codDp = "DP0003", sm = null, params = params))
    val xm4 = CheckAmmissibilitaFile.recoverPodAndDataMisura(XML.loadFile(file4), new XMLMetadata(file = file4, pivaDistributore = "01234567890", pivaUDD = "12345678901", annoMese = "201910", flusso = "SMIS", timestamp = "20191103101533", progressivo = "1", codDp = "DP0234", sm = null, params = params))
    val xm5 = CheckAmmissibilitaFile.recoverPodAndDataMisura(XML.loadFile(file5), new XMLMetadata(file = file5, pivaDistributore = "01234567890", pivaUDD = "AAA01234567890BB", annoMese = "201910", flusso = "SMIS", timestamp = "20191203101533", progressivo = "1", codDp = "DP1234", sm = null, params = params))

    println(xm1.pod, xm2.pod, xm3.pod, xm4.pod, xm5.pod, xm1.dataMisura, xm2.dataMisura, xm3.dataMisura, xm4.dataMisura, xm5.dataMisura)

  }*/


}
