package it.au.misure.ee_switching.utility

import java.io.File
import com.typesafe.config.ConfigFactory
import it.au.misure.ee_switching.model.schema.xml.FileXml
import it.au.misure.ee_switching.utility.Constants.{FUNZIONALI, STORICI}
import it.au.misure.ee_switching.utility.environment.Environment
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Ignore, Test}

@Ignore
class TestValidateUtility extends EnvironmentSparkTest {
  
  @Test
  def testValidateXmlFiles(): Unit = {

    val sc = Environment.getSpark.sparkContext

    val validationInputPath: String = "src/test/resources/files/validation"

    val funzionaliFilesXmlList = List(
      FileXml("01234567890_12345678901_202002_F2G_20191103101533_1DP0234_OK.xml",
        new File(s"${validationInputPath}/01234567890_12345678901_202002_F2G_20191103101533_1DP0234_OK.xml"),
        List("IT1234567890123"),
        outputZipPath = FileUtility.getZipOutputFilePath("01234567890_12345678901_202002_F2G_20191103101533_1DP0234_OK.xml", FUNZIONALI)),
      FileXml("01234567890_12345678901_202002_F2G_20191103101533_1DP0234_KO.xml",
        new File(s"${validationInputPath}/01234567890_12345678901_202002_F2G_20191103101533_1DP0234_KO.xml"),
        List("IT1234567890123"),
        outputZipPath = FileUtility.getZipOutputFilePath("01234567890_12345678901_202002_F2G_20191103101533_1DP0234_KO.xml", FUNZIONALI))
    )
    val storiciFilesXmlList = List(
      FileXml("01234567890_12345678901_202002_S2G_20200226101533_1DP0234_OK.xml",
        new File(s"${validationInputPath}/01234567890_12345678901_202002_S2G_20200226101533_1DP0234_OK.xml"),
        List("IT1234567890123"),
        outputZipPath = FileUtility.getZipOutputFilePath("01234567890_12345678901_202002_S2G_20200226101533_1DP0234_OK.xml", STORICI)),
      FileXml("01234567890_12345678901_202002_S2G_20200226101533_1DP0234_KO.xml",
        new File(s"${validationInputPath}/01234567890_12345678901_202002_S2G_20200226101533_1DP0234_KO.xml"),
        List("IT1234567890123"),
        outputZipPath = FileUtility.getZipOutputFilePath("01234567890_12345678901_202002_S2G_20200226101533_1DP0234_KO.xml", STORICI))
    )

    val funzionaliXmlChunksRdd = sc.parallelize(funzionaliFilesXmlList)
    val storiciXmlChunksRdd = sc.parallelize(storiciFilesXmlList)

    ValidateUtility.validateXmlFiles(funzionaliXmlChunksRdd, FUNZIONALI).foreach(fileXml => {
      if (fileXml.file.getName.split("/").last.contains("OK"))
        Assert.assertTrue(fileXml.errorListXSD.equals(Constants.OK))
      if (fileXml.file.getName.split("/").last.contains("KO"))
        Assert.assertTrue(!fileXml.errorListXSD.equals(Constants.OK) && fileXml.errorListXSD.nonEmpty)
    })

    ValidateUtility.validateXmlFiles(storiciXmlChunksRdd, STORICI).foreach(fileXml => {
      if (fileXml.file.getName.split("/").last.contains("OK"))
        Assert.assertTrue(fileXml.errorListXSD.equals(Constants.OK))
      if (fileXml.file.getName.split("/").last.contains("KO"))
        Assert.assertTrue(!fileXml.errorListXSD.equals(Constants.OK) && fileXml.errorListXSD.nonEmpty)
    })

  }

}
