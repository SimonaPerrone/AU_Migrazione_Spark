package it.eng.au.eng.ammissibilita.file

//import it.au.misure.cli.FlussoMisureTool
import it.eng.au.ammissibilita.pod.CheckAmmissibilitaPod.{filenameRegexSMIS, getXMLMetadataSMIS}
import it.eng.au.args.AmmissibilitaParameters
import it.eng.au.model.XMLMetadata
import it.eng.au.schema.{RcuAziendaPSchema, RcuPodDistrPSchema, RcuPodPSchema, RcuUddPSchema, RcusPodDistrPSchema, RcusPodPSchema}
import it.eng.au.utility.{Constants, SystemUtility}
import junit.framework.TestCase
import it.eng.au.ammissibilita.file.CheckAmmissibilitaFile.getRCUMaps
import it.eng.au.ammissibilita.file.CheckAmmissibilitaFileRulesSMIS
import it.eng.au.eng.utility.{EnvironmentSparkTest, SparkLocal}
import it.eng.au.utility.environment.Environment
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Ignore

import scala.util.Try
import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, YearMonth}
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.{SchemaFactory, Validator}
import scala.xml.XML

@Ignore
class TestCheckAmmissibilitaFileSMIS extends EnvironmentSparkTest {
  SystemUtility.setLocalLaunch()

  /*
  def testRun(): Unit = {
    val arguments: Array[String] = Array(
      "-ia",
      "-g",
      "-y", "2019",
      "-m", "09",
      "-s", "16"
    )

    FlussoMisureTool.main(arguments)
  }*/

  def testCheckAmmissibilitaFileFlow(): Unit = {
    SystemUtility.setLocalLaunch()

    val file = new File("src/test/resources/input_SMIS_prova/SMIS_01234567890_12345678901/2018/1203/01234567890_12345678901_201811_SMIS_20181203101533_1DP0001.xml")
    val fileWithMatches = Seq(file).map(file => (file, filenameRegexSMIS.findFirstMatchIn(file.getName)))

    val params = AmmissibilitaParameters(
      g = Constants._1G,
      year = "2020",
      month = "12",
      day = "15"
    )
    val fileWithMeta = fileWithMatches.map({ case (file, matches) =>
      val factory: SchemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
      var flussoSMISXSDBroad: Validator = factory.newSchema(new StreamSource("src/main/resources/deploy/XSD_SMIS/FlussiDatiMisuraPrelievoEE-Flusso1-RiprogrSostMisuratore.xsd")).newValidator()
      //      val flusso2XSDBroad: Validator = factory.newSchema(new StreamSource("src/main/resources/deploy/XSD_EE/FlussiDatiMisuraPrelievoEE-Flusso2-Rettifica.xsd")).newValidator()

      getXMLMetadataSMIS(file, matches, params).copy(
        flusso1XSDBroad = null,
        flusso2XSDBroad = null,
        flussoSMISXSDBroad = flussoSMISXSDBroad,
        mapFileNames = null,
        mapCodDPRcuPivaUdd = Environment.getSpark.sparkContext.broadcast(Map("" -> Set(""))),
        mapPivaRcu = Environment.getSpark.sparkContext.broadcast(Map("12345678901" -> (LocalDateTime.MIN, LocalDateTime.MAX)))
      )
    }
    )
    val fileWithMessages = CheckAmmissibilitaFileRulesSMIS.check(XML.loadFile(fileWithMeta.head.file), fileWithMeta.head)

    println(fileWithMessages)
    println(XML.loadFile(fileWithMeta.head.file))
    println(fileWithMeta.head)


  }

  def testRecoverRddXmlWithMetaSMIS(): Unit = {

    val params = AmmissibilitaParameters(
      g = Constants._1G,
      year = "2018",
      month = "12",
      day = "03",
      isSmis = true
    )

    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._

    val rcuAzienda = Environment.getSpark.sparkContext.parallelize(
      List(
        ("azienda1", "0012345"),
        ("azienda2", "0045678"),
        ("azienda3", "0067890")
      )
    ).toDF(RcuAziendaPSchema.n_id_azienda, RcuAziendaPSchema.t_piva)

    val rcuUddP = Environment.getSpark.sparkContext.parallelize(
      List(
        ("DP0001", "azienda1", "null", "null"),
        ("DP0002", "azienda2", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0"),
        ("DP0003", "azienda3", "2014-08-01 00:00:00.0", "null")
      )
    ).toDF(RcuUddPSchema.t_codice_terna, RcuUddPSchema.n_id_udd, RcuUddPSchema.d_inizio, RcuUddPSchema.d_fine)

    /*val rcuUddP = Environment.getSpark.sparkContext.parallelize(
  List(
    ("DP0001", "azienda1", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0"),
    ("DP0002", "azienda2", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0"),
    ("DP0003", "azienda3", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0")
  )
).toDF(RcuUddPSchema.t_codice_terna, RcuUddPSchema.n_id_udd, RcuUddPSchema.d_inizio, RcuUddPSchema.d_fine)*/

    val (mapPivaRcu, _) = getRCUMaps(rcuAzienda, rcuUddP, params)
    val mapPivaRcuBC = Environment.getSpark.sparkContext.broadcast(mapPivaRcu)
    val annoMeseFile = Try(YearMonth.parse("202108", DateTimeFormatter.ofPattern("yyyyMM")))
    val pivaUddRCUMap = mapPivaRcuBC.value
    val tpl = pivaUddRCUMap.get("0012345")
    val (pivaStartDate, pivaEndDate) = tpl.get
    val condition1 = annoMeseFile.isFailure
    val condition2 = annoMeseFile.get.isBefore(YearMonth.from(pivaStartDate))
    val condition3 = annoMeseFile.get.isAfter(YearMonth.from(pivaEndDate))

    println(s"mapPivaRcu: " + mapPivaRcu)
    println(s"annoMeseFile: " + annoMeseFile)
    println(s"tpl: " + tpl)
    println(s"pivaStartDate: " + pivaStartDate + "\npivaEndDate: " + pivaEndDate)
    println(s"condition1: " + condition1 + "\ncondition2: " + condition2 + "\ncondition3: " + condition3 + "\nfinal condition: " + (condition1 || condition2 || condition3))
    println("\n\n\n\n\n")
  }

  def testRegex(): Unit = {
    //    val regex: Regex = "^([A-Za-z0-9]+)_([A-Za-z0-9]+)_(\\d+)_(SMIS)_(\\d+)_(\\d+)(DP\\d+).xml$".r
    val regex = "(DP|dp|Dp|dP)([1-9][0-9][0-9][0-9]|000[1-9])"
    val x = "DP0001"
    x.matches(regex)
  }

}
