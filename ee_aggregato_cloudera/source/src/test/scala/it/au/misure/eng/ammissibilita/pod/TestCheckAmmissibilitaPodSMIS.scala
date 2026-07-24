package it.au.misure.eng.ammissibilita.pod

import it.au.misure.cli.FlussoMisureTool
import it.au.misure.eng.ammissibilita.file.CheckAmmissibilitaFile
import it.au.misure.eng.ammissibilita.pod.CheckAmmissibilitaPod.{filenameRegexSMIS, getXMLMetadataSMIS, validate, writeAggregatedPodTxtReport, writeOnHive}
import it.au.misure.eng.args.AmmissibilitaParameters
import it.au.misure.eng.model.XMLMetadata
import it.au.misure.eng.schema._
import it.au.misure.eng.utility.{Constants, SparkLocal, SystemUtility}
import junit.framework.TestCase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Ignore

import java.io.File
import java.time.LocalDateTime
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.{SchemaFactory, Validator}
import scala.xml.XML

@Ignore
class TestCheckAmmissibilitaPodSMIS extends TestCase with SparkLocal {
  SystemUtility.setLocalLaunch()

  def testRun(): Unit = {
    SystemUtility.setLocalLaunch()
    val arguments: Array[String] = Array(
      "-iap",
      "-g",
      "-y", "2019",
      "-m", "09",
      "-s", "16"
    )

    FlussoMisureTool.main(arguments)
  }

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
        mapCodDPRcuPivaUdd = sc.broadcast(Map("" -> Set(""))),
        mapPivaRcu = sc.broadcast(Map("12345678901" -> (LocalDateTime.MIN, LocalDateTime.MAX)))
      )
    }
    )
    val fileWithMessages = CheckAmmissibilitaPodRulesSMIS.check(XML.loadFile(fileWithMeta.head.file), fileWithMeta.head)

    println(fileWithMessages)
    println(XML.loadFile(fileWithMeta.head.file))
    println(fileWithMeta.head)


  }


  def testRecoverRddXmlWithMetaSMIS(): Unit = {

    val fileWithMatches = xmlMeta.fileWithMatches

    val params = AmmissibilitaParameters(
      g = Constants._1G,
      year = "2018",
      month = "12",
      day = "03",
      isSmis = true
    )



    //    val fileWithMatch = CheckAmmissibilitaFile.getRDDDaysFolder("/home/luca/IdeaProjects/ee_aggregato_cloudera/source/src/test/resources/TMP_1G/isilonshare1G",params)

    //    val x  = 2

    //    val fileWithMeta = fileWithMatches.map({ case (file, matches) =>
    ////      val factory: SchemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
    //     // var flussoSMISXSDBroad: Validator = factory.newSchema(new StreamSource("src/main/resources/deploy/XSD_SMIS/FlussiDatiMisuraPrelievoEE-Flusso1-RiprogrSostMisuratore.xsd")).newValidator()
    //      //      val flusso2XSDBroad: Validator = factory.newSchema(new StreamSource("src/main/resources/deploy/XSD_EE/FlussiDatiMisuraPrelievoEE-Flusso2-Rettifica.xsd")).newValidator()
    //
    //      getXMLMetadataSMIS(file, matches, params).copy(
    //        flusso1XSDBroad = null,
    //        flusso2XSDBroad = null,
    //        //flussoSMISXSDBroad = flussoSMISXSDBroad,
    //        mapFileNames = null,
    //        mapCodDPRcuPivaUdd = sc.broadcast(Map("" -> Set(""))),
    //        mapPivaRcu = sc.broadcast( Map("12345678901"-> (LocalDateTime.MIN, LocalDateTime.MAX)) )
    //      )
    //    }
    //    )

    import sqlContext.implicits._
    val azienda = sc.parallelize(
      List(
        ("azienda1","IT999E00000003"),
        ("azienda2","IT999E00000003"),
        ("azienda3","IT999E00000003")
      )
    ).toDF(RcuAziendaPSchema.n_id_azienda, RcuAziendaPSchema.t_piva)

    val rcuPod = sc.parallelize(
      List(
        ("IT999E00000003","IT999E00000003"),
        ("IT999E00000011","IT999E00000003"),
        ("rcupod3","IT999E00000003")
      )
    ).toDF(RcuPodPSchema.n_id_pod, RcuPodPSchema.t_codice_pod)

    val rcusPod = sc.parallelize(
      List(
        ("rcuspod1","IT999E00000003"),
        ("rcuspod2","IT999E00000003"),
        ("rcuspod3","IT999E00000003")
      )
    ).toDF(RcusPodPSchema.n_id_pod, RcuPodPSchema.t_codice_pod)

    val rcuPodDistr = sc.parallelize(
      List(
        ("IT999E00000003", "azienda1", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0"),
        ("IT999E00000011", "azienda2", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0"),
        ("rcupod3", "azienda3", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0")
      )
    ).toDF(RcuPodDistrPSchema.n_id_pod, RcuPodDistrPSchema.n_id_distr, RcuPodDistrPSchema.d_inizio, RcuPodDistrPSchema.d_fine)

    val rcusPodDistr = sc.parallelize(
      List(
        ("IT999E00000003", "azienda1", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0"),
        ("IT999E00000011", "azienda2", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0"),
        ("rcupod3", "azienda3", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0")
      )
    ).toDF(RcusPodDistrPSchema.n_id_pod, RcusPodDistrPSchema.n_id_distr, RcusPodDistrPSchema.d_inizio, RcusPodDistrPSchema.d_fine)

    val rcuPodUdd = sc.parallelize(
      List(
        ("IT999E00000003", "azienda1", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0"),
        ("IT999E00000011", "azienda2", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0"),
        ("rcupod3", "azienda3", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0")
      )
    ).toDF(RcuPodUddPSchema.n_id_pod, RcuPodUddPSchema.n_id_udd, RcuPodUddPSchema.d_inizio, RcuPodUddPSchema.d_fine)

    val rcusPodUdd = sc.parallelize(
      List(
        ("IT999E00000003", "azienda1", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0"),
        ("IT999E00000011", "azienda2", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0"),
        ("rcupod3", "azienda3", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0")
      )
    ).toDF(RcusPodUddPSchema.n_id_pod, RcusPodUddPSchema.n_id_udd, RcusPodUddPSchema.d_inizio, RcusPodUddPSchema.d_fine)

    val rcuUddP = sc.parallelize(
      List(
        ("DP0001", "azienda1", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0"),
        ("DP0002", "azienda2", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0"),
        ("DP0003", "azienda3", "2014-08-01 00:00:00.0", "2022-08-01 00:00:00.0")
      )
    ).toDF(RcuUddPSchema.t_codice_terna, RcuUddPSchema.n_id_udd, RcuUddPSchema.d_inizio, RcuUddPSchema.d_fine)


/*    val result = CheckAmmissibilitaPod.recoverRddXmlWithMetaSMIS(fileWithMatches, azienda, rcuPod, rcusPod, rcuPodDistr, rcusPodDistr, rcuPodUdd, rcusPodUdd, rcuUddP)
    //result.collect().foreach(println)

    val fileWithMessages = validate(result, params).persist(StorageLevel.MEMORY_AND_DISK)

    val fileWithErrorMessages = fileWithMessages
      .map({ case (meta, messageList) =>
        val errorMessages = messageList.filter(_.ammissibilita.equalsIgnoreCase(Constants.NO))
        (meta, errorMessages)
      })
      .filter({ case (meta, messageList) => messageList.nonEmpty })
*/

  //  writeAggregatedPodTxtReport(fileWithErrorMessages, "src/test/resources/TMP_1G/isilonshare1G", params)
 /*   val messagesRDD = fileWithMessages.flatMap({ case (fileXmlWithMeta, messages) =>
      messages.map(message => message.copy(flusso = fileXmlWithMeta.flusso, anno = fileXmlWithMeta.params.year, mese = fileXmlWithMeta.params.month, giorno = fileXmlWithMeta.params.day))
    })*/

    //writeOnHive(messagesRDD)

  }
}

object xmlMeta {
  def fileWithMatches(implicit sc: SparkContext, SQLContext: SQLContext): RDD[XMLMetadata] = {
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
/*    val xm1 = CheckAmmissibilitaFile.recoverPodAndDataMisura(XML.loadFile(file), new XMLMetadata(file = file, pivaDistributore = "01234567890", pivaUDD = "12345678901", annoMese = "201811", flusso = "SMIS", timestamp = "20181203101533", progressivo = "1", codDp = "DP0001", sm = null, params = params))
    val xm2 = CheckAmmissibilitaFile.recoverPodAndDataMisura(XML.loadFile(file2), new XMLMetadata(file = file2, pivaDistributore = "01234567890", pivaUDD = "12345678901", annoMese = "201811", flusso = "SMIS", timestamp = "20181205101533", progressivo = "1", codDp = "DP0002", sm = null, params = params))
    val xm3 = CheckAmmissibilitaFile.recoverPodAndDataMisura(XML.loadFile(file3), new XMLMetadata(file = file3, pivaDistributore = "01234567890", pivaUDD = "12345678901", annoMese = "201811", flusso = "SMIS", timestamp = "20181205101533", progressivo = "1", codDp = "DP0003", sm = null, params = params))
    val xm4 = CheckAmmissibilitaFile.recoverPodAndDataMisura(XML.loadFile(file4), new XMLMetadata(file = file4, pivaDistributore = "01234567890", pivaUDD = "12345678901", annoMese = "201910", flusso = "SMIS", timestamp = "20191103101533", progressivo = "1", codDp = "DP0234", sm = null, params = params))
    val xm5 = CheckAmmissibilitaFile.recoverPodAndDataMisura(XML.loadFile(file5), new XMLMetadata(file = file5, pivaDistributore = "01234567890", pivaUDD = "AAA01234567890BB", annoMese = "201910", flusso = "SMIS", timestamp = "20191203101533", progressivo = "1", codDp = "DP1234", sm = null, params = params))


    sc.parallelize(List(xm1, xm2, xm3, xm4, xm5))*/
    null
  }

}
