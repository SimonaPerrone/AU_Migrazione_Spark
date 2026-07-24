package it.au.misure.ingestionMisureGasUnico.flow

import it.au.misure.ingestionMisureGasUnico.flow.IGMRFlow.{getPdrExtraMetadata, loadData}
import it.au.misure.ingestionMisureGasUnico.model.GasXmlMetadata
import it.au.misure.ingestionMisureGasUnico.model.schema.IGMRXMLSchema._
import it.au.misure.ingestionMisureGasUnico.model.schema.{IGMGSchema, IGMRXMLSchema}
import it.au.misure.ingestionMisureGasUnico.model.schema.rcu.{RcuGasPdrSchema, RcuGasPdrStatoSchema}
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import it.au.misure.ingestionMisureGasUnico.utility.{EnvironmentSparkTest, PropertyUtility}
import org.apache.spark.sql.Row
import org.junit.Assert

import java.io.File
import scala.xml.XML

class TestIGMRFlow extends EnvironmentSparkTest {

  def testParseXml(): Unit = {
    val tempRootPath = PropertyUtility.getTmpOutputFolder
    val inputPath = s"$tempRootPath/IGMR/IGMR/2020/12/12/01234567890_12345678901_202012_IGMR_20201212091234_2.xml"
    val inputMetadata = GasXmlMetadata(
      xmlNode = XML.load(inputPath)
      , file = new File(inputPath)
      , pivaDistributore = "01234567890"
      , pivaUtente = "12345678901"
      , anno = "2020"
      , annoRiferimento = "2020"
      , mese = "12"
      , meseRiferimento = "12"
      , giorno = "12"
      , flusso = "IGMR"
      , timestamp = "20201212091234"
      , progressivo = "1"
      , tS = ""
      , ammissibilita = Map("12345600002233" -> "OK")
    )

    val expectedRows = List(Row(
      "IGMR"
      , "12345678901"
      , "12345678901"
      , "12345600002233"
      , "1"
      , "1"
      , "12/12/2020"
      , "NO"
      , "2"
      , "1234aaabbb123456cccc"
      , "SI"
      , ""
      , "2"
      , null
      , "123456789"
      , null
      , "NO"
      , null
      , "qwe12344445556667788"
      , "G4"
      , "02"
      , "SI"
      , "NO"
      , null
      , null
      , null
      , null
      , "1"
      , null
      , "3"
      , "12"
      , "2017"
      , "07/08/2019"
      , null
      , "234222222"
      , "233456211"
      , null
      , "OK"
      , s"${PropertyUtility.getUnzipInputPath}/TMG_01234567890/DISTRIBUTORE/TMG_01234567890_12345678901/2020/1212/01234567890_12345678901_202012_IGMR_20201212091234_1.zip"
      , "2020"
      , "2020"
      , "12"
      , "12"
      , "12"
      , "01234567890_12345678901_202012_IGMR_20201212091234_1.zip"
    ))

    Assert.assertEquals(expectedRows, IGMRFlow.parseXml(inputMetadata))
  }
/*
  def testParseXmlWithCausaOstativa(): Unit = {
    val tempRootPath = PropertyUtility.getTmpOutputFolder
    val inputPath = s"$tempRootPath/IGMR/IGMR/2020/12/12/01234567890_12345678901_202012_IGMR_20201212091234_2.xml"
    val inputMetadata = GasXmlMetadata(
      xmlNode = XML.load(inputPath)
      , file = new File(inputPath)
      , pivaDistributore = "01234567890"
      , pivaUtente = "12345678901"
      , anno = "2020"
      , annoRiferimento = "2020"
      , mese = "12"
      , meseRiferimento = "12"
      , giorno = "12"
      , flusso = "IGMR"
      , timestamp = "20201212091234"
      , progressivo = "1"
      , tS = ""
      , ammissibilita = Map("12345600002233" -> "OK")
    )

    val expectedRows = List(Row(
      "IGMR"
      , "12345678901"
      , "01234567890"
      , "12345600002233"
      , "1"
      , "1"
      , "12/12/2020"
      , "SI"
      , "1234aaabbb1234"
      , "SI"
      , "1223wwwwwd123456cccc"
      , "1"
      , "NO"
      , "000000123"
      , "000000133"
      , "E"
      , "SI"
      , "2"
      , "qwe12344445556667788"
      , "G4"
      , "02"
      , "SI"
      , "NO"
      , null
      , null
      , null
      , null
      , "1"
      , null
      , "3"
      , "12"
      , "2017"
      , "07/08/2019"
      , null
      , "000000001"
      , "000000001"
      , null
      , "OK"
      , s"${PropertyUtility.getUnzipInputPath}/TMG_01234567890/DISTRIBUTORE/TMG_01234567890_12345678901/2020/1212/01234567890_12345678901_202012_IGMR_20201212091234_1.zip"
      , "2020"
      , "2020"
      , "12"
      , "12"
      , "12"
      , "01234567890_12345678901_202012_IGMR_20201212091234_1.zip"
    ))

    Assert.assertEquals(expectedRows, IGMRFlow.parseXml(inputMetadata))
  }
*/

  def testGetPdrExtraMetadata(): Unit = {
    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._

    val prtIgmg = Environment.getSpark.sparkContext.parallelize(Seq(
      ("12345600002233", "01234567890", "12345678901", "12/12/2020", "OK")
    ))
      .toDF(IGMGSchema.cod_pdr, IGMGSchema.piva_distr, IGMGSchema.piva_utente, IGMGSchema.data_misura, IGMGSchema.ammissibilita)

    val rcugasPdr = Environment.getSpark.sparkContext.parallelize(Seq(
      ("idPdr1", "12345600002233"),
      ("idPdr2", "12345600002200"),
      ("idPdr3", "12345600002299")
    )).toDF(RcuGasPdrSchema.n_id_pdr, RcuGasPdrSchema.t_codice_pdr)

    val rcugasPdrStato = Environment.getSpark.sparkContext.parallelize(Seq(
      ("idPdr1", "P", "2020-12-01 00:00:00", "2021-02-01 00:00:00"),
      ("idPdr2", "D", "2020-12-01 00:00:00", "2021-01-01 00:00:00"),
      ("idPdr2", "P", "2020-12-01 00:00:00", "2021-01-01 00:00:00"),
      ("idPdr3", "D", "2020-12-01 00:00:00", "2021-01-01 00:00:00")
    )).toDF(RcuGasPdrStatoSchema.n_id_pdr, RcuGasPdrStatoSchema.t_cod_stato_pdr, RcuGasPdrStatoSchema.d_data_inizio, RcuGasPdrStatoSchema.d_data_fine)

    val inputRdd = loadData()

    val pdrWithMetaRdd = inputRdd.flatMap(gasXmlMetada => {
      (gasXmlMetada.xmlNode \\ IGMRXMLSchema.FlussoIGMR \\ IGMRXMLSchema.DatiPdR).toList
        .map(datiPdr => (datiPdr, gasXmlMetada))
    })

    val pdrWithExtraMetaRdd = getPdrExtraMetadata(pdrWithMetaRdd, prtIgmg, rcugasPdr, rcugasPdrStato)

    pdrWithExtraMetaRdd.map(f=>f._2).take(10).foreach(println)

    Assert.assertTrue(pdrWithExtraMetaRdd.map(f=>f._2.pdrRcuExist).first)
    Assert.assertEquals("2020-12-01T00:00", pdrWithExtraMetaRdd.map(f=>f._2.pdrValidFrom.toString).first)
    Assert.assertEquals("2021-02-01T00:00", pdrWithExtraMetaRdd.map(f=>f._2.pdrValidTo.toString).first)
    Assert.assertEquals("RELATIVO_IGMG_PRESENTE", pdrWithExtraMetaRdd.map(f=>f._2.igmgMatch).first)

    }


  }
