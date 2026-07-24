package it.au.misure.ingestionMisureGasUnico.flow

import it.au.misure.ingestionMisureGasUnico.flow.IGMGFlow.{getPdrExtraMetadata, loadData}
import it.au.misure.ingestionMisureGasUnico.model.GasXmlMetadata
import it.au.misure.ingestionMisureGasUnico.model.schema.IGMGXMLSchema
import it.au.misure.ingestionMisureGasUnico.model.schema.rcu.{RcuGasPdrSchema, RcuGasPdrStatoSchema}
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import it.au.misure.ingestionMisureGasUnico.utility.{EnvironmentSparkTest, PropertyUtility}
import org.apache.spark.sql.{Row, SaveMode}
import org.junit.Assert

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.xml.XML

class TestIGMGFlow extends EnvironmentSparkTest {
  def testParseXml(): Unit = {
    val tempRootPath = PropertyUtility.getTmpOutputFolder
    val inputPath = s"$tempRootPath/IGMG/IGMG/2020/12/12/01234567890_12345678901_202012_IGMG_20201212091234_1.xml"
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
      , flusso = "IGMG"
      , timestamp = "20201212091234"
      , progressivo = "1"
      , tS = ""
      , ammissibilita = Map("12345600002233" -> "OK")
    )

    val expectedRows = List(Row(
      "IGMG"
      , "12345678901"
      , "01234567890"
      , "12345600002233"
      , "1"
      , "1"
      , "12/12/2020"
      , null
      , "1234aaabbb1234"
      , "SI"
      , "1223wwwwwd123456cccc"
      , "1"
      , "NO"
      , "000000000000123"
      , "000000000000133"
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
      , "000000000000001"
      , "000000000000001"
      , null
      , "OK"
      , s"${PropertyUtility.getUnzipInputPath}/TMG_01234567890/DISTRIBUTORE/TMG_01234567890_12345678901/2020/1212/01234567890_12345678901_202012_IGMG_20201212091234_1.zip"
      , "2020"
      , "2020"
      , "12"
      , "12"
      , "12"
      , "01234567890_12345678901_202012_IGMG_20201212091234_1.zip"
    ))

    Assert.assertEquals(expectedRows, IGMGFlow.parseXml(inputMetadata))
  }

  def testParseXmlWithCausaOstativa(): Unit = {
    val tempRootPath = PropertyUtility.getTmpOutputFolder
    val inputPath = s"$tempRootPath/IGMG/IGMG/2020/12/12/01234567890_12345678901_202012_IGMG_20201212091234_2.xml"
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
      , flusso = "IGMG"
      , timestamp = "20201212091234"
      , progressivo = "1"
      , tS = ""
      , ammissibilita = Map("12345600002233" -> "OK")
    )

    val expectedRows = List(Row(
      "IGMG"
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
      , s"${PropertyUtility.getUnzipInputPath}/TMG_01234567890/DISTRIBUTORE/TMG_01234567890_12345678901/2020/1212/01234567890_12345678901_202012_IGMG_20201212091234_1.zip"
      , "2020"
      , "2020"
      , "12"
      , "12"
      , "12"
      , "01234567890_12345678901_202012_IGMG_20201212091234_1.zip"
    ))

    Assert.assertEquals(expectedRows, IGMGFlow.parseXml(inputMetadata))
  }

 /* private def createDbAndTables(): Unit = {
    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._

    val rcugasPdrLocation = "src/test/resources/hive/rcugas_pdr_p"
    val rcugasPdrStatoLocation = "src/test/resources/hive/rcugas_pdr_stato_p"
    val rcugasDb = PropertyUtility.config.getString("hive.db.rcugas")

    Environment.getSpark.sql(s"DROP DATABASE IF EXISTS $rcugasDb CASCADE")
    Environment.getSpark.sql(s"CREATE DATABASE IF NOT EXISTS $rcugasDb")
    Environment.getSpark.sql(s"CREATE EXTERNAL TABLE ${PropertyUtility.getRcugasPdrTable} ( n_id_pdr string, t_codice_pdr string ) STORED AS PARQUET LOCATION '$rcugasPdrLocation'")
    Environment.getSpark.sql(s"CREATE EXTERNAL TABLE ${PropertyUtility.getRcugasPdrStatoTable} ( n_id_pdr string, t_cod_stato_pdr string, d_data_inizio string, d_data_fine string) STORED AS PARQUET LOCATION '$rcugasPdrStatoLocation'")

    val rcugasPdr = Environment.getSpark.sparkContext.parallelize(Seq(
      ("idPdr1", "12345600002233"),
      ("idPdr2", "12345600002200"),
      ("idPdr3", "12345600002299")
    )).toDF(RcuGasPdrSchema.n_id_pdr, RcuGasPdrSchema.t_codice_pdr)
    rcugasPdr.write.mode(SaveMode.Overwrite).insertInto(PropertyUtility.getRcugasPdrTable)

    val rcugasPdrStato = Environment.getSpark.sparkContext.parallelize(Seq(
      ("idPdr1", "P", "2021-01-01 00:00:00", "2021-02-01 00:00:00"),
      ("idPdr1", "D", "2021-01-01 00:00:00", "2021-02-01 00:00:00"),
      ("idPdr2", "P", "2020-12-01 00:00:00", "2021-01-01 00:00:00"),
      ("idPdr3", "D", "2020-12-01 00:00:00", "2021-01-01 00:00:00")
    )).toDF(RcuGasPdrStatoSchema.n_id_pdr, RcuGasPdrStatoSchema.t_cod_stato_pdr, RcuGasPdrStatoSchema.d_data_inizio, RcuGasPdrStatoSchema.d_data_fine)
    rcugasPdrStato.write.mode(SaveMode.Overwrite).insertInto(PropertyUtility.getRcugasPdrStatoTable)
  }

  def testGetPdrExtraMetadata(): Unit = {
    createDbAndTables()

    val inputRdd = loadData()

    val pdrWithMetaRdd = inputRdd.flatMap(gasXmlMetada => {
      (gasXmlMetada.xmlNode \\ IGMGXMLSchema.FlussoIGMG \\ IGMGXMLSchema.DatiPdR).toList
        .map(datiPdr => (datiPdr, gasXmlMetada))
    })

    val pdrWithExtraMetaRdd = getPdrExtraMetadata(pdrWithMetaRdd)

    val result = pdrWithExtraMetaRdd.map(x => ((x._1 \\ IGMGXMLSchema.cod_PdR).text, x._2.pdrRcuExist, x._2.pdrValidFrom, x._2.pdrValidTo))

    Assert.assertEquals((LocalDateTime.MAX, LocalDateTime.MAX), result.filter(_._1 == "12345600002233").map(x => (x._3, x._4)).collect.head)
    Assert.assertEquals((LocalDateTime.parse("2020-12-01 00:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), LocalDateTime.parse("2021-01-01 00:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))), result.filter(_._1 == "12345600002200").map(x => (x._3, x._4)).collect.head)
    Assert.assertEquals((LocalDateTime.MAX, LocalDateTime.MAX), result.filter(_._1 == "12345600002299").map(x => (x._3, x._4)).collect.head)
    Assert.assertEquals((LocalDateTime.MAX, LocalDateTime.MAX), result.filter(_._1 == "12345600002266").map(x => (x._3, x._4)).collect.head)
  }
  */
}
