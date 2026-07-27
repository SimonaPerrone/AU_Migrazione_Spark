package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.Sm1Table
import it.sferanet.au.model.prestazionale.Sm1
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

import java.text.SimpleDateFormat

class Sm1TableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy")
  val dateFormat2: SimpleDateFormat = Constants.FORMAT_DATE_LOAD

  def chekNew(obj: Sm1): Unit = {
    Assert.assertTrue(obj.service == "SM1")
    Assert.assertTrue(obj.pdr == "00882105631622") //cod_prd
    Assert.assertTrue(dateFormat.format(obj.date.get) == "02/04/2021") //data
    Assert.assertTrue(obj.outcome.isEmpty) //esitoVal
    Assert.assertTrue(obj.measure.get == 53.0) //letTotPrel
    Assert.assertTrue(obj.converted.isEmpty) //letTotConv
    Assert.assertTrue(obj.serialNumberMis.get == "MTSB034803938071") //matricola misuratore
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matricola convertitore
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_03707740233/2021/0402/05608890488_03707740233_202104_SM1_20210402210002_2_M.zip")
    //fixme
    //    Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2021-03-06T06:54:03.289817")
    Assert.assertTrue(obj.isNewRoute)

  }

  def checkOld(obj: Sm1): Unit = {
    Assert.assertTrue(obj.service == "SM1")
    Assert.assertTrue(obj.pdr == "03050000064553") //cod_prd
    Assert.assertTrue(dateFormat.format(obj.date.get) == "23/09/2019") //data
    Assert.assertTrue(obj.outcome.isEmpty) //esitoVal
    Assert.assertTrue(obj.measure.get == 1425.0) //letTotPrel
    Assert.assertTrue(obj.converted.isEmpty) //letTotConv
    Assert.assertTrue(obj.serialNumberMis.get == "SMGR034017085042") //matricola misuratore
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matricola convertitore
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare1/GAS_INJ/TMG_02166820510/DISTRIBUTORE/TMG_02166820510_02968430237/2020/0528/02166820510_02968430237_SM10150_20200528173016_1.xml")
    //    fixme
    //    Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2020-10-29T19:37:48.877351")
    Assert.assertTrue(obj.readType.get == 'E')
    Assert.assertTrue(!obj.isNewRoute)
  }

  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = Sm1Table("./src/test/resources/integrazione-ca/misure/sm1/old_route").get()
    val obj: Sm1 = measures.filter(obj => obj.pdr == "03050000064553").take(1)(0)
    checkOld(obj)


  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = Sm1Table("./src/test/resources/integrazione-ca/misure/sm1/new_route").get()
    val obj: Sm1 = measures.take(1)(0)
    chekNew(obj)


  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = Sm1Table("./src/test/resources/integrazione-ca/misure/sm1/both_route").get()
    val newT: Sm1 = measures.filter(rmv => rmv.pdr == "00882105631622").take(1)(0)
    val old: Sm1 = measures.filter(rmv => rmv.pdr == "03050000064553").take(1)(0)
    checkOld(old)
    chekNew(newT)
  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2, rdd3 OK
    // rdd4, rdd5, rdd6 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Sm1(ammissibilita = None, outcome = Some('1'), readType = Some('E'),
         service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result1 = new Sm1Table(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Sm1(ammissibilita = Some("OK"), outcome = None, readType = Some('E'),
         service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result2 = new Sm1Table(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Sm1(ammissibilita = Some("OK"), outcome = None, readType = Some('A'),
         service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result3 = new Sm1Table(null).filterValidRows(rdd3)
    Assert.assertEquals(1, result3.count())

    val rdd4 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Sm1(ammissibilita = Some("OK"), outcome = None, readType = Some('S'),
         service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result4 = new Sm1Table(null).filterValidRows(rdd4)
    Assert.assertEquals(0, result4.count())

    val rdd5 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Sm1(ammissibilita = None, outcome = Some('1'), readType = Some('A'),
         service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result5 = new Sm1Table(null).filterValidRows(rdd5)
    Assert.assertEquals(0, result5.count())

    val rdd6 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Sm1(ammissibilita = None, outcome = Some('0'), readType = Some('E'),
         service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result6 = new Sm1Table(null).filterValidRows(rdd5)
    Assert.assertEquals(0, result6.count())
  }
}

