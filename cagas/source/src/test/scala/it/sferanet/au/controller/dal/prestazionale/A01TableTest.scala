package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.A01Table
import it.sferanet.au.model.prestazionale.A01
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

import java.text.SimpleDateFormat

class A01TableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()


  def checkOldRouteObj(obj: A01): Unit = {
    Assert.assertTrue(!obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "A01") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2020-12-31T04:04:05.302968")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == (-1)) //segn_conv
    Assert.assertTrue(obj.measure.getOrElse(-1) == 2597) // segn_mis
    Assert.assertTrue(obj.pdr == "08200000026362") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare1/GAS_INJ/TMG_02462970415/DISTRIBUTORE/TMG_02462970415_02089000422/2020/1230/02462970415_02089000422_199901_A010150_20201229163342_1.xml") //local_file

    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "4251464") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("05/01/1999")) == 0) //data_comp
    Assert.assertTrue(obj.outcome.getOrElse('^') == '1') //esito

  }

  def checkNewRouteObj(obj: A01): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "A01") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-03-26T03:32:32.471000")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == (-1)) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == 1289) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "10400000116081") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare_gas/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_01812630224/2021/0325/06724610966_01812630224_202103_A01_20210325044608_00_M.zip") //local_file

    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "MTSB035602152632") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("24/03/2021")) == 0) //data_pres
    Assert.assertTrue(obj.outcome.isEmpty) //esito
    Assert.assertTrue(obj.readType.getOrElse("") == 'E') //tipo_lettura
    Assert.assertTrue(obj.collected.getOrElse("") == "T") //raccolta
  }


  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = A01Table("./src/test/resources/integrazione-ca/misure/a01/old_route").get()
    val obj: A01 = measures.take(1)(0)
    checkOldRouteObj(obj)
  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = A01Table("./src/test/resources/integrazione-ca/misure/a01/new_route").get()
    val obj: A01 = measures.take(1)(0)
    checkNewRouteObj(obj)

  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = A01Table("./src/test/resources/integrazione-ca/misure/a01/both_route").get()
    val obj = measures.take(2)
    val obj_n: A01 = obj.filter(_.isNewRoute == true)(0)
    val obj_o: A01 = obj.filter(_.isNewRoute == false)(0)

    checkOldRouteObj(obj_o)
    checkNewRouteObj(obj_n)

  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2, rdd3 OK
    // rdd4, rdd5 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      A01(ammissibilita = None, outcome = Some('1'), readType = None,
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null
        , measure = null, converted = null, serialNumberMis = null, serialNumberConv = null,
        collected = null, local_file = null, d_caricamento = null, isNewRoute = true)
    )
    val result1 = new A01Table(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      A01(ammissibilita = Some("OK"), outcome = None, readType = Some('E'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null
        , measure = null, converted = null, serialNumberMis = null, serialNumberConv = null,
        collected = null, local_file = null, d_caricamento = null, isNewRoute = true)
    )
    val result2 = new A01Table(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      A01(ammissibilita = Some("OK"), outcome = None, readType = Some('A'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null
        , measure = null, converted = null, serialNumberMis = null, serialNumberConv = null,
        collected = null, local_file = null, d_caricamento = null, isNewRoute = true)
    )
    val result3 = new A01Table(null).filterValidRows(rdd3)
    Assert.assertEquals(1, result3.count())

    val rdd4 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      A01(ammissibilita = None, outcome = Some('2'), readType = None,
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null
        , measure = null, converted = null, serialNumberMis = null, serialNumberConv = null,
        collected = null, local_file = null, d_caricamento = null, isNewRoute = true)
    )
    val result4 = new A01Table(null).filterValidRows(rdd4)
    Assert.assertEquals(0, result4.count())

    val rdd5 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      A01(ammissibilita = Some("OK"), outcome = None, readType = Some('S'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null
        , measure = null, converted = null, serialNumberMis = null, serialNumberConv = null,
        collected = null, local_file = null, d_caricamento = null, isNewRoute = true)
    )
    val result5 = new A01Table(null).filterValidRows(rdd5)
    Assert.assertEquals(0, result5.count())

  }
}
