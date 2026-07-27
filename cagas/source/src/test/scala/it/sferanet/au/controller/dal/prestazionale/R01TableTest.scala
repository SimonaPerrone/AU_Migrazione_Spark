package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.{M01Table, R01Table}
import it.sferanet.au.model.prestazionale.{M01, R01}
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class R01TableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()


  def checkOldRouteObj(obj: R01): Unit = {
    Assert.assertTrue(!obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "R01") //flusso
    //    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2020-12-31T04:04:05.302968"))==0) //d_caricamento
    Assert.assertTrue(obj.converted.isEmpty) //segn_conv
    Assert.assertTrue(obj.measure.isEmpty) // segn_mis
    Assert.assertTrue(obj.pdr == "10930000016834") //cod_pdr
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare1/GAS_INJ/TMG_03003150160/DISTRIBUTORE/TMG_03003150160_00273990168/2020/1217/03003150160_00273990168_201903_R01.0150_20201209104634_180.xml") //local_file
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.isEmpty) //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("14/03/2019")) == 0) //data_comp
    Assert.assertTrue(obj.outcome.get == 0) //esito

  }

  def checkNewRouteObj(obj: R01): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "R01") //flusso
    //    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-03-26T03:32:32.471000"))==0) //d_caricamento
    Assert.assertTrue(obj.converted.isEmpty) //letTotConv converted measure
    Assert.assertTrue(obj.measure.get == 0.0) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "11420000001608") //cod_pdr
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_04015810874/DISTRIBUTORE/TMG_04015810874_04121780870/2021/0202/04015810874_04121780870_200101_R01_20210202110911_4767_M.zip") //local_file

    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.get == "53662252") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("15/01/2001")) == 0) //data_pres
    Assert.assertTrue(obj.outcome.isEmpty) //esito
    Assert.assertTrue(obj.readType.get == 'E') //tipo_lettura
    Assert.assertTrue(obj.collected.get == "T") //raccolta
  }


  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = R01Table("./src/test/resources/integrazione-ca/misure/r01/old_route").get()
    val obj: R01 = measures.take(1)(0)
    checkOldRouteObj(obj)
  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = R01Table("./src/test/resources/integrazione-ca/misure/r01/new_route").get()
    val obj: R01 = measures.take(1)(0)
    checkNewRouteObj(obj)

  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = R01Table("./src/test/resources/integrazione-ca/misure/r01/both_route").get()
    val obj = measures.take(2)
    val obj_n: R01 = obj.filter(_.isNewRoute == true)(0)
    val obj_o: R01 = obj.filter(_.isNewRoute == false)(0)

    checkOldRouteObj(obj_o)
    checkNewRouteObj(obj_n)

  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2, rdd3 OK
    // rdd4 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      R01(ammissibilita = None, readType = None, outcome = Some('1'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result1 = new R01Table(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      R01(ammissibilita = Some("OK"), readType = Some('E'), outcome = None,
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result2 = new R01Table(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      R01(ammissibilita = Some("OK"), readType = Some('A'), outcome = None,
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result3 = new R01Table(null).filterValidRows(rdd3)
    Assert.assertEquals(1, result3.count())

    val rdd4 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      R01(ammissibilita = Some("OK"), readType = Some('S'), outcome = None,
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result4 = new R01Table(null).filterValidRows(rdd4)
    Assert.assertEquals(0, result4.count())

    val rdd5 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      R01(ammissibilita = None, readType = None, outcome = Some('0'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result5 = new R01Table(null).filterValidRows(rdd5)
    Assert.assertEquals(0, result5.count())
  }

}
