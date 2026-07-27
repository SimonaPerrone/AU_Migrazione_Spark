package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.V01Table
import it.sferanet.au.model.prestazionale.V01
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class V01TableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()


  def checkNewRouteObj(obj: V01): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "V01") //flusso
    //    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-03-26T03:32:32.471000"))==0) //d_caricamento
    Assert.assertTrue(obj.converted.isEmpty) //letTotConv converted measure
    Assert.assertTrue(obj.measure.get == 29066.0) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "03270027702390") //cod_pdr
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2021/0414/01791490343_01178580997_202104_V01_20210413200152_1_M.zip") //local_file
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.get == "820372") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("11/04/2021")) == 0) //data_pres

    Assert.assertTrue(obj.readType.get == 'E') //tipo_lettura
    Assert.assertTrue(obj.collected.get == "T") //raccolta
  }

  def checkNewRouteObj2(obj: V01): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "V01") //flusso
    //    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-03-26T03:32:32.471000"))==0) //d_caricamento
    Assert.assertTrue(obj.converted.isEmpty) //letTotConv converted measure
    Assert.assertTrue(obj.measure.get == 3294.0) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "03270029619511") //cod_pdr
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_06655971007/2021/0402/01791490343_06655971007_202103_V01_20210401200024_1_M.zip") //local_file
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.get == "MTSB033600694566") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("30/03/2021")) == 0) //data_pres

    Assert.assertTrue(obj.readType.get == 'E') //tipo_lettura
    Assert.assertTrue(obj.collected.get == "T") //raccolta
  }

  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = V01Table("./src/test/resources/integrazione-ca/misure/v01/old_route").get()
    Assert.assertTrue(measures.count() == 0)
    //    15690019093460
  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = V01Table("./src/test/resources/integrazione-ca/misure/v01/new_route").get()
    val obj1: V01 = measures.filter(r => r.pdr == "03270027702390").take(1)(0)
    val obj2: V01 = measures.filter(r => r.pdr == "03270029619511").take(1)(0)
    checkNewRouteObj(obj1)
    checkNewRouteObj2(obj2)

  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = V01Table("./src/test/resources/integrazione-ca/misure/v01/both_route").get()
    val obj = measures.take(1)(0)

    Assert.assertTrue(measures.count() == 1)
    checkNewRouteObj(obj)

  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      V01(readType = Some('E'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result1 = new V01Table(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      V01(readType = Some('A'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result2 = new V01Table(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      V01(readType = Some('S'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result3 = new V01Table(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())
  }

}
