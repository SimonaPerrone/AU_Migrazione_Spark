package it.sferanet.au.controller.dal.rettifica

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.rettifica.RslTable
import it.sferanet.au.model.rettifica.Rsl
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class RslTableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()


  def checkOldRouteObj(obj: Rsl): Unit = {
    Assert.assertTrue(!obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "RSL") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-01-19T20:10:47.235759")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == (-1)) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == 25175) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "00594200434084") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare1/GAS_INJ_RECUPERO/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2019/0108/05608890488_06655971007_201812_RSL0400_20190108101500_1.xml") //local_file
    Assert.assertTrue(obj.motivation.get == 1) //mottRettLett motivazione
    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "0052171876") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("01/07/2008")) == 0) //data_comp
    Assert.assertTrue(obj.collected.getOrElse("") == "") //raccolta solo nel tracciato std
  }

  def checkNewRouteObj(obj: Rsl): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "RSL") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-03-31T03:27:36.116000")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == (-1)) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == 30889) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "07780000005000") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare_gas/TMG_00825150709/DISTRIBUTORE/TMG_00825150709_11744581007/2021/0330/00825150709_11744581007_202102_RSL_20210330200838_42_R.zip") //local_file
    Assert.assertTrue(obj.motivation.get == 1) //mottRettLett motivazione
    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "4327576") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("01/02/2021")) == 0) //data_comp
    Assert.assertTrue(obj.collected.getOrElse("") == "S") //raccolta solo nel tracciato std
  }


  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = RslTable("./src/test/resources/integrazione-ca/misure/rsl/old_route").get()
    val obj: Rsl = measures.take(1)(0)
    checkOldRouteObj(obj)
  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = RslTable("./src/test/resources/integrazione-ca/misure/rsl/new_route").get()
    val obj: Rsl = measures.take(1)(0)
    checkNewRouteObj(obj)

  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = RslTable("./src/test/resources/integrazione-ca/misure/rsl/both_route").get()
    val obj = measures.take(2)
    val obj_n: Rsl = obj.filter(_.isNewRoute == true)(0)
    val obj_o: Rsl = obj.filter(_.isNewRoute == false)(0)

    checkOldRouteObj(obj_o)
    checkNewRouteObj(obj_n)

  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3, rdd4 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rsl(motivation = Some(1),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result1 = new RslTable(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rsl(motivation = Some(5),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result2 = new RslTable(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rsl(motivation = Some(0),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result3 = new RslTable(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())

    val rdd4 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rsl(motivation = Some(6),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result4 = new RslTable(null).filterValidRows(rdd4)
    Assert.assertEquals(0, result4.count())
  }

}
