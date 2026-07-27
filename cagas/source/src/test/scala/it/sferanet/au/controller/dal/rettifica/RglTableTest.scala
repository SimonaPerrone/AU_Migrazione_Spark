package it.sferanet.au.controller.dal.rettifica

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.rettifica.{R40rTable, RglTable}
import it.sferanet.au.model.rettifica.{R40r, Rgl}
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class RglTableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()


  def checkOldRouteObj(obj: Rgl): Unit = {
    Assert.assertTrue(!obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "RGL") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-01-13T13:01:30.130597")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == 863729) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == 838932) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "11750001080026") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare1/GAS_INJ/TMG_01273110690/DISTRIBUTORE/TMG_01273110690_01760860161/2021/0112/01273110690_01760860161_201305_RGL0055_20201231150324_1.XML") //local_file
    Assert.assertTrue(obj.motivation.get == 3) //mottRettLett motivazione
    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "11110203") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "65276") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.getFormatter("yyyy-MM-dd").parse("2013-05-27 00:00:00")) == 0) //data_comp
    Assert.assertTrue(obj.collected.getOrElse("") == "") //raccolta solo nel tracciato std
  }

  def checkNewRouteObj(obj: Rgl): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "RGL") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-04-02T03:22:05.958000")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == (-1)) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == (-1)) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "03050000152313") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare_gas/TMG_02166820510/DISTRIBUTORE/TMG_02166820510_01016870329/2021/0401/02166820510_01016870329_202001_RGL_20210401163920_6_R.zip") //local_file
    Assert.assertTrue(obj.motivation.get == 6) //mottRettLett motivazione
    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("16/01/2020")) == 0) //data_comp
    Assert.assertTrue(obj.collected.getOrElse("") == "AC") //raccolta solo nel tracciato std
  }


  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = RglTable("./src/test/resources/integrazione-ca/misure/rgl/old_route").get()
    val obj: Rgl = measures.take(1)(0)
    checkOldRouteObj(obj)
  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = RglTable("./src/test/resources/integrazione-ca/misure/rgl/new_route").get()
    val obj: Rgl = measures.take(1)(0)
    checkNewRouteObj(obj)

  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = RglTable("./src/test/resources/integrazione-ca/misure/rgl/both_route").get()
    val obj = measures.take(2)
    val obj_n: Rgl = obj.filter(_.isNewRoute == true)(0)
    val obj_o: Rgl = obj.filter(_.isNewRoute == false)(0)

    checkOldRouteObj(obj_o)
    checkNewRouteObj(obj_n)

  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2, rdd3 OK
    // rdd4, rdd5 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rgl(motivation = Some(1), ammissibilita = None,
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result1 = new RglTable(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rgl(motivation = Some(6), ammissibilita = None,
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result2 = new RglTable(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rgl(motivation = Some(7), ammissibilita = Some("OK"),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result3 = new RglTable(null).filterValidRows(rdd3)
    Assert.assertEquals(1, result3.count())

    val rdd4 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rgl(motivation = Some(7),ammissibilita = None,
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result4 = new RglTable(null).filterValidRows(rdd4)
    Assert.assertEquals(0, result4.count())

    val rdd5 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rgl(motivation = Some(8), ammissibilita = Some("OK"),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result5 = new RglTable(null).filterValidRows(rdd5)
    Assert.assertEquals(0, result5.count())
  }

}
