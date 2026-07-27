package it.sferanet.au.controller.dal.rettifica

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.rettifica.M01rTable
import it.sferanet.au.model.rettifica.M01r
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class M01RTableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()


  def checkNewRouteObj(obj: M01r): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "M01R") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-03-04T04:09:21.305000")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == (-1)) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == 7263) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "11690000005341") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare_gas/TMG_01501460438/DISTRIBUTORE/TMG_01501460438_02078510423/2021/0303/01501460438_02078510423_201906_M01R_20210303150354_6_R.zip") //local_file
    Assert.assertTrue(obj.motivation.get == 2) //mottRettLett motivazione
    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "30032618") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("04/06/2019")) == 0) //data_comp
    Assert.assertTrue(obj.collected.getOrElse("") == "T") //raccolta
  }


  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = M01rTable("./src/test/resources/integrazione-ca/misure/m01r").get()
    val obj: M01r = measures.take(1)(0)
    Assert.assertTrue(measures.filter(p => p.isNewRoute == false).count() == 0)
    checkNewRouteObj(obj)

  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3, rdd4 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      M01r(motivation = Some(1),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result1 = new M01rTable(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      M01r(motivation = Some(5),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result2 = new M01rTable(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      M01r(motivation = Some(0),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result3 = new M01rTable(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())

    val rdd4 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      M01r(motivation = Some(6),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result4 = new M01rTable(null).filterValidRows(rdd4)
    Assert.assertEquals(0, result4.count())
  }
}
