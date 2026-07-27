package it.sferanet.au.controller.dal.rettifica

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.rettifica.RmvTable
import it.sferanet.au.model.rettifica.Rmv
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class RmvTableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()


  def checkOldRouteObj(obj: Rmv): Unit = {
    Assert.assertTrue(!obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "RMV") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2020-07-08T08:04:43.203058")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == 0) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == 10142.0) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "15330000042252") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare1/GAS_INJ/TMG_04152790962/DISTRIBUTORE/TMG_04152790962_06655971007/2020/0707/04152790962_06655971007_202003_RMV0400_20200707161047_11.xml") //local_file
    Assert.assertTrue(obj.motivation.get == 1) //mottRettLett motivazione
    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "6190319") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("10/03/2020")) == 0) //data_comp
    Assert.assertTrue(obj.collected.getOrElse("") == "") //raccolta solo nel tracciato std
  }

  def checkNewRouteObj(obj: Rmv): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "RMV") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-03-20T03:27:45.195000")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == (-1)) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == (-1)) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "00600036001437") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare_gas/TMG_00850720194/DISTRIBUTORE/TMG_00850720194_01262680190/2021/0319/00850720194_01262680190_202103_RMV_20210319163845_3_R.zip") //local_file
    Assert.assertTrue(obj.motivation.get == 6) //mottRettLett motivazione
    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "52279655") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("04/03/2021")) == 0) //data_comp
    Assert.assertTrue(obj.collected.getOrElse("") == "V") //raccolta solo nel tracciato std
  }


  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = RmvTable("./src/test/resources/integrazione-ca/misure/rmv/old_route").get()
    val obj: Rmv = measures.take(1)(0)
    checkOldRouteObj(obj)
  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = RmvTable("./src/test/resources/integrazione-ca/misure/rmv/new_route").get()
    val obj: Rmv = measures.take(1)(0)
    checkNewRouteObj(obj)

  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = RmvTable("./src/test/resources/integrazione-ca/misure/rmv/both_route").get()
    val obj = measures.take(2)
    val obj_n: Rmv = obj.filter(_.isNewRoute == true)(0)
    val obj_o: Rmv = obj.filter(_.isNewRoute == false)(0)

    checkOldRouteObj(obj_o)
    checkNewRouteObj(obj_n)

  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3, rdd4 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rmv(motivation = Some(1),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result1 = new RmvTable(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rmv(motivation = Some(5),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result2 = new RmvTable(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rmv(motivation = Some(0),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result3 = new RmvTable(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())

    val rdd4 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rmv(motivation = Some(6),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result4 = new RmvTable(null).filterValidRows(rdd4)
    Assert.assertEquals(0, result4.count())
  }

}
