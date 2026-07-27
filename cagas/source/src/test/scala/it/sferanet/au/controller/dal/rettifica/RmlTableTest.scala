package it.sferanet.au.controller.dal.rettifica

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.rettifica.RmlTable
import it.sferanet.au.model.rettifica.Rml
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class RmlTableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  def checkOldRouteObj(obj: Rml): Unit = {
    Assert.assertTrue(!obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "RML") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-01-19T19:01:51.411657")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == (-1)) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == 16269) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "03270033216381") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare1/GAS_INJ_RECUPERO/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2018/0906/01791490343_01178580997_200102_RML0055_20180906010000_1.XML") //local_file
    Assert.assertTrue(obj.motivation.get == 2) //mottRettLett motivazione
    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "50788949") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("09/02/2001")) == 0) //data_comp
    Assert.assertTrue(obj.collected.getOrElse("") == "") //raccolta solo nel tracciato std
  }

  def checkNewRouteObj(obj: Rml): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "RML") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-03-05T03:43:03.052000")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == (-1)) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == (-1)) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "01023900023221") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare_gas/TMG_00757920152/DISTRIBUTORE/TMG_00757920152_12300020158/2021/0304/00757920152_12300020158_202102_RML_20210304123016_1_R.zip") //local_file
    Assert.assertTrue(obj.motivation.get == 6) //mottRettLett motivazione
    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "0006474103") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("30/09/2018")) == 0) //data_comp
    Assert.assertTrue(obj.collected.getOrElse("") == "AC") //raccolta solo nel tracciato std
  }


  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = RmlTable("./src/test/resources/integrazione-ca/misure/rml/old_route").get()
    val obj: Rml = measures.take(1)(0)
    checkOldRouteObj(obj)
  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = RmlTable("./src/test/resources/integrazione-ca/misure/rml/new_route").get()
    val obj: Rml = measures.take(1)(0)
    checkNewRouteObj(obj)

  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = RmlTable("./src/test/resources/integrazione-ca/misure/rml/both_route").get()
    val obj = measures.take(2)
    val obj_n: Rml = obj.filter(_.isNewRoute == true)(0)
    val obj_o: Rml = obj.filter(_.isNewRoute == false)(0)

    checkOldRouteObj(obj_o)
    checkNewRouteObj(obj_n)
  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3, rdd4 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rml(motivation = Some(1),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result1 = new RmlTable(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rml(motivation = Some(6),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result2 = new RmlTable(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rml(motivation = Some(0),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result3 = new RmlTable(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())

    val rdd4 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Rml(motivation = Some(7),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result4 = new RmlTable(null).filterValidRows(rdd4)
    Assert.assertEquals(0, result4.count())
  }
}
