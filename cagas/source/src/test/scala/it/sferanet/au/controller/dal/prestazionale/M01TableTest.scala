package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.M01Table
import it.sferanet.au.model.prestazionale.M01
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class M01TableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()


  def checkNewRouteObj(obj: M01): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "M01") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-02-25T03:42:03.953000")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == (-1)) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == 243035) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "01611113001514") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare_gas/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_12300020158/2021/0224/06724610966_12300020158_202102_M01_20210224092953_00_M.zip") //local_file

    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "003022069") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("23/02/2021")) == 0) //data_comp
    Assert.assertTrue(obj.readType.getOrElse("") == 'E') //tipoLettura
    Assert.assertTrue(obj.collected.getOrElse("") == "T") //raccolta
  }


  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = M01Table("./src/test/resources/integrazione-ca/misure/m01/old_route").get()
    Assert.assertTrue(measures.isEmpty())
  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = M01Table("./src/test/resources/integrazione-ca/misure/m01/new_route").get()
    val obj: M01 = measures.take(1)(0)
    checkNewRouteObj(obj)

  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = M01Table("./src/test/resources/integrazione-ca/misure/m01/both_route").get()
    val obj = measures.take(2)
    val obj_n: M01 = obj.filter(_.isNewRoute == true)(0)
    val obj_o = obj.filter(_.isNewRoute == false)

    Assert.assertTrue(obj_o.isEmpty)
    checkNewRouteObj(obj_n)

  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      M01(readType = Some('E'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result1 = new M01Table(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      M01(readType = Some('A'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result2 = new M01Table(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      M01(readType = Some('S'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result3 = new M01Table(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())
  }
}
