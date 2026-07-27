package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.FUITable
import it.sferanet.au.model.prestazionale.FUI
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

import java.text.SimpleDateFormat

class FUITableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy")
  val dateFormat2: SimpleDateFormat = Constants.FORMAT_DATE_LOAD

  def chekEff(obj: FUI): Unit = {
    Assert.assertTrue(obj.service == "FUI")
    Assert.assertTrue(obj.pdr == "05260200281588")
    Assert.assertTrue(obj.readType.get == 'E')
    //    Assert.assertTrue(dateFormat.format(obj.date.get) == "01/04/2021")
    Assert.assertTrue(obj.measure.get == 100.0)
    Assert.assertTrue(obj.converted.get == 105.0)
    Assert.assertTrue(obj.serialNumberMis.get == "MIT0032012135789")
    Assert.assertTrue(obj.serialNumberConv.isEmpty)
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_02221101203/2021/0403/12883450152_02221101203_202104_FUI_20210403043122_177_M.zip")
    //fixme Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2021-03-06T06:54:03.289817")
    Assert.assertTrue(obj.isNewRoute)

  }

  def chekSost(obj: FUI): Unit = {
    Assert.assertTrue(obj.service == "FUI")
    Assert.assertTrue(obj.pdr == "15810000024807")
    Assert.assertTrue(obj.readType.get == 'E')
    Assert.assertTrue(dateFormat.format(obj.date.get) == "01/04/2021")
    Assert.assertTrue(obj.measure.get == 102.0)
    Assert.assertTrue(obj.converted.isEmpty)
    Assert.assertTrue(obj.serialNumberMis.get == "MIT0032111543379")
    Assert.assertTrue(obj.serialNumberConv.isEmpty)
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_01341400198/DISTRIBUTORE/TMG_01341400198_02221101203/2021/0403/01341400198_02221101203_202104_FUI_20210403043939_78_M.zip")
    //fixme Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2021-03-06T06:54:03.289817")
    Assert.assertTrue(obj.isNewRoute)

  }

  @Test
  def testAppOnlyEff(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val obj = FUITable("./src/test/resources/integrazione-ca/misure/fui/new_route/eff").get().take(1)(0)
    chekEff(obj)


  }

  @Test
  def testAppOnlySost(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val obj = FUITable("./src/test/resources/integrazione-ca/misure/fui/new_route/sost").get().take(1)(0)
    chekSost(obj)


  }

  @Test
  def testAppEffAndSost(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measure = FUITable("./src/test/resources/integrazione-ca/misure/fui/new_route/eff_and_sost").get()
    val eff = measure.filter(fui => fui.pdr == "05260200281588").take(1)(0)
    val sost = measure.filter(fui => fui.pdr == "15810000024807").take(1)(0)

    Assert.assertTrue(measure.filter(p => p.isNewRoute == false).count() == 0)
    chekEff(eff)
    chekSost(sost)

  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      FUI(readType = Some('E'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, raccolta = null)
    )
    val result1 = new FUITable(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      FUI(readType = Some('A'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, raccolta = null)
    )
    val result2 = new FUITable(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      FUI(readType = Some('S'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, raccolta = null)
    )
    val result3 = new FUITable(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())
  }
}
