package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.A02Table
import it.sferanet.au.model.prestazionale.A02
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class A02TableTest extends EnvironmentSparkTest with Checker {

  def checkOldRouteObj(obj: A02): Unit = {
    Assert.assertTrue(!obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "A02") //flusso
    //    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2020-06-19T13:22:46.690333"))==0) //d_caricamento
    Assert.assertTrue(obj.converted.isEmpty) //segn_conv
    Assert.assertTrue(obj.measure.get == 4110.0) // segn_mis
    Assert.assertTrue(obj.pdr == "00883201890187") //cod_pdr
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare1/GAS_INJ/TMG_02166820510/DISTRIBUTORE/TMG_02166820510_12300020158/2020/1223/02166820510_12300020158_A020150_20201223085001_1.xml") //local_file

    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.get == "31075904") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("15/04/2020")) == 0) //data_comp
    Assert.assertTrue(obj.collected.isEmpty) //raccolta

  }

  def checkNewRouteObj(obj: A02): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "A02") //flusso
    //    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-03-19T03:31:47.584000"))==0) //d_caricamento
    Assert.assertTrue(obj.converted.isEmpty) //letTotConv converted measure
    Assert.assertTrue(obj.measure.get == 5.0) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "15964204320469") //cod_pdr
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_01219980529/2021/0216/05608890488_01219980529_201612_A02_20210216180009_1_M.zip") //local_file
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.get == "SMGR034115625405") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("07/12/2016")) == 0) //data_pres
    Assert.assertTrue(obj.readType.get == 'E') //tipo_lettura
    Assert.assertTrue(obj.collected.get == "T") //raccolta
  }


  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = A02Table("./src/test/resources/integrazione-ca/misure/a02/old_route").get()
    val obj: A02 = measures.take(1)(0)
    checkOldRouteObj(obj)
  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = A02Table("./src/test/resources/integrazione-ca/misure/a02/new_route").get()
    val obj: A02 = measures.take(1)(0)
    checkNewRouteObj(obj)

  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = A02Table("./src/test/resources/integrazione-ca/misure/a02/both_route").get()
    val old: A02 = measures.filter(r => r.pdr == "00883201890187").take(1)(0)
    val _new: A02 = measures.filter(r => r.pdr == "15964204320469").take(1)(0)
    checkOldRouteObj(old)
    checkNewRouteObj(_new)
  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      A02(ammissibilita = Some("OK"), readType = Some('E'),
        service = null, pdr = null,  date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null,
        collected = null, local_file = null, d_caricamento = null, isNewRoute = false)
    )
    val result1 = new A02Table(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      A02(ammissibilita = Some("OK"), readType = Some('A'),
        service = null, pdr = null,  date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null,
        collected = null, local_file = null, d_caricamento = null, isNewRoute = false)
    )
    val result2 = new A02Table(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      A02(ammissibilita = Some("OK"), readType = Some('S'),
        service = null, pdr = null,  date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null,
        collected = null, local_file = null, d_caricamento = null, isNewRoute = false)
    )
    val result3 = new A02Table(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())
  }

}
