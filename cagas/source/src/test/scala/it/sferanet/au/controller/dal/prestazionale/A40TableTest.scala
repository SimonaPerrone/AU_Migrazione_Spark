package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.A40Table
import it.sferanet.au.model.prestazionale.A40
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class A40TableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()


  def checkOldRouteObj(obj: A40): Unit = {
    Assert.assertTrue(!obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "A40") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2020-06-19T13:22:46.690333")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == (-1)) //segn_conv
    Assert.assertTrue(obj.measure.getOrElse(-1) == 13412) // segn_mis
    Assert.assertTrue(obj.pdr == "01510000013225") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare1/GAS_INJ/TMG_08317570151/DISTRIBUTORE/TMG_08317570151_01368720080/2020/0515/08317570151_01368720080_202005_A400150_20200515114330_2.xml") //local_file

    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "7111485") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("15/09/2011")) == 0) //data_comp
    Assert.assertTrue(obj.outcome.getOrElse('^') == 1) //esito
    Assert.assertTrue(obj.collected.getOrElse("") == "") //raccolta

  }

  def checkNewRouteObj(obj: A40): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "A40") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-03-19T03:31:47.584000")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == (-1)) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == 0) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "61491833004856") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare_gas/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_06655971007/2021/0318/06724610966_06655971007_202103_A40_20210318034457_00_M.zip") //local_file

    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "MTSB035604940616") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("17/03/2021")) == 0) //data_pres
    Assert.assertTrue(obj.outcome.isEmpty) //esito
    Assert.assertTrue(obj.readType.getOrElse("") == 'E') //tipo_lettura
    Assert.assertTrue(obj.collected.getOrElse("") == "T") //raccolta
  }


  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    //    val env:Environment=SparkTest.env
    //    val measures=A40Table("./src/test/resources/integrazione-ca/misure/A40/old_route").get()
    //    val obj:A40=measures.take(1)(0)
    //    checkOldRouteObj(obj)
  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    //    val env:Environment=SparkTest.env
    //    val measures=A40Table("./src/test/resources/integrazione-ca/misure/A40/new_route").get()
    //    val obj:A40=measures.take(1)(0)
    //    checkNewRouteObj(obj)

  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    //    val env:Environment=SparkTest.env
    //    val measures=A40Table("./src/test/resources/integrazione-ca/misure/A40/both_route").get()
    //    val obj = measures.take(2)
    //    val obj_n:A40=obj.filter(_.isNewRoute==true)(0)
    //    val obj_o:A40=obj.filter(_.isNewRoute==false)(0)
    //
    //    checkOldRouteObj(obj_o)
    //    checkNewRouteObj(obj_n)

  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      A40(ammissibilita = None, readType = None, outcome = Some('1'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null,
        collected = null, local_file = null, d_caricamento = null, isNewRoute = false)
    )
    val result1 = new A40Table(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      A40(ammissibilita = Some("OK"), readType = Some('E'), outcome = None,
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null,
        collected = null, local_file = null, d_caricamento = null, isNewRoute = false)
    )
    val result2 = new A40Table(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      A40(ammissibilita = Some("OK"), readType = Some('A'), outcome = None,
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null,
        collected = null, local_file = null, d_caricamento = null, isNewRoute = false)
    )
    val result3 = new A40Table(null).filterValidRows(rdd3)
    Assert.assertEquals(1, result3.count())

    val rdd4 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      A40(ammissibilita = None, readType = None, outcome = Some('0'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null,
        collected = null, local_file = null, d_caricamento = null, isNewRoute = false)
    )
    val result4 = new A40Table(null).filterValidRows(rdd4)
    Assert.assertEquals(0, result4.count())

    val rdd5 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      A40(ammissibilita = Some("OK"), readType = Some('S'), outcome = None,
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null,
        collected = null, local_file = null, d_caricamento = null, isNewRoute = false)
    )
    val result5 = new A40Table(null).filterValidRows(rdd5)
    Assert.assertEquals(0, result5.count())
  }

}
