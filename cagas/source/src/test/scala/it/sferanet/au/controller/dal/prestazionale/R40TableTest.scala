package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.R40Table
import it.sferanet.au.model.prestazionale.R40
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class R40TableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  def checkNewRouteObj(obj: R40): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "R40") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-02-05T03:57:02.191000")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == (-1)) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == 0) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "09951201424304") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare_gas/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_01178580997/2021/0204/00489490011_01178580997_202005_R40_20210204150006_1_M.zip") //local_file

    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "MIT0032013581795") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("06/05/2020")) == 0) //data_comp
    Assert.assertTrue(obj.readType.getOrElse("") == 'E') //tipoLettura
    Assert.assertTrue(obj.collected.getOrElse("") == "T")
  }


  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = R40Table("./src/test/resources/integrazione-ca/misure/r40").get()
    val obj: R40 = measures.take(1)(0)
    Assert.assertTrue(measures.filter(p => p.isNewRoute == false).count() == 0)
    checkNewRouteObj(obj)

  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      R40(readType = Some('E'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result1 = new R40Table(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      R40(readType = Some('A'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result2 = new R40Table(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      R40(readType = Some('S'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, collected = null)
    )
    val result3 = new R40Table(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())
  }
}
