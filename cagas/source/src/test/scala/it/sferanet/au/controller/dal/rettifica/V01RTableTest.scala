package it.sferanet.au.controller.dal.rettifica

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.rettifica.V01RTable
import it.sferanet.au.model.rettifica.V01R
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

import java.text.SimpleDateFormat

class V01RTableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy")
  val dateFormat2: SimpleDateFormat = Constants.FORMAT_DATE_LOAD

  def chekNew(obj: V01R): Unit = {
    Assert.assertTrue(obj.service == "V01R")
    Assert.assertTrue(obj.pdr == "11980100100759")
    Assert.assertTrue(dateFormat.format(obj.date.get) == "15/01/2021")
    Assert.assertTrue(obj.measure.get == 10043.0)
    Assert.assertTrue(obj.converted.isEmpty)
    Assert.assertTrue(obj.serialNumberMis.get == "61849494")
    Assert.assertTrue(obj.serialNumberConv.isEmpty)
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_07231501219/DISTRIBUTORE/TMG_07231501219_06567051211/2021/0210/07231501219_06567051211_202101_V01R_20210210104606_1_R.zip")
    //fixme Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2021-03-06T06:54:03.289817")
    Assert.assertTrue(obj.isNewRoute)
    Assert.assertTrue(obj.motivation.get == 1)
    Assert.assertTrue(obj.raccolta.get == "T")
  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = V01RTable("./src/test/resources/integrazione-ca/misure/v01r").get()
    val obj: V01R = measures.take(1)(0)
    Assert.assertTrue(measures.filter(p => p.isNewRoute == false).count() == 0)
    chekNew(obj)
  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3, rdd4 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      V01R(motivation = Some(1),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, raccolta = null)
    )
    val result1 = new V01RTable(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      V01R(motivation = Some(5),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, raccolta = null)
    )
    val result2 = new V01RTable(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      V01R(motivation = Some(0),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, raccolta = null)
    )
    val result3 = new V01RTable(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())

    val rdd4 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      V01R(motivation = Some(6),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, raccolta = null)
    )
    val result4 = new V01RTable(null).filterValidRows(rdd4)
    Assert.assertEquals(0, result4.count())
  }

}
