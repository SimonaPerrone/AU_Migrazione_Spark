package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.AD5Table
import it.sferanet.au.model.prestazionale.AD5
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

import java.text.SimpleDateFormat

class AD5TableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy")
  val dateFormat2: SimpleDateFormat = Constants.FORMAT_DATE_LOAD

  def chekNew(obj: AD5): Unit = {
    Assert.assertTrue(obj.service == "AD5")
    Assert.assertTrue(obj.pdr == "16280000305431")
    Assert.assertTrue(obj.readType.get == 'E')
    Assert.assertTrue(dateFormat.format(obj.date.get) == "18/12/2019")
    Assert.assertTrue(obj.measure.get == 40024.0)
    Assert.assertTrue(obj.converted.get == 14857.0)
    Assert.assertTrue(obj.serialNumberMis.get == "0023428951")
    Assert.assertTrue(obj.serialNumberConv.get == "MIT0030010096131")
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_01141160992/2021/0225/00489490011_01141160992_201912_AD5_20210225120008_1_M.zip")
    //fixme Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2021-03-06T06:54:03.289817")
    Assert.assertTrue(obj.isNewRoute)


    Assert.assertTrue(obj.raccolta.get == "T")
  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = AD5Table("./src/test/resources/integrazione-ca/misure/ad5/new_route").get()
    val obj: AD5 = measures.take(1)(0)
    Assert.assertTrue(measures.filter(p => p.isNewRoute == false).count() == 0)
    chekNew(obj)
  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      AD5(readType = Some('E'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, raccolta = null)
    )
    val result1 = new AD5Table(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      AD5(readType = Some('A'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, raccolta = null)
    )
    val result2 = new AD5Table(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      AD5(readType = Some('S'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, raccolta = null)
    )
    val result3 = new AD5Table(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())
  }
}
