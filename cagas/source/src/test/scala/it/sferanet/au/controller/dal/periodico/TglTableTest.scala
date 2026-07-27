package it.sferanet.au.controller.dal.periodico

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.periodico.TglTable
import it.sferanet.au.model.periodico.Tgl
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

class TglTableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy")
  val dateFormat2: SimpleDateFormat = Constants.FORMAT_DATE_LOAD

  def chekNew(obj: Tgl): Unit = {
    Assert.assertTrue(obj.service == "TGL")
    Assert.assertTrue(obj.pdr == "00600002006264") //cod_prd
    Assert.assertTrue(dateFormat.format(obj.date.get) == "28/02/2021") //data
    Assert.assertTrue(obj.isValid.isEmpty) //esitoVal
    Assert.assertTrue(obj.measure.get == 62458.0) //letTotPrel
    Assert.assertTrue(obj.converted.getOrElse(0) == 0) //letTotConv
    Assert.assertTrue(obj.serialNumberMis == Some("1645002475")) //matricola misuratore
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matricola convertitore
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_00850720194/DISTRIBUTORE/TMG_00850720194_02968430237/2021/0305/00850720194_02968430237_202102_TGL_20210305143919_1_M.zip")
    //fixme
    //    Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2021-03-06T06:54:03.289817")
    Assert.assertTrue(obj.isNewRoute)

  }

  def checkOld(obj: Tgl): Unit = {
    Assert.assertTrue(obj.service == "TGL")
    Assert.assertTrue(obj.pdr == "15350500800817") //cod_prd
    Assert.assertTrue(dateFormat.format(obj.date.get) == "01/03/2020") //data
    Assert.assertTrue(obj.isValid.get == "SI") //esitoVal
    Assert.assertTrue(obj.measure.get == 7923.0) //letTotPrel
    Assert.assertTrue(obj.converted.getOrElse(0) == 0) //letTotConv
    Assert.assertTrue(obj.serialNumberMis.get == "MTSB036703087981") //matricola misuratore
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matricola convertitore
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare1/GAS_INJ/TMG_04802420267/DISTRIBUTORE/TMG_04802420267_01201910260/2020/0908/04802420267_01201910260_202003_TGL0050_20200908150002_265.xml")
    //    fixme
    //    Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2020-10-29T19:37:48.877351")
    Assert.assertTrue(!obj.isNewRoute)
  }

  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TglTable("./src/test/resources/integrazione-ca/misure/tgl/old_route").get()
    val obj: Tgl = measures.filter(obj => obj.pdr == "15350500800817" && obj.local_file.get == "/mnt/isilonshare1/GAS_INJ/TMG_04802420267/DISTRIBUTORE/TMG_04802420267_01201910260/2020/0908/04802420267_01201910260_202003_TGL0050_20200908150002_265.xml").take(1)(0)
    checkOld(obj)


  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TglTable("./src/test/resources/integrazione-ca/misure/tgl/new_route").get()
    val obj: Tgl = measures.take(1)(0)
    chekNew(obj)
  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TglTable("./src/test/resources/integrazione-ca/misure/tgl/both_route").get()
    val newT: Tgl = measures.filter(rmv => rmv.pdr == "00600002006264").take(1)(0)
    val old: Tgl = measures.filter(rmv => rmv.pdr == "15350500800817").take(1)(0)
    checkOld(old)
    chekNew(newT)
  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = Constants.getDate(simpleDateFormat, "2020-01-01")

    // rdd1, rdd2 OK
    // rdd3 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Tgl(readType = Some('E'), isValid = Some("SI"), ammissibilita = None,
        service = null, pdr = "1", date = date, pivaDistr = null, pivaUtente = null,  measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null, d_caricamento = null, isNewRoute = false)
    )
    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Tgl(readType = Some('E'), isValid = None, ammissibilita = Some("OK"),
        service = null, pdr = "1", date = date, pivaDistr = null, pivaUtente = null,  measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null, d_caricamento = null, isNewRoute = false)
    )
    // BAD read type
    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Tgl(readType = Some('S'), isValid = None, ammissibilita = Some("OK"),
        service = null, pdr = "1", date = date, pivaDistr = null, pivaUtente = null,  measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null, d_caricamento = null, isNewRoute = false)
    )

    val result1 = new TglTable(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val result2 = new TglTable(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val result3 = new TglTable(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())
  }
}
