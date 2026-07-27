package it.sferanet.au.controller.dal.periodico

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.periodico.{TglTable, TmlTable}
import it.sferanet.au.model.periodico.{Tgl, Tml}
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

import java.text.SimpleDateFormat

class TmlTableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy")
  val dateFormat2: SimpleDateFormat = Constants.FORMAT_DATE_LOAD

  def chekNew(obj: Tml): Unit = {
    Assert.assertTrue(obj.service == "TML")
    Assert.assertTrue(obj.pdr == "11610000110402") //cod_prd
    Assert.assertTrue(dateFormat.format(obj.date.get) == "01/03/2021") //data
    Assert.assertTrue(obj.isValid.isEmpty) //esitoVal
    Assert.assertTrue(obj.measure.get == 179.0) //letTotPrel
    Assert.assertTrue(obj.converted.getOrElse(0) == 0) //letTotConv
    Assert.assertTrue(obj.serialNumberMis.get == "MTSB037103530829") //matricola misuratore
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matricola convertitore
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_03178060236/DISTRIBUTORE/TMG_03178060236_02968430237/2021/0303/03178060236_02968430237_202103_TML_20210303111927_1_M.zip")
    //fixme
    //    Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2021-03-06T06:54:03.289817")
    Assert.assertTrue(obj.isNewRoute)
  }

  def checkOld(obj: Tml): Unit = {
    Assert.assertTrue(obj.service == "TML")
    Assert.assertTrue(obj.pdr == "00105200028333") //cod_prd
    Assert.assertTrue(dateFormat.format(obj.date.get) == "31/03/2020") //data
    Assert.assertTrue(obj.isValid.get == "SI") //esitoVal
    Assert.assertTrue(obj.measure.get == 5696.0) //letTotPrel
    Assert.assertTrue(obj.converted.getOrElse(0) == 0) //letTotConv
    Assert.assertTrue(obj.serialNumberMis.get == "SMGR034115189367") //matricola misuratore
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matricola convertitore
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2020/0405/12883450152_12883420155_202003_TML0050_20200405072729_991.xml")
    //    'T'HH:mm:ss.ssssss
    //    fixme
    //    Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2020-10-29T19:37:48.877351")
    Assert.assertTrue(!obj.isNewRoute)
  }

  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TmlTable("./src/test/resources/integrazione-ca/misure/tml/old_route").get()
    val obj: Tml = measures.filter(rmv => rmv.pdr == "00105200028333").take(1)(0)
    checkOld(obj)


  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TmlTable("./src/test/resources/integrazione-ca/misure/tml/new_route").get()
    val Tml: RDD[Tml] = measures.filter(rmv => rmv.pdr == "11610000110402")
    val obj: Tml = Tml.take(1)(0)
    chekNew(obj)


  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TmlTable("./src/test/resources/integrazione-ca/misure/tml/both_route").get()
    val newT: Tml = measures.filter(rmv => rmv.pdr == "11610000110402").take(1)(0)
    val old: Tml = measures.filter(rmv => rmv.pdr == "00105200028333").take(1)(0)
    checkOld(old)
    chekNew(newT)
  }

  def testFilterValid(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = Constants.getDate(simpleDateFormat, "2020-01-01")

    // rdd1, rdd2 OK
    // rdd3 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Tml(readType = Some('E'), isValid = Some("SI"), ammissibilita = None,
        service = null, pdr = "1", date = date, pivaDistr = null, pivaUtente = null,  measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null, d_caricamento = null, isNewRoute = false)
    )
    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Tml(readType = Some('E'), isValid = None, ammissibilita = Some("OK"),
        service = null, pdr = "1", date = date, pivaDistr = null, pivaUtente = null,  measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null, d_caricamento = null, isNewRoute = false)
    )
    // BAD read type
    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Tml(readType = Some('S'), isValid = None, ammissibilita = Some("OK"),
        service = null, pdr = "1", date = date, pivaDistr = null, pivaUtente = null,  measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null, d_caricamento = null, isNewRoute = false)
    )

    val result1 = new TmlTable(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val result2 = new TmlTable(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val result3 = new TmlTable(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())
  }
}
