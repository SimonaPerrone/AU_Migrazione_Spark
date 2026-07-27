package it.sferanet.au.controller.dal.autolettura

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.autolettura.TalTable
import it.sferanet.au.model.autolettura.Tal
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.rdd.RDD
import org.junit.{Assert, Test}

import java.text.SimpleDateFormat

class TalTableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy")
  val dateFormat2: SimpleDateFormat = Constants.FORMAT_DATE_LOAD

  def chekNew(obj: Tal): Unit = {
    Assert.assertTrue(obj.service == "TAL")
    Assert.assertTrue(obj.pdr == "03130000814534") //cod_prd
    Assert.assertTrue(dateFormat.format(obj.date.get) == "28/01/2020") //data
    Assert.assertTrue(obj.outcome.get == 'F') //esitoVal
    Assert.assertTrue(obj.measure.get == 38961.0) //letTotPrel
    Assert.assertTrue(obj.converted.getOrElse(0) == 0) //letTotConv
    Assert.assertTrue(obj.serialNumberMis.get == "3435174") //matricola misuratore
    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matricola convertitore
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_01217720539/DISTRIBUTORE/TMG_01217720539_03916040656/2021/0222/01217720539_03916040656_202001_TAL_20210222113102_1_M.zip")
    //fixme
    //    Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2021-03-06T06:54:03.289817")
    Assert.assertTrue(obj.isNewRoute)

  }

  def checkOld(obj: Tal): Unit = {
    Assert.assertTrue(obj.service == "TAL")
    Assert.assertTrue(obj.pdr == "03081000238930") //cod_prd
    Assert.assertTrue(dateFormat.format(obj.date.get) == "03/03/2020") //data
    Assert.assertTrue(obj.outcome.get == 'F') //esitoVal
    Assert.assertTrue(obj.measure.get == 4387.0) //letTotPrel
    Assert.assertTrue(obj.converted.getOrElse(0) == 0) //letTotConv
    Assert.assertTrue(obj.serialNumberMis.get == "229170032110099600") //matricola misuratore
    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matricola convertitore
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare1/GAS_INJ/TMG_03479071205/DISTRIBUTORE/TMG_03479071205_10246981004/2020/1026/03479071205_10246981004_202003_TAL.0150_20201026100007_0001.XML")

    println(Constants.getDate(Constants.FORMAT_DATE_LOAD, "2020-10-29T19:19:00.812019"))
    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd"), "2020-10-29"))
    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"), "2020-10-29T19:37:48.877351"))
    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS"), "2020-10-29T19:37:48.87"))
    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"), "2020-10-29T19:37:48.877"))
    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSS"), "2020-10-29T19:37:48.8773"))
    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSS"), "2020-10-29T19:37:48.87735"))
    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"), "2020-10-29T19:37:48.877351"))
    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), "2020-10-29T19:37:48.877351"))

    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd"), "2020-10-29"))
    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.ssssss"), "2020-10-29T19:37:48.877351"))
    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.ss"), "2020-10-29T19:37:48.87"))
    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss"), "2020-10-29T19:37:48.877"))
    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.ssss"), "2020-10-29T19:37:48.8773"))
    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sssss"), "2020-10-29T19:37:48.87735"))
    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.ssssss"), "2020-10-29T19:37:48.877351"))
    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), "2020-10-29T19:37:48.877351"))


    println(Constants.getDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"), "2020-10-29T19:37:48"))

    //    'T'HH:mm:ss.ssssss
    //    fixme
    //    Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2020-10-29T19:37:48.877351")
    Assert.assertTrue(!obj.isNewRoute)
  }

  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TalTable("./src/test/resources/integrazione-ca/misure/tal/old_route").get()
    val obj: Tal = measures.take(1)(0)
    checkOld(obj)


  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TalTable("./src/test/resources/integrazione-ca/misure/tal/new_route").get()
    val tal: RDD[Tal] = measures.filter(rmv => rmv.pdr == "03130000814534")
    val obj: Tal = tal.take(1)(0)
    chekNew(obj)
  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TalTable("./src/test/resources/integrazione-ca/misure/tal/both_route").get()
    val newT: Tal = measures.filter(rmv => rmv.pdr == "03130000814534").take(1)(0)
    val old: Tal = measures.filter(rmv => rmv.pdr == "03081000238930").take(1)(0)
    checkOld(old)
    chekNew(newT)
  }
}
