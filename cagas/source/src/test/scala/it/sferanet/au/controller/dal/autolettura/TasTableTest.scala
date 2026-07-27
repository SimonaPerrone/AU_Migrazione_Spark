package it.sferanet.au.controller.dal.autolettura

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.autolettura.TasTable
import it.sferanet.au.model.autolettura.Tas
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.junit.{Assert, Test}

import java.text.SimpleDateFormat

class TasTableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy")
  val dateFormat2: SimpleDateFormat = Constants.FORMAT_DATE_LOAD

  private def checkOld(obj: Tas): Unit = {
    Assert.assertTrue(obj.service == "TAS")
    Assert.assertTrue(obj.pdr == "03340011173335") //cod_prd
    Assert.assertTrue(dateFormat.format(obj.date.get) == "02/03/2020") //data
    Assert.assertTrue(obj.outcome.get == 'F') //esitoVal
    Assert.assertTrue(obj.measure.get == 2171.0) //letTotPrel
    Assert.assertTrue(obj.converted.getOrElse(0) == 0) //letTotConv
    Assert.assertTrue(obj.serialNumberMis.get == "30018930") //matricola misuratore
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matricola convertitore
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare1/GAS_INJ/TMG_01177760491/DISTRIBUTORE/TMG_01177760491_07670380968/2020/1119/01177760491_07670380968_202003_TAS0150_20201119095023_1.XML")
    //    fixme
    //    Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2020-10-29T19:37:48.877351")
    Assert.assertTrue(!obj.isNewRoute)
  }

  private def checkNew(obj: Tas): Unit = {
    Assert.assertTrue(obj.service == "TAS")
    Assert.assertTrue(obj.pdr == "11370000037631") //cod_prd
    println(obj.date)
    Assert.assertTrue(dateFormat.format(obj.date.get) == "30/03/2021") //data
    Assert.assertTrue(obj.outcome.get == 'V') //esitoVal
    Assert.assertTrue(obj.measure.get == 5733.0) //letTotPrel
    Assert.assertTrue(obj.converted.getOrElse(0) == 0) //letTotConv
    Assert.assertTrue(obj.serialNumberMis.get == "MIT0032110019953") //matricola misuratore
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matricola convertitore
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_00930530324/DISTRIBUTORE/TMG_00930530324_12300020158/2021/0331/00930530324_12300020158_202103_TAS_20210331132201_1_M.zip")
    //fixme
    //    Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2021-03-06T06:54:03.289817")
    Assert.assertTrue(obj.isNewRoute)
  }

  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TasTable("./src/test/resources/integrazione-ca/misure/tas/old_route").get()
    val obj: Tas = measures.filter(rmv => rmv.pdr == "03340011173335").take(1)(0)
    checkOld(obj)

  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TasTable("./src/test/resources/integrazione-ca/misure/tas/new_route").get()
    val obj: Tas = measures.take(1)(0)
    checkNew(obj)
  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TasTable("./src/test/resources/integrazione-ca/misure/tas/both_route").get()
    val old: Tas = measures.filter(rmv => rmv.pdr == "03340011173335").take(1)(0)
    val newT: Tas = measures.filter(rmv => rmv.pdr == "11370000037631").take(1)(0)
    checkOld(old)
    checkNew(newT)


  }
}
