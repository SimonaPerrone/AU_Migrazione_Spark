package it.sferanet.au.controller.dal.autolettura

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.autolettura.TavTable
import it.sferanet.au.model.autolettura.Tav
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.rdd.RDD
import org.junit.{Assert, Test}

import java.text.SimpleDateFormat

class TavTableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy")
  val dateFormat2: SimpleDateFormat = Constants.FORMAT_DATE_LOAD

  def chekNew(obj: Tav): Unit = {
    Assert.assertTrue(obj.service == "TAV")
    Assert.assertTrue(obj.pdr == "15340000007189") //cod_prd
    Assert.assertTrue(dateFormat.format(obj.date.get) == "25/10/2018") //data
    Assert.assertTrue(obj.outcome.get == 'F') //esitoVal
    Assert.assertTrue(obj.measure.get == 233.0) //letTotPrel
    Assert.assertTrue(obj.converted.isEmpty) //letTotConv
    Assert.assertTrue(obj.serialNumberMis.get == "5678066") //matricola misuratore
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matricola convertitore
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_04080690656/DISTRIBUTORE/TMG_04080690656_06655971007/2021/0107/04080690656_06655971007_201810_TAV_20210107174422_19_M.zip")
    //fixme
    //    Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2021-03-06T06:54:03.289817")
    Assert.assertTrue(obj.isNewRoute)

  }

  def checkOld(obj: Tav): Unit = {
    Assert.assertTrue(obj.service == "TAV")
    Assert.assertTrue(obj.pdr == "02260146510020") //cod_prd
    Assert.assertTrue(dateFormat.format(obj.date.get) == "20/03/2020") //data
    Assert.assertTrue(obj.outcome.get == 'V') //esitoVal
    Assert.assertTrue(obj.measure.get == 25385.0) //letTotPrel
    Assert.assertTrue(obj.converted.isEmpty) //letTotConv
    Assert.assertTrue(obj.serialNumberMis.get == "2056895911") //matricola misuratore
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matricola convertitore
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare1/GAS_INJ/TMG_11744720159/DISTRIBUTORE/TMG_11744720159_08526440154/2020/1028/11744720159_08526440154_202003_TAV0150_20201028170243_1.xml")
    //    'T'HH:mm:ss.ssssss
    //    fixme
    //    Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2020-10-29T19:37:48.877351")
    Assert.assertTrue(!obj.isNewRoute)
  }

  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TavTable("./src/test/resources/integrazione-ca/misure/tav/old_route").get()
    val obj: Tav = measures.take(1)(0)
    checkOld(obj)


  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TavTable("./src/test/resources/integrazione-ca/misure/tav/new_route").get()
    val Tav: RDD[Tav] = measures.filter(rmv => rmv.pdr == "15340000007189")
    val obj: Tav = Tav.take(1)(0)
    chekNew(obj)


  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TavTable("./src/test/resources/integrazione-ca/misure/tav/both_route").get()
    val newT: Tav = measures.filter(rmv => rmv.pdr == "15340000007189").take(1)(0)
    val old: Tav = measures.filter(rmv => rmv.pdr == "02260146510020").take(1)(0)
    checkOld(old)
    chekNew(newT)
  }
}
