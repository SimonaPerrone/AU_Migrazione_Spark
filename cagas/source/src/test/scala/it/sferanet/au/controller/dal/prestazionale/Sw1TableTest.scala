package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.{Sm1Table, Sw1Table}
import it.sferanet.au.model.prestazionale.{Sm1, Sw1}
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class Sw1TableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()


  def checkEffObj(obj: Sw1): Unit = {
    Assert.assertTrue(!obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "SW1") //flusso
    //    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-02-25T03:42:03.953000"))==0) //d_caricamento
    //    Assert.assertTrue(obj.converted.get == 28376.0) //letTotConv converted measure
    //    Assert.assertTrue(obj.measure.get == 28375.0) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "15270000000546") //cod_pdr
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare1/GAS_INJ/TMG_01605360666/DISTRIBUTORE/TMG_01605360666_02106730415/2020/1006/01605360666_02106730415_202003_SW10350_20200302115035_3.xml") //local_file
    Assert.assertTrue(obj.readType.get == 'S') //esito
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.get == "780340") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("01/03/2020")) == 0) //data_comp
  }

  def checkSostObj(obj: Sw1): Unit = {
    Assert.assertTrue(!obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "SW1") //flusso
    //    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-02-25T03:42:03.953000"))==0) //d_caricamento
    Assert.assertTrue(obj.converted.get == 28373.0) //letTotConv converted measure
    Assert.assertTrue(obj.measure.get == 28371.0) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "15270000000820") //cod_pdr
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare1/GAS_INJ/TMG_01605360666/DISTRIBUTORE/TMG_01605360666_02106730415/2020/1006/01605360666_02106730415_202003_SW10350_20200302115035_10.xml") //local_file
    Assert.assertTrue(obj.readType.get == 'E') //esito
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.get == "788119") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("01/03/2020")) == 0) //data_comp
  }


  @Test
  def testOnlyEffOld(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val obj = Sw1Table("./src/test/resources/integrazione-ca/misure/sw1/old_route/eff").get().take(1).head
    checkEffObj(obj)
  }

  @Test
  def testOnlySostOld(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val obj = Sw1Table("./src/test/resources/integrazione-ca/misure/sw1/old_route/sost").get().take(1).head
    checkSostObj(obj)
  }

  @Test
  def testEffAndSostOld(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = Sw1Table("./src/test/resources/integrazione-ca/misure/sw1/old_route/eff_and_sost").get()
    val objeff = measures.filter(sw1 => sw1.pdr == "15270000000546").take(1).head
    val objesost = measures.filter(sw1 => sw1.pdr == "15270000000820").take(1).head
    checkSostObj(objesost)
    checkEffObj(objeff)
  }

  @Test
  def testEffOldAndStandard(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = Sw1Table("./src/test/resources/integrazione-ca/misure/sw1/old_route/eff_and_sost").get()
    val objeff = measures.filter(sw1 => sw1.pdr == "15270000000546").take(1).head
    val nNewRoute = measures.filter(sw1 => sw1.pdr == "fake1").count()
    Assert.assertTrue(nNewRoute == 0)
    checkEffObj(objeff)
  }

  @Test
  def test0351Filtered(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = Sw1Table("./src/test/resources/integrazione-ca/misure/sw1/to_filter_out").get()
    Assert.assertTrue(measures.isEmpty())
  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1,  OK
    // rdd2, rdd3 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Sw1(ammissibilita = None, readType = Some('E'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result1 = new Sw1Table(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Sw1(ammissibilita = None, readType = Some('S'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result2 = new Sw1Table(null).filterValidRows(rdd2)
    Assert.assertEquals(0, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Sw1(ammissibilita = Some("OK"), readType = Some('E'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result3 = new Sw1Table(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())
  }

}
