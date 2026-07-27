package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.{Sw1Table, Swg1Table}
import it.sferanet.au.model.prestazionale.{Sw1, Swg1}
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class Swg1TableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()


  def checkEffObj(obj: Swg1): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "SWG1") //flusso
    //    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-02-25T03:42:03.953000"))==0) //d_caricamento
    //    Assert.assertTrue(obj.converted.get==52131.0) //letTotConv converted measure
    //    Assert.assertTrue(obj.measure.get ==52130.0) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "08200000016470") //cod_pdr
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_02462970415/DISTRIBUTORE/TMG_02462970415_03429130234/2021/0217/02462970415_03429130234_202102_SWG1_20210205020713_2_M.zip") //local_file
    Assert.assertTrue(obj.readType.get == 'E') //esito
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.get == "3781598") //matr_mis matricola misuratore
    //    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("27/01/2021"))==0) //data_comp
  }

  def checkSostObj(obj: Swg1): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "SWG1") //flusso
    //    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-02-25T03:42:03.953000"))==0) //d_caricamento
    Assert.assertTrue(obj.converted.isEmpty) //letTotConv converted measure
    Assert.assertTrue(obj.measure.get == 52153.0) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "08200000016471") //cod_pdr
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_02462970415/DISTRIBUTORE/TMG_02462970415_03429130234/2021/0217/02462970415_03429130234_202102_SWG1_20210205020713_2_M.zip") //local_file
    Assert.assertTrue(obj.readType.get == 'E') //esito
    Assert.assertTrue(obj.serialNumberConv.isEmpty) //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.get == "3781598") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("01/02/2021")) == 0) //data_comp
  }


  @Test
  def testOnlyEffStandard(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val obj = Swg1Table("./src/test/resources/integrazione-ca/misure/swg1/standard/eff").get().take(1).head
    checkEffObj(obj)
  }

  @Test
  def testOnlySostStandard(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val obj = Swg1Table("./src/test/resources/integrazione-ca/misure/swg1/standard/sost").get().take(1).head
    checkSostObj(obj)
  }

  @Test
  def testEffAndSostStandard(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")

    val measures = Swg1Table("./src/test/resources/integrazione-ca/misure/swg1/standard/eff_and_sost").get()
    val objeff = measures.filter(Swg1 => Swg1.pdr == "08200000016470").take(1).head
    val objesost = measures.filter(Swg1 => Swg1.pdr == "08200000016471").take(1).head
    checkSostObj(objesost)
    checkEffObj(objeff)
  }

  @Test
  def testEffOldAndStandard(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = Swg1Table("./src/test/resources/integrazione-ca/misure/swg1/standard_and_old/").get()
    val objeff = measures.filter(Swg1 => Swg1.pdr == "08200000016470").take(1).head
    val nNewRoute = measures.filter(Swg1 => Swg1.pdr == "fake1").count()
    Assert.assertTrue(nNewRoute == 0)
    checkEffObj(objeff)
  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3  KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Swg1(ammissibilita = Some("OK"), readType = Some('E'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result1 = new Swg1Table(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Swg1(ammissibilita = Some("OK"), readType = Some('A'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result2 = new Swg1Table(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Swg1(ammissibilita = Some("OK"), readType = Some('S'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result3 = new Swg1Table(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())
  }

}
