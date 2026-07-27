package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.{FDDTable, TmvTable}
import it.sferanet.au.model.prestazionale.{FDD, Tmv}
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class TmvTableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  def checkOldRouteObj(obj: Tmv): Unit = {
    Assert.assertTrue(!obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "TMV") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2020-12-01T19:28:14.252140")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == 52135.0) //segn_conv
    Assert.assertTrue(obj.measure.getOrElse(-1) == 52134.0) // segn_mis
    Assert.assertTrue(obj.pdr == "08200000016471") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12300020158/2020/1130/00489490011_12300020158_202011_TMV0350_20201130203156_287.xml") //local_file
    Assert.assertTrue(obj.readType.getOrElse("") == 'S')
    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "ELS3034016763461") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("24/11/2020")) == 0) //data_comp
  }

  def checkNewRouteObj(obj: Tmv): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "TMV") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-03-06T05:05:36.837000")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == 52141.0) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == 52140.0) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "03390000023950") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare_gas/TMG_03167510365/DISTRIBUTORE/TMG_03167510365_07670380968/2021/0305/03167510365_07670380968_202103_TMV_20210305155519_36_M.zip") //local_file
    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "061145259") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("12/12/2020")) == 0) //data_pres
    Assert.assertTrue(obj.readType.getOrElse("") == 'E') //tipo_lettura
  }

  def checkOldRouteObjForBoth(obj: Tmv): Unit = {
    Assert.assertTrue(!obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "TMV") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2020-12-01T19:28:14.252140")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == -1) //segn_conv
    Assert.assertTrue(obj.measure.getOrElse(-1) == -1) // segn_mis
    Assert.assertTrue(obj.pdr == "00352800186457") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12300020158/2020/1130/00489490011_12300020158_202011_TMV0350_20201130203156_287.xml") //local_file
    Assert.assertTrue(obj.readType.getOrElse("") == 'S')
    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "ELS3034016763461") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("24/11/2020")) == 0) //data_comp
  }

  def checkNewRouteObjForBoth(obj: Tmv): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "TMV") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-03-06T05:05:36.837000")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == -1) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == -1) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "03390000023956") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare_gas/TMG_03167510365/DISTRIBUTORE/TMG_03167510365_07670380968/2021/0305/03167510365_07670380968_202103_TMV_20210305155519_36_M.zip") //local_file
    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "061145259") //matr_mis matricola misuratore
    //    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("")) == 0) //data_pres
    Assert.assertTrue(obj.readType.getOrElse("") == 'E') //tipo_lettura
  }


  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TmvTable("./src/test/resources/integrazione-ca/misure/tmv/old_route/sost").get()
    val obj: Tmv = measures.take(1)(0)
    checkOldRouteObj(obj)
  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TmvTable("./src/test/resources/integrazione-ca/misure/tmv/new_route/sost").get()
    val obj: Tmv = measures.take(1)(0)
    checkNewRouteObj(obj)
  }

  @Test
  def testAppBoth(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = TmvTable("./src/test/resources/integrazione-ca/misure/tmv/both_route").get()
    val obj = measures.take(2)
    val obj_n: Tmv = obj.filter(_.isNewRoute == true)(0)
    val obj_o: Tmv = obj.filter(_.isNewRoute == false)(0)

    checkOldRouteObjForBoth(obj_o)
    checkNewRouteObjForBoth(obj_n)
  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2, rdd3 OK
    //  rdd4, rdd5 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Tmv(ammissibilita = None, readType = Some('E'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result1 = new TmvTable(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Tmv(ammissibilita = Some("OK"), readType = Some('E'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result2 = new TmvTable(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Tmv(ammissibilita = Some("OK"), readType = Some('A'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result3 = new TmvTable(null).filterValidRows(rdd3)
    Assert.assertEquals(1, result3.count())

    val rdd4 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Tmv(ammissibilita = None, readType = Some('A'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result4 = new TmvTable(null).filterValidRows(rdd4)
    Assert.assertEquals(0, result4.count())

    val rdd5 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Tmv(ammissibilita = Some("OK"), readType = Some('S'),
        service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false)
    )
    val result5 = new TmvTable(null).filterValidRows(rdd5)
    Assert.assertEquals(0, result5.count())
  }

}
