package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.{FDDTable, Im1PostTable}
import it.sferanet.au.model.prestazionale.{FDD, Im1Post}
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class Im1PostTableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  def checkOldRouteObj(obj: Im1Post): Unit = {
    Assert.assertTrue(!obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "IM1POST") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2020-04-08T09:58:25.137973")) == 0) //d_caricamento
    Assert.assertTrue(obj.measure.getOrElse(-1) == 0) //letMisuratore
    Assert.assertTrue(obj.converted.getOrElse(-1) == (-1)) //letCorrettore
    Assert.assertTrue(obj.pdr == "05260200843562") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2020/0407/12883450152_12883420155_201307_IM10306_20200407095714_121.xml") //local_file

    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "8122586") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("19/07/2013")) == 0) //data_comp

    Assert.assertTrue(obj.readType.getOrElse("") == 'E') //tipo_lettura
    Assert.assertTrue(obj.coefCorr.getOrElse(-1) == 1.028073)
    Assert.assertTrue(obj.cau_int_mis.getOrElse(-1) == 6)
    Assert.assertTrue(obj.cau_int_cor.getOrElse(-1) == (-1))
  }


  @Test
  def testOnlyOldRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = Im1PostTable("./src/test/resources/integrazione-ca/misure/im1post/old_route").get()
    val obj: Im1Post = measures.take(1)(0)
    checkOldRouteObj(obj)
  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Im1Post(readType = Some('E'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, coefCorr = null, cau_int_cor = None, cau_int_mis =  None)
    )
    val result1 = new Im1PostTable(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Im1Post(readType = Some('S'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, coefCorr = null, cau_int_cor = None, cau_int_mis =  None)
    )
    val result2 = new Im1PostTable(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      Im1Post(readType = Some('A'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, coefCorr = null, cau_int_cor = None, cau_int_mis =  None)
    )
    val result3 = new Im1PostTable(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())
  }

}
