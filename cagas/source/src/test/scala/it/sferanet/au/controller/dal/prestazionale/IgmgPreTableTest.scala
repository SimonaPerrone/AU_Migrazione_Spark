package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.{IgmgPostTable, IgmgPreTable}
import it.sferanet.au.model.prestazionale.{IgmgPost, IgmgPre}
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class IgmgPreTableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  def checkNewRouteObj(obj: IgmgPre): Unit = {
    Assert.assertTrue(obj.isNewRoute) //se nuovo flusso (std)
    Assert.assertTrue(obj.service == "IGMGPRE") //flusso
    Assert.assertTrue(obj.d_caricamento.get.compareTo(Constants.FORMAT_DATE_LOAD.parse("2021-04-01T03:16:14.918000")) == 0) //d_caricamento
    Assert.assertTrue(obj.converted.getOrElse(-1) == (-1)) //letTotConv converted measure
    Assert.assertTrue(obj.measure.getOrElse(-1) == 46041) //letTotPrel measure
    Assert.assertTrue(obj.pdr == "03160000253928") //cod_pdr
    Assert.assertTrue(obj.local_file.getOrElse("") == "/mnt/isilonshare_gas/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_06655971007/2021/0331/06724610966_06655971007_202103_IGMG_20210331153020_00.zip") //local_file

    Assert.assertTrue(obj.serialNumberConv.getOrElse("") == "") //matr_conv matricola convertitore
    Assert.assertTrue(obj.serialNumberMis.getOrElse("") == "541707") //matr_mis matricola misuratore
    Assert.assertTrue(obj.date.get.compareTo(Constants.STANDARD_FORMAT_DATE.parse("08/10/2010")) == 0) //data_comp

    Assert.assertTrue(obj.cau_int_cor.getOrElse(-1) == (-1))
    Assert.assertTrue(obj.cau_int_mis.getOrElse(-1) == 1)
    Assert.assertTrue(obj.readType.getOrElse("") == 'E') //tipoLettura
    Assert.assertTrue(obj.coefCorr.getOrElse(-1.0) == 1.014352) //coefficiente di correzione POST
  }


  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = IgmgPreTable("./src/test/resources/integrazione-ca/misure/igmgPre").get()
    val obj: IgmgPre = measures.take(1)(0)
    checkNewRouteObj(obj)

  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      IgmgPre(readType = Some('S'), cau_int_cor = Some(1), cau_int_mis = None,
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, coefCorr = null)
    )
    val result1 = new IgmgPreTable(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      IgmgPre(readType = Some('E'), cau_int_cor = None, cau_int_mis = Some(1),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, coefCorr = null)
    )
    val result2 = new IgmgPreTable(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      IgmgPre(readType = Some('E'), cau_int_cor = None, cau_int_mis = None,
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, coefCorr = null)
    )
    val result3 = new IgmgPreTable(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())
  }

}
