package it.sferanet.au.controller.dal.prestazionale

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.dal.prestazionale.FDDTable
import it.sferanet.au.model.prestazionale.FDD
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

import java.text.SimpleDateFormat

class FDDTableTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy")
  val dateFormat2: SimpleDateFormat = Constants.FORMAT_DATE_LOAD

  def chekNewEff(obj: FDD): Unit = {
    Assert.assertTrue(obj.service == "FDD")
    Assert.assertTrue(obj.pdr == "00108700079854")
    Assert.assertTrue(obj.readType.get == 'E')
    //    Assert.assertTrue(dateFormat.format(obj.date.get) == "01/04/2021")
    //    Assert.assertTrue(obj.measure.get == 100.0)
    //    Assert.assertTrue(obj.converted.get== 105.0)
    Assert.assertTrue(obj.serialNumberMis.isEmpty)
    Assert.assertTrue(obj.serialNumberConv.isEmpty)
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_02221101203/2021/0409/12883450152_02221101203_202104_FDD_20210409023321_409_M.zip")
    //fixme Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2021-03-06T06:54:03.289817")
    Assert.assertTrue(obj.isNewRoute)
    Assert.assertTrue(obj.raccolta.get == "S")
  }

  def chekNewSost(obj: FDD): Unit = {
    Assert.assertTrue(obj.service == "FDD")
    Assert.assertTrue(obj.pdr == "15810000024808")
    Assert.assertTrue(obj.readType.get == 'E')
    Assert.assertTrue(dateFormat.format(obj.date.get) == "01/04/2021")
    Assert.assertTrue(obj.measure.get == 101.0)
    Assert.assertTrue(obj.converted.get == 106.0)
    Assert.assertTrue(obj.serialNumberMis.isEmpty)
    Assert.assertTrue(obj.serialNumberConv.isEmpty)
    Assert.assertTrue(obj.local_file.get == "/mnt/isilonshare_gas/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_02221101203/2021/0409/12883450152_02221101203_202104_FDD_20210409023321_409_M.zip")
    //fixme Assert.assertTrue(dateFormat2.format(obj.d_caricamento.get) == "2021-03-06T06:54:03.289817")
    Assert.assertTrue(obj.isNewRoute)
    Assert.assertTrue(obj.raccolta.get == "S")
  }

  @Test
  def testAppOnlyNewRoute(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = FDDTable("./src/test/resources/integrazione-ca/misure/fdd/new_route").get()
    val objEff: FDD = measures.filter(r => r.pdr == "00108700079854").take(1)(0)
    val objSost: FDD = measures.filter(r => r.pdr == "15810000024808").take(1)(0)

    chekNewEff(objEff)
    chekNewSost(objSost)

  }

  @Test
  def testAppONewRouteAndOld(): Unit = {
    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    val measures = FDDTable("./src/test/resources/integrazione-ca/misure/fdd/both_route").get()
    val objEff: FDD = measures.filter(r => r.pdr == "00108700079854").take(1)(0)
    val objSost: FDD = measures.filter(r => r.pdr == "15810000024808").take(1)(0)

    val old = measures.filter(o => o.isNewRoute == false).count()
    Assert.assertTrue(old == 0)
    chekNewEff(objEff)
    chekNewSost(objSost)

  }

  def testFilterValidRows(): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // rdd1, rdd2 OK
    // rdd3 KO
    val rdd1 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      FDD(readType = Some('E'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, raccolta = null)
    )
    val result1 = new FDDTable(null).filterValidRows(rdd1)
    Assert.assertEquals(1, result1.count())

    val rdd2 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      FDD(readType = Some('A'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, raccolta = null)
    )
    val result2 = new FDDTable(null).filterValidRows(rdd2)
    Assert.assertEquals(1, result2.count())

    val rdd3 = spark.sparkContext.parallelize(Seq(1)).map(r =>
      FDD(readType = Some('S'),
        ammissibilita = None, service = null, pdr = null, date = null, pivaDistr = null, pivaUtente = null,
        measure = null, converted = null, serialNumberMis = null, serialNumberConv = null, local_file = null,
        d_caricamento = null, isNewRoute = false, raccolta = null)
    )
    val result3 = new FDDTable(null).filterValidRows(rdd3)
    Assert.assertEquals(0, result3.count())
  }
}
