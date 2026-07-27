package it.sferanet.au.controller.dal

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.model.Flow
import it.sferanet.au.model.periodico.Tgl
import it.sferanet.au.model.prestazionale._
import it.sferanet.au.model.rettifica._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.text.SimpleDateFormat


class FlowTest extends EnvironmentSparkTest {

  def testFields(): Unit = {
    val format = new SimpleDateFormat("yyyy-mm-dd")

    val flowOld = Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(0),
      converted = Some(1), serialNumberMis = None, serialNumberConv = None,
      local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"), motivation = Some(4), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)

    Assert.assertEquals(20200110, flowOld.dateLoadFromLocalFile)
    Assert.assertEquals("20200109151501", Constants.FORMAT_DATE_CLOUD_FILENAME.format(flowOld.timestampLocalFile))
    Assert.assertEquals(1, flowOld.progressiveLocalFile)
    Assert.assertEquals("00489490011_12420101003_201912_RGL0050_20200109151501_1.XML", flowOld.fileNameLocalFile)
    Assert.assertEquals(false, flowOld.isNewRoute)


    val flowNew = Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(0),
      converted = Some(1), serialNumberMis = None, serialNumberConv = None,
      local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_RGL0050_20200109151501_1_M.xml"), motivation = Some(4), d_caricamento = None, isNewRoute = true, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)

    Assert.assertEquals(20200110, flowNew.dateLoadFromLocalFile)
    Assert.assertEquals("20200109151501", Constants.FORMAT_DATE_CLOUD_FILENAME.format(flowNew.timestampLocalFile))
    Assert.assertEquals(1, flowNew.progressiveLocalFile)
    Assert.assertEquals("00489490011_12420101003_201912_RGL0050_20200109151501_1_M.XML", flowNew.fileNameLocalFile)
    Assert.assertEquals(true, flowNew.isNewRoute)


    val flowIGMG = IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
      converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = Some(2), cau_int_cor = Some(2),
      local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_IGMG_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None)

    Assert.assertEquals(20200110, flowIGMG.dateLoadFromLocalFile)
    Assert.assertEquals("20200109151501", Constants.FORMAT_DATE_CLOUD_FILENAME.format(flowIGMG.timestampLocalFile))
    Assert.assertEquals(1, flowIGMG.progressiveLocalFile)
    Assert.assertEquals("00489490011_12420101003_201912_IGMG_20200109151501_1.xml", flowIGMG.fileNameLocalFile)
    Assert.assertEquals(true, flowIGMG.isNewRoute)

  }

  def testOrderings(): Unit = {
    val format = new SimpleDateFormat("yyyy-mm-dd")

    val flows1 = List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = None, serialNumberConv = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_RGL0050_20200109151501_1_M.xml"),
        motivation = Some(4), d_caricamento = None, isNewRoute = true, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = None, serialNumberConv = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0111/00489490011_12420101003_201912_RGL0050_20200109151501_1_M.xml"),
        motivation = Some(4), d_caricamento = None, isNewRoute = true, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )

    Assert.assertEquals(20200111, flows1.sorted(Flow.priorityOrderingFlows).last.dateLoadFromLocalFile)

    val flows2 = List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = None, serialNumberConv = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_RGL0050_20200109151501_1_M.xml"),
        motivation = Some(4), d_caricamento = None, isNewRoute = true, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = None, serialNumberConv = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_RGL0050_20200109151502_1_M.xml"),
        motivation = Some(4), d_caricamento = None, isNewRoute = true, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )

    Assert.assertEquals("20200109151502", Constants.FORMAT_DATE_CLOUD_FILENAME.format(flows2.sorted(Flow.priorityOrderingFlows).last.timestampLocalFile))

    val flows3 = List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = None, serialNumberConv = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_RGL0050_20200109151501_1.xml"),
        motivation = Some(4), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = None, serialNumberConv = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_RGL0050_20200109151501_1_M.xml"),
        motivation = Some(4), d_caricamento = None, isNewRoute = true, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )

    Assert.assertEquals(false, flows3.sorted(Flow.priorityOrderingFlows).last.isNewRoute)

    val flows4 = List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = None, serialNumberConv = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_RGL0050_20200109151501_1_M.xml"),
        motivation = Some(4), d_caricamento = None, isNewRoute = true, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = None, serialNumberConv = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1_M.xml"),
        d_caricamento = None, isNewRoute = true, readType = None, isValid = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )

    Assert.assertEquals("TGL", flows4.sorted(Flow.priorityOrderingFlows).last.service)

    val flows5 = List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = None, serialNumberConv = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_RGL0050_20200109151501_2_M.xml"),
        motivation = Some(4), d_caricamento = None, isNewRoute = true, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = None, serialNumberConv = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_RGL0050_20200109151501_1_M.xml"),
        motivation = Some(4), d_caricamento = None, isNewRoute = true, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )

    Assert.assertEquals(2, flows5.sorted(Flow.priorityOrderingFlows).last.progressiveLocalFile)

    val flows6 = List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = None, serialNumberConv = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_RGL0050_20200109151501_1_M.xml"),
        motivation = Some(4), d_caricamento = None, isNewRoute = true, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(500),
        converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = Some(2), cau_int_cor = Some(2),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_IGMG_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-01")), readType = None, measure = Some(1500),
        converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = Some(2), cau_int_cor = Some(2),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_IGMG_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )

    Assert.assertEquals("RGL", flows6.sorted(Flow.priorityOrderingFlows).head.service)
    Assert.assertEquals("IGMGPRE", flows6.sorted(Flow.priorityOrderingFlows).tail.head.service)
    Assert.assertEquals("IGMGPOST", flows6.sorted(Flow.priorityOrderingFlows).last.service)


    val flows7 = List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2020-01-01")), measure = Some(0),
        converted = Some(1), serialNumberMis = None, serialNumberConv = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_RGL0050_20200109151501_1_M.xml"),
        motivation = Some(4), d_caricamento = None, isNewRoute = true, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(format.parse("2020-01-02")), readType = None, measure = Some(500),
        converted = Some(1000), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = Some(2), cau_int_cor = Some(2),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_IGMG_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(format.parse("2020-01-02")), readType = None, measure = Some(1500),
        converted = Some(2000), serialNumberMis = Some("100"), serialNumberConv = Some("200"), coefCorr = None, cau_int_mis = Some(2), cau_int_cor = Some(2),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_IGMG_20200109151501_1.xml"), d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )

    Assert.assertEquals("RGL", flows7.sorted(Flow.temporalOrderingFlows).head.service)
    Assert.assertEquals("IGMGPRE", flows7.sorted(Flow.temporalOrderingFlows).tail.head.service)
    Assert.assertEquals("IGMGPOST", flows7.sorted(Flow.temporalOrderingFlows).last.service)

  }

  def testFlowFilter(): Unit = {
    val sqlContext = Environment.getSqlContext
    import sqlContext.implicits._

    // TGL e RGL
    val data1 = Seq(("EE", "NON BLOCCANTE"), ("022000", "NON BLOCCANTE"), ("052021", "OK"), ("062021", null), ("072021", "BLOCCANTE"), ("072025", "NON BLOCCANTE"), ("082026", "BLOCCANTE"))
    val rdd1 = Environment.getSparkContext.parallelize(data1)
    val df1 = rdd1.toDF("mese_comp", "ammissibilita")

    Environment.setProperty("flow.read.startDate", "202101")
    Environment.setProperty("flow.read.endDate", "202110")
    val output1 = df1.filter(Flow.flowFilter("mese_comp", col("mese_comp"), col("ammissibilita")))

    Assert.assertEquals(2, output1.count())
    Assert.assertEquals(1, output1.filter(col("mese_comp") === "052021").count())
    Assert.assertEquals(1, output1.filter(col("mese_comp") === "062021").count())
    Assert.assertEquals(0, output1.filter(col("mese_comp") === "EE").count())
    Assert.assertEquals(2, output1.filter(!(col("ammissibilita") <=> "BLOCCANTE")).count())
    Assert.assertEquals(0, output1.filter(col("ammissibilita") === "BLOCCANTE").count())


    // flussi diversi da TGL e RGL
    val data = Seq(("EE", "NON BLOCCANTE"), ("200002", "NON BLOCCANTE"), ("202105", "OK"), ("202106", null), ("202107", "BLOCCANTE"), ("202507", "NON BLOCCANTE"), ("202608", "BLOCCANTE"))
    val rdd = Environment.getSparkContext.parallelize(data)
    val df = rdd.toDF("annomese", "ammissibilita")

    Environment.setProperty("flow.read.startDate", "202101")
    Environment.setProperty("flow.read.endDate", "202110")
    val output2 = df.filter(Flow.flowFilter("annomese", col("annomese"), col("ammissibilita")))

    Assert.assertEquals(2, output2.count())
    Assert.assertEquals(1, output2.filter(col("annomese") === "202105").count())
    Assert.assertEquals(1, output2.filter(col("annomese") === "202106").count())
    Assert.assertEquals(0, output2.filter(col("annomese") === "EE").count())
    Assert.assertEquals(2, output2.filter(!(col("ammissibilita") <=> "BLOCCANTE")).count())
    Assert.assertEquals(0, output2.filter(col("ammissibilita") === "BLOCCANTE").count())

  }

}
