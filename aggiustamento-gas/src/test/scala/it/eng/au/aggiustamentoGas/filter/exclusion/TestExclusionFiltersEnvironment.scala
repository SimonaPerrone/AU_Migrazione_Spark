package it.eng.au.aggiustamentoGas.filter.exclusion

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.model.agg.{DailyConsumption, ExternalDailyInfo, FlowWithInfo}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Rml, Tgl}
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasConnessioniDistr2
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class TestExclusionFiltersEnvironment extends EnvironmentSparkTest {

  def testExcludeFlows(): Unit = {
    Environment.setProperty("filter.exclusion.enabled", "true")
    val flowRDD: RDD[Flow] = Environment.getSpark.sparkContext.parallelize(
      List(
        Tgl(service = "TGL", pdr = "10", readType = None, date = None, measure = None, converted = None,
          serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None,
          localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        ),
        Rml(service = "RML", pdr = "11", date = None, measure = None, converted = None, serialNumberMis = None,
          pivaDistr = None, pivaUtente = None, serialNumberConv = None, motivation = None, dataCaricamento = None, freqLet = None, readType = None, tipoRettifica = None,
          localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        ),
        Tgl(service = "TGL", pdr = "12", readType = None, date = None, measure = None, converted = None,
          serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None,
          localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml")
        ),
        Rml(service = "RML", pdr = "13", date = None, measure = None, converted = None, serialNumberMis = None,
          pivaDistr = None, pivaUtente = None, serialNumberConv = None, motivation = None, dataCaricamento = None, freqLet = None, readType = None, tipoRettifica = None,
          localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml")
        )
      )
    )
    val exclusionFilterController = new ExclusionFilterController(isStrong = false)
    exclusionFilterController.exclusionFileDf.show(false)

    val filteredFLows = exclusionFilterController.excludeFlows(flowRDD).cache

    filteredFLows.collect.foreach(println)

    Assert.assertTrue(exclusionFilterController.isEnabled)
    Assert.assertEquals(0, filteredFLows.filter(_.localFile.getOrElse("-1").equals("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml")).count)
    Assert.assertEquals(1, filteredFLows.filter(_.pdr.equals("10")).count)
    Assert.assertEquals(0, filteredFLows.filter(_.pdr.equals("11")).count)

  }

  def testExcludeRemi(): Unit = {
    Environment.setProperty("rcugas.sqoop.date", "20210611")
    Environment.setProperty("filter.exclusion.enabled", "true")

    val date = DateTime.parse("11/06/2021", DateTimeFormat.forPattern("dd/MM/yyyy"))
    val conn2Distr = RcuGasConnessioniDistr2(tCodicePdr = "1", dataInizioConn = date, dataFineConn = date, nIdDistr = "", tRemi = "666", idRegioneClimatica = Some(1))
    val conn2DistrOne = RcuGasConnessioniDistr2(tCodicePdr = "2", dataInizioConn = date.minusYears(1), dataFineConn = date.plusYears(1), nIdDistr = "", tRemi = "111", idRegioneClimatica = Some(1))
    val conn2DistrTwo = RcuGasConnessioniDistr2(tCodicePdr = "3", dataInizioConn = date.minusYears(1), dataFineConn = date.plusYears(1), nIdDistr = "", tRemi = "222", idRegioneClimatica = Some(1))
    val conn2DistrThree = RcuGasConnessioniDistr2(tCodicePdr = "4", dataInizioConn = date.minusYears(1), dataFineConn = date.plusYears(1), nIdDistr = "", tRemi = "333", idRegioneClimatica = Some(1))

    val eiRemiForcing = ExternalDailyInfo(rcuGasConnessioniDistr2List = Option(Iterable(conn2Distr)))
    val eiNoRemiForcing = ExternalDailyInfo(rcuGasConnessioniDistr2List = Option(Iterable(conn2DistrOne)))
    val eiAnnoMeseForcingTwo = ExternalDailyInfo(rcuGasConnessioniDistr2List = Option(Iterable(conn2DistrTwo)))
    val eiAnnoMeseForcingThree = ExternalDailyInfo(rcuGasConnessioniDistr2List = Option(Iterable(conn2DistrThree)))

    val measures = Environment.getSpark.sparkContext.parallelize(
      List(("1", (List(FlowWithInfo(flow = Tgl(service = "TGL", pdr = "1", readType = None, date = None, measure = None, converted = None, serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = None))),
        eiRemiForcing)),
        ("2", (List(FlowWithInfo(flow = Tgl(service = "TGL", pdr = "2", readType = None, date = None, measure = None, converted = None, serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = None))),
          eiNoRemiForcing)),
        ("3", (List(FlowWithInfo(flow = Tgl(service = "TGL", pdr = "3", readType = None, date = None, measure = None, converted = None, serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = None))),
          eiAnnoMeseForcingTwo)),
        ("4", (List(FlowWithInfo(flow = Tgl(service = "TGL", pdr = "4", readType = None, date = None, measure = None, converted = None, serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = None))),
          eiAnnoMeseForcingThree)),
        ("5", (List(FlowWithInfo(flow = Tgl(service = "TGL", pdr = "5", readType = None, date = None, measure = None, converted = None, serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = None))),
          eiRemiForcing))
      ))

    val exclusionFilterController = new ExclusionFilterController(isStrong = false)
    exclusionFilterController.exclusionFileDf.show(false)
    val measuresWithInfo = exclusionFilterController.excludeRemiPdr(measures, "20210611")
    measuresWithInfo.collect.foreach(println)

    Assert.assertEquals(2, measuresWithInfo.count)
  }

  def testForceExclusion(): Unit = {
    Environment.setProperty("rcugas.sqoop.date", "20210611")
    Environment.setProperty("filter.forceExclusion.enabled", "true")

    val date = DateTime.parse("11/06/2021", DateTimeFormat.forPattern("dd/MM/yyyy"))
    val conn2Distr = RcuGasConnessioniDistr2(tCodicePdr = "1", dataInizioConn = date, dataFineConn = date, nIdDistr = "", tRemi = "666", idRegioneClimatica = Some(1))
    val conn2DistrOne = RcuGasConnessioniDistr2(tCodicePdr = "1", dataInizioConn = date.minusYears(1), dataFineConn = date.plusYears(1), nIdDistr = "", tRemi = "111", idRegioneClimatica = Some(1))
    val conn2DistrTwo = RcuGasConnessioniDistr2(tCodicePdr = "1", dataInizioConn = date.minusYears(1), dataFineConn = date.plusYears(1), nIdDistr = "", tRemi = "111", idRegioneClimatica = Some(1))
    val conn2DistrThree = RcuGasConnessioniDistr2(tCodicePdr = "1", dataInizioConn = date.minusYears(1), dataFineConn = date.plusYears(1), nIdDistr = "", tRemi = "111", idRegioneClimatica = Some(1))

    val eiRemiForcing = ExternalDailyInfo(rcuGasConnessioniDistr2List = Option(Iterable(conn2Distr)))
    val eiNoRemiForcing = ExternalDailyInfo(rcuGasConnessioniDistr2List = Option(Iterable(conn2DistrOne)))
    val eiAnnoMeseForcingTwo = ExternalDailyInfo(rcuGasConnessioniDistr2List = Option(Iterable(conn2DistrTwo)))
    val eiAnnoMeseForcingThree = ExternalDailyInfo(rcuGasConnessioniDistr2List = Option(Iterable(conn2DistrThree)))

    val dailyCons = Environment.getSpark.sparkContext.parallelize(
      List(
        (createNullDailyConsumption("1"), eiNoRemiForcing), //1, 111,           true
        (createNullDailyConsumption("2"), eiNoRemiForcing), //2, 111,           true
        (createNullDailyConsumption("3"), eiNoRemiForcing), //3, 111,           false
        (createNullDailyConsumption("4"), eiRemiForcing), //4, 666,           true
        (createNullDailyConsumption("5", Some("202201")), eiAnnoMeseForcingTwo), //5, 111, 202201    true
        (createNullDailyConsumption("6", Some("202202")), eiAnnoMeseForcingThree) //6, 111, 202202    false
      )
    )

    val eF = new ForceExclusionController(isStrong = false)
    eF.exclusionFileDf.show(false)
    val forcedExclusionCons = eF.forceExclusion(dailyCons, "20210611").keys

    Assert.assertTrue(forcedExclusionCons.filter(_.pdr.equals("1")).collect().head.forceExclusion)
    Assert.assertTrue(forcedExclusionCons.filter(_.pdr.equals("2")).collect().head.forceExclusion)
    Assert.assertFalse(forcedExclusionCons.filter(_.pdr.equals("3")).collect().head.forceExclusion)
    Assert.assertTrue(forcedExclusionCons.filter(_.pdr.equals("4")).collect().head.forceExclusion)
    Assert.assertTrue(forcedExclusionCons.filter(_.pdr.equals("5")).collect().head.forceExclusion)
    Assert.assertFalse(forcedExclusionCons.filter(_.pdr.equals("6")).collect().head.forceExclusion)
  }

  private def createNullDailyConsumption(pdr: String, annoMese: Option[String] = None) = new DailyConsumption(pdr = pdr
    , date = null
    , value = null
    , pprof = null
    , coefficient = null
    , ca = null
    , idRegClim = null
    , codProfStd = null
    , segnante = null
    , idFormula = 0
    , errorCode = 0
    , pivaDistr = null
    , pivaUdd = null
    , pivaUdb = null
    , pivaIt = null
    , pivaRdb = null
    , dtg = null
    , codRemi = null
    , tipoCliente = null
    , unitMisPrel = null
    , annoMese = annoMese
    , session = null
    , treatment = null
    , causale = null
    , isValid = true
    , leftMeasureLocalFile = null
    , rightMeasureLocalFile = null
    , tCodIstat = null
    , classeMisuratore = null
    , valueNotSterilized = null
  )


  def testTrim(): Unit = {
    Environment.setProperty("filter.exclusion.enabled", "true")
    val exclusionFilterController = new ExclusionFilterController(isStrong = false)
    exclusionFilterController.exclusionFileDf.show(false)
  }
}
