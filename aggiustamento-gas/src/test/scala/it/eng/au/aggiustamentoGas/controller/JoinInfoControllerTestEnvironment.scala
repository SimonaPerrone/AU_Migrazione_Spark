package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.utility.constants.Treatment
import it.eng.au.aggiustamentoGas.model.agg.{ExternalDailyInfo, FlowWithInfo, MonthTreatment}
import it.eng.au.aggiustamentoGas.model.measure._
import it.eng.au.aggiustamentoGas.model.rcugas._
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class JoinInfoControllerTestEnvironment extends EnvironmentSparkTest {/*
  def testGet(): Unit = {
    Environment.setProperty("period.read.endDate", "202201")
    Environment.setProperty("period.read.startDate", "202012")

    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val measures = Environment.getSpark.sparkContext.parallelize(List(
      Tal(service = "TAL", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), outcome = None),
      Tav(service = "TAV", pdr = "1", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200110151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), outcome = None),

      Rml(service = "RML", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = None, tipoRettifica = None),
      Rml(service = "RML", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(4), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = Some('A'), tipoRettifica = None),

      Rml(service = "RML", pdr = "1", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(5), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = Some('A'), tipoRettifica = None),
      Rml(service = "RML", pdr = "1", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(6), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = Some('S'), tipoRettifica = None),

      Rml(service = "RML", pdr = "1", date = Some(formatter.parseDateTime("26-04-2021")),
        measure = Some(7), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = Some('A'), tipoRettifica = None),

      Rml(service = "RML", pdr = "1", date = Some(formatter.parseDateTime("26-04-2020")),
        measure = Some(8), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = Some('A'), tipoRettifica = None)

    )).asInstanceOf[RDD[Flow]]

    val monthTreatment = Environment.getSpark.sparkContext.parallelize(List(
      MonthTreatment(pdr = "1", month = "202102", treatment = "G", calcmode = "infered", autofilled = false),
      MonthTreatment(pdr = "1", month = "202103", treatment = "M", calcmode = "infered", autofilled = false)
    ))

    val rcuGasMassivoP = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasMassivoP(nIdPdr = "a", tCodicePdr = "1", startDate = formatter.parseDateTime("23-02-2021"), endDate = formatter.parseDateTime("24-02-2021"),
        tTrattamento = Treatment.N, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None),
      RcuGasMassivoP(nIdPdr = "a", tCodicePdr = "1", startDate = formatter.parseDateTime("24-02-2021"), endDate = formatter.parseDateTime("27-02-2021"),
        tTrattamento = Treatment.N, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None),
      RcuGasMassivoP(nIdPdr = "a", tCodicePdr = "1", startDate = formatter.parseDateTime("27-02-2021"), endDate = formatter.parseDateTime("28-02-2021"),
        tTrattamento = Treatment.N, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None),
      RcuGasMassivoP(nIdPdr = "a", tCodicePdr = "1", startDate = formatter.parseDateTime("26-04-2021"), endDate = formatter.parseDateTime("28-04-2021"),
        tTrattamento = Treatment.N, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None),
      RcuGasMassivoP(nIdPdr = "a", tCodicePdr = "1", startDate = formatter.parseDateTime("28-04-2021"), endDate = formatter.parseDateTime("30-04-2021"),
        tTrattamento = Treatment.N, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
    ))

    val rcuGasConnessioniDistr2 = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasConnessioniDistr2(tCodicePdr = "1", dataInizioConn = formatter.parseDateTime("24-02-2021"), dataFineConn = formatter.parseDateTime("26-02-2021"), idRegioneClimatica = Some(1), nIdDistr = "", tRemi = "")
    ))

    val rcuGasSospensioni = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasSuspendedPdr(nIdPdr = "a", dataIniSosp = formatter.parseDateTime("24-02-2021"), dataFineSosp = formatter.parseDateTime("26-02-2021"))
    ))

    val rcuGasTech = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasTech(nIdPdr = "a", startDateTech = formatter.parseDateTime("24-02-2021"),
        endDateTech = formatter.parseDateTime("26-02-2021"), gruppoMisInt = None,
        tPreConv = None, nCoeffCorr = None, nCifreMis = None, nCifreConv = None)
    ))

    val rcuGasVarProfiloP = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasVarProfiloP(nIdPdr = "a", dataInizio = formatter.parseDateTime("24-02-2021"),
        dataFine = formatter.parseDateTime("26-02-2021"), tCodProfilo = Some("CCCC"))
    ))

    val rcuGasVarPrelAnnuoP = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasVarPrelAnnuoP(nIdPdr = "a", dataInizio = formatter.parseDateTime("24-02-2021"),
        dataFine = formatter.parseDateTime("26-02-2021"), nPrelivevoAnnuo = Some(1.0))
    ))

    val join = new JoinInfoController().get(measures, monthTreatment, rcuGasMassivoP, rcuGasConnessioniDistr2, rcuGasSospensioni, rcuGasTech, rcuGasVarPrelAnnuoP, rcuGasVarProfiloP, "202102", "202104").cache()
    val result = join.values.flatMap(f => f._1).cache()
    join.collect.foreach(println)
    result.collect.foreach(println)

    Assert.assertEquals(5, result.count)

    Assert.assertEquals(1, result.filter(_.flow.measure == Some(2)).count)
    Assert.assertEquals(1, result.filter(_.flow.measure == Some(3)).count)
    Assert.assertEquals(1, result.filter(_.flow.measure == Some(5)).count)
    Assert.assertEquals(0, result.filter(_.flow.measure == Some(6)).count)
    Assert.assertEquals(1, result.filter(_.flow.measure == Some(7)).count)

    Assert.assertTrue(result.filter(_.flow.measure == Some(7)).first.monthTreatment.isEmpty)

    Assert.assertTrue(result.filter(_.flow.measure == Some(2)).first.rcuGasTech.isEmpty)
    Assert.assertTrue(result.filter(_.flow.measure == Some(3)).first.rcuGasTech.isDefined)

    Assert.assertTrue(result.filter(_.flow.measure == Some(2)).first.rcuGasVarProfilo.isEmpty)
    Assert.assertTrue(result.filter(_.flow.measure == Some(3)).first.rcuGasVarProfilo.isDefined)

    Assert.assertTrue(result.filter(_.flow.measure == Some(2)).first.idRegioneClimatica.isEmpty)
    Assert.assertTrue(result.filter(_.flow.measure == Some(3)).first.idRegioneClimatica.isDefined)

    Assert.assertEquals("26-04-2020", result.first().flow.date.get.toString("dd-MM-yyyy"))
    Assert.assertEquals("26-04-2021", result.collect.last.flow.date.get.toString("dd-MM-yyyy"))

    Assert.assertEquals(5, join.filter(_._1 == "1").first._2._2.rcuGasMassivoPList.get.size)
  }

  def testGroupMeasuresAndSort(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val measures = Environment.getSpark.sparkContext.parallelize(List(
      Rml(service = "RML", pdr = "1", date = Some(formatter.parseDateTime("26-11-2020")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = None, tipoRettifica = None),
      Rml(service = "RML", pdr = "1", date = Some(formatter.parseDateTime("26-12-2020")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = Some('A'), tipoRettifica = None),
      Rml(service = "RML", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = Some('A'), tipoRettifica = None),
      Rml(service = "RML", pdr = "1", date = Some(formatter.parseDateTime("26-05-2021")),
        measure = Some(4), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-03-2021")), freqLet = None, readType = Some('A'), tipoRettifica = None),
      Rml(service = "RML", pdr = "1", date = Some(formatter.parseDateTime("26-06-2021")),
        measure = Some(5), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = Some('A'), tipoRettifica = None)

    )).asInstanceOf[RDD[Flow]]

    val result = new JoinInfoController().groupMeasuresAndSort(measures, "202102", "202103").cache
    result.collect.foreach(println)

    Assert.assertEquals(3, result.filter(_._1 == "1").flatMap(_._2).count())
    Assert.assertEquals(List(2.0, 3.0, 4.0), result.filter(_._1 == "1").first()._2.map(_.flow.measure.get))
  }

  def testSetRcuGasConnessioniDistr2(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val measureRDD = Environment.getSpark.sparkContext.parallelize(List(
      Tml(service = "TML", pdr = "02500000061790", date = Some(formatter.parseDateTime("30-10-2020")),
        measure = Some(8), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_X/DISTRIBUTORE/TMG_X_Y/2020/0110/X_Y_201912_TML0050_20200111151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = Some('A'),
        coefCorr = None, isValid = None),
      Tml(service = "TML", pdr = "02500000061790", date = Some(formatter.parseDateTime("01-01-2021")),
        measure = Some(8), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_X/DISTRIBUTORE/TMG_X_Y/2020/0110/X_Y_201912_TML0050_20200111151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = Some('A'),
        coefCorr = None, isValid = None)
    )).asInstanceOf[RDD[Flow]]
      .map(f => new FlowWithInfo(flow = f))
      .keyBy(_.flow.pdr)
      .groupByKey()
      .map({ case (k, list) => (k, (list.toList, ExternalDailyInfo())) })

    val conn2DistrRDD = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasConnessioniDistr2(tCodicePdr = "02500000061790", nIdDistr = "180309000000000621", tRemi = "34661501",
        dataInizioConn = formatter.parseDateTime("01-03-2018"),
        dataFineConn = formatter.parseDateTime("31-01-2020"), idRegioneClimatica = Some(24)),
      RcuGasConnessioniDistr2(tCodicePdr = "02500000061790", nIdDistr = "150601000000000307", tRemi = "34661501",
        dataInizioConn = formatter.parseDateTime("01-01-19070"),
        dataFineConn = formatter.parseDateTime("28-02-2018"), idRegioneClimatica = Some(24)),
      RcuGasConnessioniDistr2(tCodicePdr = "02500000061790", nIdDistr = "180309000000000621", tRemi = "34661501",
        dataInizioConn = formatter.parseDateTime("01-10-2020"),
        dataFineConn = formatter.parseDateTime("31-12-2700"), idRegioneClimatica = Some(24)),
      RcuGasConnessioniDistr2(tCodicePdr = "02500000061790", nIdDistr = "180309000000000621", tRemi = "34661500",
        dataInizioConn = formatter.parseDateTime("01-02-2020"),
        dataFineConn = formatter.parseDateTime("30-09-2020"), idRegioneClimatica = Some(24))
    ))

    val res = new JoinInfoController(null)
      .setRcuGasConnessioniDistr2(measureRDD, conn2DistrRDD)
      .filter({ case (key, (list, ei)) => key.equals("02500000061790") })
      .map({ case (key, (list, ei)) => ei })
      .collect()

    Assert.assertEquals(1, res.length)
    val ei = res.head

    val daysNr = DateUtility.daysBetween(formatter.parseDateTime("01-01-2020"), formatter.parseDateTime("30-09-2020"))
    val datesList = (0 until daysNr).toList.map(daysOffset => formatter.parseDateTime("30-09-2020").minusDays(daysOffset))
    datesList.foreach(d => {
      val rcuC2D = ei.findRcuGasConnessioniDistr2(d)
      Assert.assertTrue(rcuC2D.isDefined)
      Assert.assertEquals(Some(24), rcuC2D.get.idRegioneClimatica)
    })
  }*/
}
