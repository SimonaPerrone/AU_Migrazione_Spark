package it.eng.au.sbg.controller

import it.eng.au.aggiustamentoGas.controller.JoinInfoController
import it.eng.au.aggiustamentoGas.model.agg.{FlowWithInfo, MonthTreatment}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Rml, Tal, Tav, Tml}
import it.eng.au.aggiustamentoGas.model.rcugas.{RcuGasConnessioniDistr2, RcuGasMassivoP, RcuGasSuspendedPdr, RcuGasTech, RcuGasVarPrelAnnuoP, RcuGasVarProfiloP}
import it.eng.au.aggiustamentoGas.utility.constants.Treatment
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.sbg.EnvironmentSparkTest
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class JoinInfoControllerSbgTest extends EnvironmentSparkTest {
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
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = Some('A'), tipoRettifica = None),

      Tav(service = "TAV", pdr = "2", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(22), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200110151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), outcome = None)
    )).asInstanceOf[RDD[Flow]]

    val monthTreatment = Environment.getSpark.sparkContext.parallelize(List(
      MonthTreatment(pdr = "1", month = "202102", treatment = "M", calcmode = "rcugas", autofilled = false)
      , MonthTreatment(pdr = "1", month = "202103", treatment = "M", calcmode = "rcugas", autofilled = false)
      , MonthTreatment(pdr = "2", month = "202103", treatment = "G", calcmode = "rcugas", autofilled = false)
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
        tPreConv = None, nCoeffCorr = None)
    ))

    val rcuGasVarProfiloP = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasVarProfiloP(nIdPdr = "a", dataInizio = formatter.parseDateTime("24-02-2021"),
        dataFine = formatter.parseDateTime("26-02-2021"), tCodProfilo = Some("CCCC"))
    ))

    val rcuGasVarPrelAnnuoP = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasVarPrelAnnuoP(nIdPdr = "a", dataInizio = formatter.parseDateTime("24-02-2021"),
        dataFine = formatter.parseDateTime("26-02-2021"), nPrelivevoAnnuo = Some(1.0))
    ))

    val join = new JoinInfoControllerSbg().get(measures, monthTreatment, rcuGasMassivoP, rcuGasConnessioniDistr2, rcuGasSospensioni, rcuGasTech, rcuGasVarPrelAnnuoP, rcuGasVarProfiloP, "202102", "202104").cache()
    val result = join.values.flatMap(f => f._1).cache()
    join.collect.foreach(println)
    result.collect.foreach(println)

    Assert.assertEquals(1, result.filter(_.flow.measure == Some(2)).count)
    Assert.assertEquals(1, result.filter(_.flow.measure == Some(3)).count)
    Assert.assertEquals(1, result.filter(_.flow.measure == Some(5)).count)
    Assert.assertEquals(1, result.filter(_.flow.measure == Some(6)).count)
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

    Assert.assertEquals(0, result.filter(_.flow.pdr == Some(2)).count)

  }

  def testGetAutoletture(): Unit = {
    Environment.setProperty("period.read.endDate", "202103")
    Environment.setProperty("period.read.startDate", "202103")

    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val measures = Environment.getSpark.sparkContext.parallelize(List(
      Tav(service = "TAV", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200110151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), outcome = None)
      , Rml(service = "RML", pdr = "1", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = None, tipoRettifica = None)

      , Tav(service = "TAV", pdr = "2", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200110151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), outcome = None)
      , Rml(service = "RML", pdr = "2", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(4), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = None, tipoRettifica = None)

      , Tav(service = "TAV", pdr = "3", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(5), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200110151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), outcome = None)
      , Rml(service = "RML", pdr = "3", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(6), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = Some('A'), tipoRettifica = None)

      , Tav(service = "TAV", pdr = "4", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(7), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200110151501_1.xml"),
        dataCaricamento = Some(formatter.parseDateTime("24-02-2021")), outcome = None)
      , Rml(service = "RML", pdr = "4", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(8), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200111151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021")), freqLet = None, readType = Some('A'), tipoRettifica = None)
    )).asInstanceOf[RDD[Flow]]

    val monthTreatment = Environment.getSpark.sparkContext.parallelize(List(
      MonthTreatment(pdr = "1", month = "202102", treatment = "G", calcmode = "rcugas", autofilled = false)
      , MonthTreatment(pdr = "1", month = "202103", treatment = "M", calcmode = "rcugas", autofilled = false)
      , MonthTreatment(pdr = "2", month = "202102", treatment = "M", calcmode = "rcugas", autofilled = false)
      , MonthTreatment(pdr = "2", month = "202103", treatment = "G", calcmode = "rcugas", autofilled = false)
      , MonthTreatment(pdr = "3", month = "202102", treatment = "M", calcmode = "rcugas", autofilled = false)
      , MonthTreatment(pdr = "3", month = "202103", treatment = "G", calcmode = "rcugas", autofilled = false)
      , MonthTreatment(pdr = "4", month = "202102", treatment = "M", calcmode = "rcugas", autofilled = false)
      , MonthTreatment(pdr = "4", month = "202103", treatment = "M", calcmode = "rcugas", autofilled = false)
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
        tPreConv = None, nCoeffCorr = None)
    ))

    val rcuGasVarProfiloP = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasVarProfiloP(nIdPdr = "a", dataInizio = formatter.parseDateTime("24-02-2021"),
        dataFine = formatter.parseDateTime("26-02-2021"), tCodProfilo = Some("CCCC"))
    ))

    val rcuGasVarPrelAnnuoP = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasVarPrelAnnuoP(nIdPdr = "a", dataInizio = formatter.parseDateTime("24-02-2021"),
        dataFine = formatter.parseDateTime("26-02-2021"), nPrelivevoAnnuo = Some(1.0))
    ))

    val join = new JoinInfoControllerSbg().get(measures, monthTreatment, rcuGasMassivoP, rcuGasConnessioniDistr2, rcuGasSospensioni, rcuGasTech, rcuGasVarPrelAnnuoP, rcuGasVarProfiloP, "202103", "202103").cache()
    val result = join.values.flatMap(f => f._1).cache()
    join.collect.foreach(println)
    result.collect.foreach(println)


  }

  def testGetAutolettureRealCase(): Unit = {

    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val measure1 = Tml(service = "TML", pdr = "1", date = Some(formatter.parseDateTime("01-01-2022")),
      measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
      localFile = Some("/mnt/isilonshare_gas/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_12039890152/2022/0104/06724610966_12039890152_202112_TML_20220103232900_01_M.zip"),
      dataCaricamento = Some(formatter.parseDateTime("05-01-2022")), freqLet = None, readType = Some('E'), coefCorr = None)
    val measure2 = Tav(service = "TAV", pdr = "1", date = Some(formatter.parseDateTime("23-01-2022")),
      measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
      localFile = Some("/mnt/isilonshare_gas/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_12039890152/2022/0201/06724610966_12039890152_202201_TAL_20220131112640_01_M.zip"),
      dataCaricamento = Some(formatter.parseDateTime("02-02-2022")), outcome = None)
    val measure3 = Tml(service = "TML", pdr = "1", date = Some(formatter.parseDateTime("31-01-2022")),
      measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
      localFile = Some("/mnt/isilonshare_gas/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_12039890152/2022/0204/06724610966_12039890152_202201_TML_20220204093900_01_M.zip"),
      dataCaricamento = Some(formatter.parseDateTime("05-02-2022")), freqLet = None, readType = Some('E'), coefCorr = None)

    val monthTreatment = Environment.getSpark.sparkContext.parallelize(List(MonthTreatment(pdr = "1", month = "202201", treatment = "G", calcmode = "rcugas", autofilled = false)))

    val measures = Environment.getSpark.sparkContext.parallelize(List(
      measure1
      , measure2
      , measure3
    )).asInstanceOf[RDD[Flow]]
    val joinInfoController = new JoinInfoControllerSbg
    val groupedMeasures = joinInfoController.groupMeasuresAndSort(measures, "202201", "202201").cache()
    val measuresWithTreatment = joinInfoController.setFlowTreatment(groupedMeasures, monthTreatment).cache()

    val result = joinInfoController.filterAutoletturaFlow(measuresWithTreatment.map(m => (m._1, (m._2, null))), "202201", "202201").cache()

    result.values.collect.foreach(a => println(a._1.mkString("\n")))

    Assert.assertEquals(2, result.values.collect.head._1.size)
    Assert.assertFalse(result.values.collect.head._1.exists(_.flow.isInstanceOf[Tav]))
  }

  def testGetAutolettureOnlyAutolettureMeasure(): Unit = {

    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val measure = Tav(service = "TAV", pdr = "1", date = Some(formatter.parseDateTime("23-01-2022")),
      measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
      localFile = Some("/mnt/isilonshare_gas/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_12039890152/2022/0201/06724610966_12039890152_202201_TAL_20220131112640_01_M.zip"),
      dataCaricamento = Some(formatter.parseDateTime("02-02-2022")), outcome = None)

    val monthTreatment = Environment.getSpark.sparkContext.parallelize(List(MonthTreatment(pdr = "1", month = "202201", treatment = "G", calcmode = "rcugas", autofilled = false)))

    val measures = Environment.getSpark.sparkContext.parallelize(List(
      measure
    )).asInstanceOf[RDD[Flow]]
    val joinInfoController = new JoinInfoControllerSbg
    val groupedMeasures = joinInfoController.groupMeasuresAndSort(measures, "202201", "202201").cache()
    val measuresWithTreatment = joinInfoController.setFlowTreatment(groupedMeasures, monthTreatment).cache()

    val result = joinInfoController.filterAutoletturaFlow(measuresWithTreatment.map(m => (m._1, (m._2, null))), "202201", "202201").cache()

    Assert.assertEquals(0, result.values.collect.head._1.size)
    Assert.assertFalse(result.values.collect.head._1.exists(_.flow.isInstanceOf[Tav]))
  }
}
