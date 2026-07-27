package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.model.measure._
import it.eng.au.aggiustamentoGas.model.rcugas.{RcuGasMassivoP, RcuGasVarTrattamentoP}
import it.eng.au.aggiustamentoGas.utility.constants.{Treatment, TreatmentCalcMode}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class TreatmentControllerTestEnvironment extends EnvironmentSparkTest {
  def testCalcMonthTreatment(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val measures = Environment.getSpark.sparkContext.parallelize(List(
      Rml(service = "RML", pdr = "1", readType = None, date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(1), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rgl(service = "RGL", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(4), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rgl(service = "RGL", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),

      Rml(service = "RML", pdr = "1", readType = Some(0), date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(1), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rgl(service = "RGL", pdr = "1", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rgl(service = "RGL", pdr = "1", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),

      Tml(service = "TML", pdr = "2", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        freqLet = Some(4), readType = Some('E'), isValid = Some("SI"), coefCorr = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rml(service = "RML", pdr = "2", readType = Some(0), date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(6), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),

      Tml(service = "TML", pdr = "2", date = Some(formatter.parseDateTime("30-04-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        freqLet = Some(100), readType = Some('E'), isValid = Some("SI"), coefCorr = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rml(service = "RML", pdr = "2", readType = Some(0), date = Some(formatter.parseDateTime("30-04-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(100), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),

      Rgl(service = "RGL", pdr = "3", date = Some(formatter.parseDateTime("01-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rml(service = "RML", pdr = "3", readType = Some(0), date = Some(formatter.parseDateTime("15-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(1), freqLet = Some(4), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),
      Rgl(service = "RGL", pdr = "3", date = Some(formatter.parseDateTime("28-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(3), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))),

      Tml(service = "TML", pdr = "4", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        freqLet = Some(4), readType = Some('E'), isValid = None, coefCorr = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))).setAmmissibilita(Some("OK")),

      Tgl(service = "TGL", pdr = "5", date = Some(formatter.parseDateTime("01-10-2020")), readType = Some('E'), isValid = Some("SI"), measure = Some(52726.0), converted = Some(42969.0), serialNumberMis = Some("26020552"),
        serialNumberConv = Some("2297"), pivaDistr = Some("03586470282"), pivaUtente = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_03586470282/DISTRIBUTORE/TMG_03586470282_08526440154/2020/1109/03586470282_08526440154_202010_TGL0050_20201109162411_114.xml"),
        dataCaricamento = Some(formatter.parseDateTime("11-10-2020"))).setAmmissibilita(None)

    ))

    val result = new TreatmentController().calcMonthTreatment(measures).cache()

    result.collect().foreach(println)

    Assert.assertEquals(7, result.count())

    Assert.assertEquals(Treatment.G, result.filter(f => (f._1._1, f._1._2) == ("1", "202102")).first._2)
    Assert.assertEquals(Treatment.Y, result.filter(f => (f._1._1, f._1._2) == ("1", "202103")).first._2)
    Assert.assertEquals(Treatment.Y, result.filter(f => (f._1._1, f._1._2) == ("2", "202103")).first._2)
    Assert.assertEquals(Treatment.N, result.filter(f => (f._1._1, f._1._2) == ("2", "202104")).first._2)
    Assert.assertEquals(Treatment.M, result.filter(f => (f._1._1, f._1._2) == ("3", "202102")).first._2)
    Assert.assertEquals(Treatment.M, result.filter(f => (f._1._1, f._1._2) == ("4", "202103")).first._2)
    Assert.assertEquals(Treatment.G, result.filter(f => (f._1._1, f._1._2) == ("5", "202010")).first._2)

  }

  def testCalcTreatment(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

      val measures = Environment.getSpark.sparkContext.parallelize(List(
        Rml(service = "RML", pdr = "1", readType = Some(2), date = Some(formatter.parseDateTime("01-09-2022")),
          measure = Some(0.0), converted = None, serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
          localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
          motivation = Some(2), freqLet = Some(1), tipoRettifica = Some("T"), dataCaricamento = Some(formatter.parseDateTime("28-01-2023"))).asInstanceOf[Flow]
      ))

    val result = new TreatmentController().calc(measures, "201901", "202212", TreatmentCalcMode.infered, Environment.getSpark.sparkContext.emptyRDD)

    result.collect.foreach(println)
  }

  def testFillEmptyMonthTreatment(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val measures = Environment.getSpark.sparkContext.parallelize(List(
      //Scenario Y misure parziali
      (("1", "202101"), Treatment.N),
      (("1", "202102"), Treatment.N),
      (("1", "202103"), Treatment.Y),
      (("1", "202104"), Treatment.N),
      (("1", "202105"), Treatment.N),
      (("1", "202106"), Treatment.Y),
      (("1", "202107"), Treatment.N),
      (("1", "202108"), Treatment.N),

      //Scenario M misure parziali
      (("2", "202101"), Treatment.N),
      (("2", "202102"), Treatment.N),
      (("2", "202103"), Treatment.M),
      (("2", "202104"), Treatment.N),
      (("2", "202105"), Treatment.N),
      (("2", "202106"), Treatment.M),
      (("2", "202107"), Treatment.N),
      (("2", "202108"), Treatment.N),

      //Scenario Y-->M
      (("3", "202101"), Treatment.N),
      (("3", "202102"), Treatment.N),
      (("3", "202103"), Treatment.Y),
      (("3", "202104"), Treatment.N),
      (("3", "202105"), Treatment.N),
      (("3", "202106"), Treatment.M),
      (("3", "202107"), Treatment.N),
      (("3", "202108"), Treatment.N),

      //Scenario M-->G
      (("4", "202101"), Treatment.M),
      (("4", "202102"), Treatment.N),
      (("4", "202103"), Treatment.N),
      (("4", "202104"), Treatment.G),
      (("4", "202105"), Treatment.N),
      (("4", "202106"), Treatment.N),
      (("4", "202107"), Treatment.Y),
      (("4", "202108"), Treatment.N),

      //trattamento G-->N-->G
      (("5", "202101"), Treatment.G),
      (("5", "202102"), Treatment.G),
      (("5", "202103"), Treatment.N),
      (("5", "202104"), Treatment.N),
      (("5", "202105"), Treatment.G),
      (("5", "202106"), Treatment.G),
      (("5", "202107"), Treatment.N),
      (("5", "202108"), Treatment.N),

      //trattamento G-->M
      (("6", "202101"), Treatment.G),
      (("6", "202102"), Treatment.G),
      (("6", "202103"), Treatment.N),
      (("6", "202104"), Treatment.N),
      (("6", "202105"), Treatment.M),
      (("6", "202106"), Treatment.M),
      (("6", "202107"), Treatment.N),
      (("6", "202108"), Treatment.N),

      //test no mesi
      (("7", "202101"), Treatment.N),
      (("7", "202103"), Treatment.N),
      (("7", "202104"), Treatment.N),
      (("7", "202106"), Treatment.Y),
      (("7", "202107"), Treatment.N)
    ))

    val formatter2 = DateTimeFormat.forPattern("yyyyMM")
    val startDate = formatter2.parseDateTime("202101")
    val endDate = formatter2.parseDateTime("202108")

    val result = new TreatmentController().fillEmptyMonthTreatment(measures, startDate, endDate).cache()

    result.collect().sortBy(_._1).foreach(println)

    //Scenario Y misure parziali
    Assert.assertEquals(8, result.filter(_._1._1 == "1").count())
    result.filter(_._1._1 == "1").collect().foreach(f => Assert.assertEquals(Treatment.Y, f._2))
    result.filter(f => f._1._1 == "1" && f._1._2 != "202103" && f._1._2 != "202106").collect().foreach(f => Assert.assertEquals(true, f._3))
    result.filter(f => f._1._1 == "1" && f._1._2 == "202103" && f._1._2 == "202106").collect().foreach(f => Assert.assertEquals(false, f._3))


    //Scenario M misure parziali
    Assert.assertEquals(8, result.filter(_._1._1 == "2").count())
    result.filter(_._1._1 == "2").collect().foreach(f => Assert.assertEquals(Treatment.M, f._2))

    //Scenario Y-->M
    Assert.assertEquals(8, result.filter(_._1._1 == "3").count())
    result.filter(f => f._1._1 == "3" && "202105" >= f._1._2).collect().foreach(f => Assert.assertEquals(Treatment.Y, f._2))
    result.filter(f => f._1._1 == "3" && "202106" <= f._1._2).collect().foreach(f => Assert.assertEquals(Treatment.M, f._2))

    //Scenario M-->G
    Assert.assertEquals(8, result.filter(_._1._1 == "4").count())
    result.filter(f => f._1._1 == "4" && "202103" >= f._1._2).collect().foreach(f => Assert.assertEquals(Treatment.M, f._2))
    result.filter(f => f._1._1 == "4" && "202104" == f._1._2).collect().foreach(f => Assert.assertEquals(Treatment.G, f._2))
    result.filter(f => f._1._1 == "4" && "202105" <= f._1._2).collect().foreach(f => Assert.assertEquals(Treatment.Y, f._2))

    //trattamento G-->N-->G
    Assert.assertEquals(8, result.filter(_._1._1 == "5").count())
    result.filter(f => f._1._1 == "5" && "202102" >= f._1._2).collect().foreach(f => Assert.assertEquals(Treatment.G, f._2))
    result.filter(f => f._1._1 == "5" && "202103" <= f._1._2 && "202104" >= f._1._2).collect().foreach(f => Assert.assertEquals(Treatment.N, f._2))
    result.filter(f => f._1._1 == "5" && "202105" <= f._1._2 && "202106" >= f._1._2).collect().foreach(f => Assert.assertEquals(Treatment.G, f._2))
    result.filter(f => f._1._1 == "5" && "202107" <= f._1._2).collect().foreach(f => Assert.assertEquals(Treatment.N, f._2))
    result.filter(f => f._1._1 == "5").collect().foreach(f => Assert.assertEquals(false, f._3))


    //trattamento G-->M
    Assert.assertEquals(8, result.filter(_._1._1 == "6").count())
    result.filter(f => f._1._1 == "6" && "202102" >= f._1._2).collect().foreach(f => Assert.assertEquals(Treatment.G, f._2))
    result.filter(f => f._1._1 == "6" && "202103" <= f._1._2).collect().foreach(f => Assert.assertEquals(Treatment.M, f._2))

    //test no mesi
    Assert.assertEquals(8, result.filter(_._1._1 == "7").count())
    result.filter(_._1._1 == "7").collect().foreach(f => Assert.assertEquals(Treatment.Y, f._2))
    result.filter(f => f._1._1 == "7" && "202102" == f._1._2).collect().foreach(f => Assert.assertEquals(true, f._3))

  }

  def testGetRcuTreatment(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val measures = Environment.getSpark.sparkContext.parallelize(List(
      Rml(service = "RML", pdr = "1", readType = None, date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(1), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))).setTreatment(Treatment.G),
      Rgl(service = "RGL", pdr = "1", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(4), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))).setTreatment(Treatment.G),

      Rml(service = "RML", pdr = "2", readType = None, date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(1), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))).setTreatment(Treatment.G),
      Rgl(service = "RGL", pdr = "2", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(4), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))).setTreatment(Treatment.Y),
      Rgl(service = "RGL", pdr = "2", date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))).setTreatment(Treatment.N),

      Rml(service = "RML", pdr = "3", readType = None, date = Some(formatter.parseDateTime("26-02-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(1), freqLet = Some(1), tipoRettifica = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))).setTreatment(Treatment.G),
      Rgl(service = "RGL", pdr = "3", date = Some(formatter.parseDateTime("15-02-2021")),
        measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(4), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))).setTreatment(Treatment.Y),
      Rgl(service = "RGL", pdr = "3", date = Some(formatter.parseDateTime("01-02-2021")),
        measure = Some(3), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        motivation = Some(6), dataCaricamento = Some(formatter.parseDateTime("26-02-2021"))).setTreatment(Treatment.N)

    ))

    val rcuGas = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasMassivoP(tCodicePdr = "1", startDate = formatter.parseDateTime("01-01-2019"), endDate = formatter.parseDateTime("01-01-2022"),
        tTrattamento = Treatment.Y, pivaUdd = None, nIdPdr = "1", tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None),
      RcuGasMassivoP(tCodicePdr = "1", startDate = formatter.parseDateTime("01-01-2019"), endDate = formatter.parseDateTime("01-01-2022"),
        tTrattamento = Treatment.Y, pivaUdd = None, nIdPdr = "1", tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
    ))

    val rcuGasTrattamento = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasVarTrattamentoP(codicePdr = "1", dataInizio = formatter.parseDateTime("01-01-2019"), dataFine = formatter.parseDateTime("01-01-2022"),
        tTrattamentoSettlement = Treatment.Y)
    ))

    val result = new TreatmentController().getRcuTreatment(measures, rcuGasTrattamento).cache()

    result.collect().foreach(println)

    Assert.assertEquals(3, result.count())

    Assert.assertEquals(Treatment.Y, result.filter(f => (f._1._1, f._1._2) == ("1", "202102")).first._2)
    Assert.assertEquals(Treatment.G, result.filter(f => (f._1._1, f._1._2) == ("2", "202102")).first._2)
    Assert.assertEquals(Treatment.N, result.filter(f => (f._1._1, f._1._2) == ("3", "202102")).first._2)
  }

  def testGetRcuTreatment2(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val measures: RDD[Flow] = Environment.getSpark.sparkContext.parallelize(List(
      Tml(service = "TML", pdr = "1", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        freqLet = Some(4), readType = Some('E'), isValid = Some("SI"), coefCorr = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021")))
      , Tml(service = "TML", pdr = "2", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        freqLet = Some(4), readType = Some('E'), isValid = Some("SI"), coefCorr = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021")))
      , Tml(service = "TML", pdr = "3", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        freqLet = Some(4), readType = Some('E'), isValid = Some("SI"), coefCorr = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021")))
      , Tml(service = "TML", pdr = "4", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        freqLet = Some(4), readType = Some('E'), isValid = Some("SI"), coefCorr = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021")))
      , Tml(service = "TML", pdr = "5", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        freqLet = Some(4), readType = Some('E'), isValid = Some("SI"), coefCorr = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021")))
      , Tml(service = "TML", pdr = "6", date = Some(formatter.parseDateTime("26-03-2021")),
        measure = Some(1), converted = Some(10), serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None,
        localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"),
        freqLet = Some(4), readType = Some('E'), isValid = Some("SI"), coefCorr = None, dataCaricamento = Some(formatter.parseDateTime("26-02-2021")))

    ))

    val rcuGasTrattamento = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasVarTrattamentoP(codicePdr = "1", dataInizio = formatter.parseDateTime("01-01-2019"), dataFine = formatter.parseDateTime("01-01-2022"),
        tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "2", dataInizio = formatter.parseDateTime("15-03-2021"), dataFine = formatter.parseDateTime("01-01-2022"),
        tTrattamentoSettlement = Treatment.G)
      , RcuGasVarTrattamentoP(codicePdr = "3", dataInizio = formatter.parseDateTime("15-02-2021"), dataFine = formatter.parseDateTime("01-03-2021"),
        tTrattamentoSettlement = Treatment.M)
      , RcuGasVarTrattamentoP(codicePdr = "4", dataInizio = formatter.parseDateTime("15-04-2021"), dataFine = formatter.parseDateTime("01-08-2021"),
        tTrattamentoSettlement = Treatment.M)
      , RcuGasVarTrattamentoP(codicePdr = "5", dataInizio = formatter.parseDateTime("01-02-2021"), dataFine = formatter.parseDateTime("15-03-2021"),
        tTrattamentoSettlement = Treatment.M)
      , RcuGasVarTrattamentoP(codicePdr = "5", dataInizio = formatter.parseDateTime("16-03-2021"), dataFine = formatter.parseDateTime("15-04-2021"),
        tTrattamentoSettlement = Treatment.G)
    ))

    val result = new TreatmentController().getRcuTreatment(measures, rcuGasTrattamento).cache()

    result.collect().foreach(println)

    Assert.assertEquals(6, result.count())

    Assert.assertEquals(Treatment.Y, result.filter(f => (f._1._1, f._1._2) == ("1", "202103")).first._2)
    Assert.assertEquals(Treatment.G, result.filter(f => (f._1._1, f._1._2) == ("2", "202103")).first._2)
    Assert.assertEquals(Treatment.M, result.filter(f => (f._1._1, f._1._2) == ("3", "202103")).first._2)
    Assert.assertEquals(Treatment.N, result.filter(f => (f._1._1, f._1._2) == ("4", "202103")).first._2)
    Assert.assertEquals(Treatment.M, result.filter(f => (f._1._1, f._1._2) == ("5", "202103")).first._2)
    Assert.assertEquals(Treatment.N, result.filter(f => (f._1._1, f._1._2) == ("6", "202103")).first._2)
  }

  def testRcuTreatmentWithoutMeasures(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val rcuGasTrattamento = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasVarTrattamentoP(codicePdr = "1", dataInizio = formatter.parseDateTime("01-01-2022"), dataFine = formatter.parseDateTime("01-02-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "1", dataInizio = formatter.parseDateTime("02-02-2022"), dataFine = formatter.parseDateTime("01-08-2022"), tTrattamentoSettlement = Treatment.G)
      , RcuGasVarTrattamentoP(codicePdr = "2", dataInizio = formatter.parseDateTime("01-01-2022"), dataFine = formatter.parseDateTime("01-08-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "3", dataInizio = formatter.parseDateTime("01-03-2022"), dataFine = formatter.parseDateTime("01-08-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "4", dataInizio = formatter.parseDateTime("01-04-2022"), dataFine = formatter.parseDateTime("01-08-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "5", dataInizio = formatter.parseDateTime("15-03-2022"), dataFine = formatter.parseDateTime("01-08-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "6", dataInizio = formatter.parseDateTime("15-01-2022"), dataFine = formatter.parseDateTime("15-03-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "7", dataInizio = formatter.parseDateTime("03-04-2022"), dataFine = formatter.parseDateTime("15-04-2022"), tTrattamentoSettlement = Treatment.G)
      , RcuGasVarTrattamentoP(codicePdr = "7", dataInizio = formatter.parseDateTime("16-04-2022"), dataFine = formatter.parseDateTime("20-04-2022"), tTrattamentoSettlement = Treatment.M)
      , RcuGasVarTrattamentoP(codicePdr = "7", dataInizio = formatter.parseDateTime("21-04-2022"), dataFine = formatter.parseDateTime("28-04-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "8", dataInizio = formatter.parseDateTime("01-05-2022"), dataFine = formatter.parseDateTime("28-08-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "9", dataInizio = formatter.parseDateTime("30-04-2022"), dataFine = formatter.parseDateTime("28-08-2022"), tTrattamentoSettlement = Treatment.Y)
    ))

    val treatmentController = new TreatmentController

    val result = treatmentController.getRcuTreatmentWithoutMeasures(rcuGasTrattamento, "202202", "202204").cache()

    result.sortBy({ case ((pdr, month), _, _) => (pdr, month) }).collect.foreach(println)

    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "1" && month == "202202" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.Y)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "1" && month == "202203" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.G)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "1" && month == "202204" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.G)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "2" && month == "202202" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.Y)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "2" && month == "202203" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.Y)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "2" && month == "202204" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.Y)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "3" && month == "202202" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.N)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "3" && month == "202203" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.Y)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "3" && month == "202204" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.Y)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "4" && month == "202202" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.N)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "4" && month == "202203" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.N)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "4" && month == "202204" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.Y)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "5" && month == "202202" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.N)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "5" && month == "202203" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.Y)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "5" && month == "202204" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.Y)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "6" && month == "202202" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.Y)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "6" && month == "202203" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.Y)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "6" && month == "202204" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.N)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "7" && month == "202202" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.N)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "7" && month == "202203" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.N)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "7" && month == "202204" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.G)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "8" && month == "202202" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.N)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "8" && month == "202203" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.N)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "8" && month == "202204" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.N)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "9" && month == "202202" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.N)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "9" && month == "202203" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.N)
    Assert.assertEquals(result.filter({ case ((pdr, month), _, _) => pdr == "9" && month == "202204" }).map({ case ((pdr, month), treatment, _) => treatment }).collect().head, Treatment.Y)
  }
}
