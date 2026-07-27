package it.sferanet.au.controller.ca

import it.sferanet.au.EnvironmentSparkTest/*
import it.sferanet.au.controller.ca.ConsumptionControllerTest._
import it.sferanet.au.model.MeasureValueType._
import it.sferanet.au.model._
import it.sferanet.au.model.periodico.{Tgl, Tml}
import it.sferanet.au.model.prestazionale.{A01, A40, Im1Post, Im1Pre}
import it.sferanet.au.model.rettifica.{Rgl, Rsl}
import it.sferanet.au.utilities.{Constants, Environment}
import org.junit.Assert

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.DAYS
import java.time.{LocalDate, YearMonth, ZoneId}
import scala.util.Random*/

class ConsumptionControllerTest extends EnvironmentSparkTest {/*

  def testCeMean(): Unit = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val getTimestamp = (string: String) => new Timestamp(formatter.parse(string).getTime)
    val yearPreviousToFlowEndDate = YearMonth.parse("202303", DateTimeFormatter.ofPattern("yyyyMM")).minusYears(1).getYear

    val monthsRange = List(5, 6, 7, 9)
      .map(YearMonth.of(yearPreviousToFlowEndDate, _))
      .map(month => {
        val startDate = month.atDay(1)
        val endDate = month.atEndOfMonth()
        (startDate, endDate)
      })

    val consumption = IndexedSeq(
      Consumption("pdrA", "service1", "service2", getTimestamp("2022-05-28"), getTimestamp("2022-05-30"), 0.0, 100.0, None, None, ConsumptionErrorStates.None, None, None, None, None, None, None, "0"),
      Consumption("pdrB", "service3", "service4", getTimestamp("2022-05-28"), getTimestamp("2022-06-15"), 0.0, 100.0, None, None, ConsumptionErrorStates.None, None, None, None, None, None, None, "0"),
      Consumption("pdrC", "service5", "service6", getTimestamp("2022-04-28"), getTimestamp("2022-05-05"), 0.0, 100.0, None, None, ConsumptionErrorStates.None, None, None, None, None, None, None, "0"),
      Consumption("pdrD", "service7", "service8", getTimestamp("2022-07-28"), getTimestamp("2022-09-15"), 0.0, 100.0, None, None, ConsumptionErrorStates.None, None, None, None, None, None, None, "0"),
      Consumption("pdrE", "service8", "service9", getTimestamp("2022-08-05"), getTimestamp("2022-08-15"), 0.0, 100.0, None, None, ConsumptionErrorStates.None, None, None, None, None, None, None, "0"),
      Consumption("pdrF", "service10", "service11", getTimestamp("2022-04-01"), getTimestamp("2022-10-01"), 0.0, 100.0, None, None, ConsumptionErrorStates.None, None, None, None, None, None, None, "0")
    )

    val ceMean = ConsumptionController.ceMean(monthsRange, consumption)

    println(ceMean)
  }

  def testA01Measures(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    val measures = Environment.getSparkContext.parallelize(List(
      A01(pdr = "1", service = "A01", date = Some(format.parse("2020-01-01")), readType = Some('a'), outcome = None,
        measure = Some(1), converted = Some(10), serialNumberConv = None, serialNumberMis = None, local_file = None, d_caricamento = None, isNewRoute = false, collected = None,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A01(pdr = "1", service = "A01", date = Some(format.parse("2020-01-02")), readType = Some('a'), outcome = None, measure = Some(2),
        converted = Some(20), serialNumberConv = None, serialNumberMis = None, local_file = None, d_caricamento = None, isNewRoute = false, collected = None,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A40(pdr = "1", service = "A40", date = Some(format.parse("2020-01-03")), readType = Some('a'), outcome = None,
        measure = Some(3), converted = Some(30), serialNumberConv = None, serialNumberMis = None, local_file = None, d_caricamento = None, isNewRoute = false, collected = None,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A40(pdr = "1", service = "A40", date = Some(format.parse("2020-01-04")), readType = Some('a'), outcome = None,
        measure = Some(4), converted = Some(40), serialNumberConv = None, serialNumberMis = None, local_file = None, d_caricamento = None, isNewRoute = false, collected = None,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      A01(pdr = "1", service = "A01", date = Some(format.parse("2020-01-05")), readType = Some('a'), outcome = None,
        measure = Some(5), converted = Some(50), serialNumberConv = None, serialNumberMis = None, local_file = None, d_caricamento = None, isNewRoute = false, collected = None,
        pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2020-01-01"), endDate = format.parse("2020-01-02"), t_codice_pdr = "1", n_coeff_correzione = Some(1),
        t_misuratore_integrato = None, t_pre_conv = None, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-02"), endDate = format.parse("2020-01-03"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-03"), endDate = format.parse("2020-01-04"), t_codice_pdr = "1", n_coeff_correzione = Some(3),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-04"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(4),
        t_misuratore_integrato = None, t_pre_conv = None, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2019-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)
    consumptions.foreach(println)

  }

  def testExecuteForPCMeasures1(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    val measures = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('a'), isValid = None,
        measure = Some(1), converted = Some(10), serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-02")), readType = Some('a'), isValid = None, measure = Some(2),
        converted = Some(20), serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-03")), readType = Some('a'), isValid = None,
        measure = Some(3), converted = Some(30), serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-04")), readType = Some('a'), isValid = None,
        measure = Some(4), converted = Some(40), serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-05")), readType = Some('a'), isValid = None,
        measure = Some(5), converted = Some(50), serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2020-01-01"), endDate = format.parse("2020-01-02"), t_codice_pdr = "1", n_coeff_correzione = Some(1),
        t_misuratore_integrato = None, t_pre_conv = None, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-02"), endDate = format.parse("2020-01-03"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-03"), endDate = format.parse("2020-01-04"), t_codice_pdr = "1", n_coeff_correzione = Some(3),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-04"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(4),
        t_misuratore_integrato = None, t_pre_conv = None, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2019-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)
    consumptions.foreach(println)

    consumptions.foreach(f => {
      Assert.assertEquals(CoeffLabel.RCU.toString, f.tipoCoeff)
    })

    //C-C
    Assert.assertEquals(10, consumptions.head.startvalue, 0)
    Assert.assertEquals(20, consumptions.head.endvalue, 0)

    //C-C
    Assert.assertEquals(20, consumptions(1).startvalue, 0)
    Assert.assertEquals(30, consumptions(1).endvalue, 0)

    //C-C
    Assert.assertEquals(30.0, consumptions(2).startvalue, 0)
    Assert.assertEquals(40.0, consumptions(2).endvalue, 0)

    //C-C
    Assert.assertEquals(40, consumptions(3).startvalue, 0)
    Assert.assertEquals(50, consumptions(3).endvalue, 0)
  }

  def testExecuteForPCMeasuresPK1(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    val measures = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('a'), isValid = None,
        measure = Some(1), converted = None, serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-02")), readType = Some('a'), isValid = None,
        measure = Some(2), converted = None, serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-03")), readType = Some('a'), isValid = None,
        measure = Some(3), converted = None, serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-04")), readType = Some('a'), isValid = None,
        measure = Some(4), converted = None, serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-05")), readType = Some('a'), isValid = None,
        measure = Some(5), converted = None, serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2020-01-01"), endDate = format.parse("2020-01-02"), t_codice_pdr = "1",
        n_coeff_correzione = Some(1), t_misuratore_integrato = None, t_pre_conv = None, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-02"), endDate = format.parse("2020-01-03"), t_codice_pdr = "1",
        n_coeff_correzione = Some(2), t_misuratore_integrato = None, t_pre_conv = None, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-03"), endDate = format.parse("2020-01-04"), t_codice_pdr = "1",
        n_coeff_correzione = Some(3), t_misuratore_integrato = None, t_pre_conv = None, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-04"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1",
        n_coeff_correzione = Some(4), t_misuratore_integrato = None, t_pre_conv = None, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2019-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)
    consumptions.foreach(println)
    consumptions.foreach(f => {
      Assert.assertEquals(CoeffLabel.RCU.toString, f.tipoCoeff)
    })

    //K - K Prel *1
    Assert.assertEquals(1 * 2, consumptions.head.startvalue, 0)
    Assert.assertEquals(2 * 2, consumptions.head.endvalue, 0)

    //K - K Prel *2
    Assert.assertEquals(2 * 3, consumptions(1).startvalue, 0)
    Assert.assertEquals(3 * 3, consumptions(1).endvalue, 0)

    //K - K Prel *3
    Assert.assertEquals(3 * 4, consumptions(2).startvalue, 0)
    Assert.assertEquals(4 * 4, consumptions(2).endvalue, 0)

    //K - K Prel *4
    Assert.assertEquals(4 * 4, consumptions(3).startvalue, 0)
    Assert.assertEquals(5 * 4, consumptions(3).endvalue, 0)
  }

  def testExecuteForPCMeasuresPK2(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    val measures = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('a'), isValid = None,
        measure = Some(1), converted = None, serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-02")), readType = Some('a'), isValid = None,
        measure = Some(2), converted = None, serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-03")), readType = Some('a'), isValid = None,
        measure = Some(3), converted = None, serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-04")), readType = Some('a'), isValid = None,
        measure = Some(4), converted = None, serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-05")), readType = Some('a'), isValid = None,
        measure = Some(5), converted = None, serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2020-01-01"), endDate = format.parse("2020-01-01"), t_codice_pdr = "1",
        n_coeff_correzione = Some(1), t_misuratore_integrato = None, t_pre_conv = None, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-02"), endDate = format.parse("2020-01-03"), t_codice_pdr = "1",
        n_coeff_correzione = Some(2), t_misuratore_integrato = None, t_pre_conv = None, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-04"), endDate = format.parse("2020-01-04"), t_codice_pdr = "1",
        n_coeff_correzione = Some(3), t_misuratore_integrato = None, t_pre_conv = None, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-05"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1",
        n_coeff_correzione = Some(4), t_misuratore_integrato = None, t_pre_conv = None, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2019-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)
    consumptions.foreach(println)
    consumptions.foreach(f => {
      Assert.assertEquals(CoeffLabel.RCU.toString, f.tipoCoeff)
    })

    //K - K Prel *2
    Assert.assertEquals(1 * 2, consumptions.head.startvalue, 0)
    Assert.assertEquals(2 * 2, consumptions.head.endvalue, 0)

    //K - K Prel *2
    Assert.assertEquals(2 * 2, consumptions(1).startvalue, 0)
    Assert.assertEquals(3 * 2, consumptions(1).endvalue, 0)

    //K - K Prel *3
    Assert.assertEquals(3 * 3, consumptions(2).startvalue, 0)
    Assert.assertEquals(4 * 3, consumptions(2).endvalue, 0)

    //K - K Prel *4
    Assert.assertEquals(4 * 4, consumptions(3).startvalue, 0)
    Assert.assertEquals(5 * 4, consumptions(3).endvalue, 0)
  }


  def testExecuteForPCMeasuresIM1(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    val measures = Environment.getSparkContext.parallelize(List(
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-01")), readType = Some('E'), measure = Some(9),
        converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None, cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.0), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-01")), readType = Some('E'), measure = Some(1),
        converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.0), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-02")), collected = Some("AC"), measure = Some(2),
        converted = Some(20), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-03")), collected = Some("AC"), measure = Some(3),
        converted = Some(30), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-04")), collected = Some("AC"), measure = Some(4),
        converted = Some(40), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), measure = Some(5),
        converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false,
        pivaDistr = None, pivaUtente = None, ammissibilita = None),

      Tgl("TGL", "00881407814753", Some(format.parse("2019-10-26")), None, None, Some('E'), Some("SI"), Some(4.648104E7), Some(1.3545493E7), Some("0039213617"),
        Some("SC10000007936"), Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), None, None, isNewRoute = false),
      Im1Pre("IM1PRE", "00881407814753", Some(format.parse("2019-10-27")), None, None, Some('E'), Some(4.648261E7), Some(1.3547207E7), Some("0039213617"), Some("SC10000007936"), Some(1.0), Some(4), None,
        Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2020/0529/00489490011_03707740233_IM10306_20200529124819_28.xml"), None, None, isNewRoute = false)
    )).map(_.asInstanceOf[Flow])

    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),

      RcuGasMassivoTech(format.parse("2019-10-28"), format.parse("2019-10-27"), "00881407814753", Some(1.0), Some("NO"), Some("SI"), Some(8), Some(8)),
      RcuGasMassivoTech(format.parse("2019-10-28"), format.parse("2040-10-28"), "00881407814753", Some(1.03), Some("NO"), Some("SI"), Some(8), Some(8)),
      RcuGasMassivoTech(format.parse("2018-06-01"), format.parse("2019-10-27"), "00881407814753", Some(1.0), Some("NO"), Some("SI"), Some(8), Some(8))
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "22")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)
    consumptions.foreach(println)
    consumptions.foreach(c => println(c.nCoeffCorrezione))
    //    consumptions.foreach(f => {Assert.assertEquals(f.tipoCoeff,CoeffLabel.CM.toString)})

    //P-P
    //    Assert.assertEquals(1.0, consumptions.head.startvalue, 0)
    //    Assert.assertEquals(2.0, consumptions.head.endvalue, 0)

    //K-K
    //    Assert.assertEquals(2, consumptions(1).startvalue, 0)
    //    Assert.assertEquals(3, consumptions(1).endvalue, 0)

    //K-K
    //    Assert.assertEquals(3, consumptions(2).startvalue, 0)
    //    Assert.assertEquals(4, consumptions(2).endvalue, 0)

    //K-K
    //    Assert.assertEquals(4, consumptions(3).startvalue, 0)
    //    Assert.assertEquals(5, consumptions(3).endvalue, 0)
  }

  def testExecuteIm1Im1(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    val measures = Environment.getSparkContext.parallelize(List(
      Tgl("TGL", "05260200744780", Some(format.parse("2019-06-05")), None, None, Some('E'), Some("SI"), Some(427846.0), Some(161411.0), Some("59020747"), Some("1108007195"), Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2019/0707/12883450152_12883420155_201906_TGL0050_20190707043754_90.xml"), None, None, isNewRoute = false),
      Im1Pre("IM1PRE", "05260200744780", Some(format.parse("2019-06-06")), None, None, Some('E'), Some(427845.0), Some(161411.0), Some("59020747"), Some("1108007195"), Some(1.0), Some(6), Some(3), Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2020/0713/12883450152_12883420155_201906_IM10306_20200713160137_462.xml"), None, None, isNewRoute = false),
      Im1Post("IM1POST", "05260200744780", Some(format.parse("2019-06-06")), None, None, Some('E'), Some(0.0), Some(161411.0), Some("16761709"), Some("1108007195"), Some(1.0), Some(6), Some(3), Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2020/0713/12883450152_12883420155_201906_IM10306_20200713160137_462.xml"), None, None, isNewRoute = false),
      Im1Pre("IM1PRE", "05260200744780", Some(format.parse("2019-06-19")), None, None, Some('E'), Some(2.0), Some(161411.0), Some("16761709"), Some("1108007195"), Some(1.0), Some(6), Some(5), Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2020/0402/12883450152_12883420155_201906_IM10306_20200402163043_62.xml"), None, None, isNewRoute = false),
      Im1Post("IM1POST", "05260200744780", Some(format.parse("2019-06-19")), None, None, Some('E'), Some(2.0), Some(161411.0), Some("16761709"), Some("1108007195"), Some(1.0), Some(6), Some(5), Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2020/0402/12883450152_12883420155_201906_IM10306_20200402163043_62.xml"), None, None, isNewRoute = false),
      Tgl("TGL", "05260200744780", Some(format.parse("2019-06-20")), None, None, Some('E'), Some("SI"), Some(2.0), Some(161411.0), Some("16761709"), Some("1108007195"), Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2019/0707/12883450152_12883420155_201906_TGL0050_20190707043754_90.xml"), None, None, isNewRoute = false),

      Rsl("RSL", "00594202093219", Some(format.parse("2019-04-01")), None, None, Some(3680.0), None, Some("SMGR034115517615"), None, Some("S"), Some(1), Some("/mnt/isilonshare1/GAS_INJ_RECUPERO/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12300020158/2019/1107/05608890488_12300020158_201910_RSL0400_20191107143000_1.xml"), None, None, isNewRoute = false),
      Im1Pre("IM1PRE", "00594202093219", Some(format.parse("2019-10-31")), None, None, Some('E'), Some(3911.0), Some(0.0), Some("SMGR034115517615"), None, Some(1.007), Some(3), None, Some("/mnt/isilonshare1/GAS_INJ/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2020/0526/05608890488_06655971007_IM10306_20200526225540_591.xml"), None, None, isNewRoute = false),
      Im1Post("IM1POST", "00594202093219", Some(format.parse("2019-10-31")), None, None, Some('E'), Some(1.0), None, Some("FIOR034119419709"), None, Some(1.007), Some(3), None, Some("/mnt/isilonshare1/GAS_INJ/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2020/0526/05608890488_06655971007_IM10306_20200526225540_591.xml"), None, None, isNewRoute = false),
      Im1Pre("IM1PRE", "00594202093219", Some(format.parse("2019-11-25")), None, None, Some('S'), Some(113.0), Some(0.0), Some("FIOR034119419709"), None, Some(1.007), Some(3), None, Some("/mnt/isilonshare1/GAS_INJ/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2020/0526/05608890488_06655971007_IM10306_20200526220839_6058.xml"), None, None, isNewRoute = false),
      Im1Post("IM1POST", "00594202093219", Some(format.parse("2019-11-25")), None, None, Some('S'), Some(0.0), None, Some("MIT0032013026454"), None, Some(1.007), Some(3), None, Some("/mnt/isilonshare1/GAS_INJ/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2020/0526/05608890488_06655971007_IM10306_20200526220839_6058.xml"), None, None, isNewRoute = false),
      Tml("TML", "00594202093219", Some(format.parse("2019-11-30")), None, None, Some('E'), Some("SI"), Some(15.0), None, Some("MIT0032013026454"), None, Some("/mnt/isilonshare1/GAS_INJ/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2019/1206/05608890488_06655971007_201911_TML0050_20191206114500_1.xml"), None, None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])

    measures.map(v => (v.pdr, v.service, v.date, v.coefCorr)).foreach(println)


    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(format.parse("2019-06-06"), format.parse("2020-03-26"), "05260200744780", None, None, Some("SI"), Some(6), Some(9)),
      RcuGasMassivoTech(format.parse("2020-03-27"), format.parse("2100-03-26"), "05260200744780", Some(1.1), Some("NO"), Some("SI"), Some(6), Some(9)),

      RcuGasMassivoTech(format.parse("2019-11-26"), format.parse("2019-11-25"), "00594202093219", Some(1.2), Some("SI"), Some("NO"), Some(7), None),
      RcuGasMassivoTech(format.parse("2019-10-31"), format.parse("2019-11-25"), "00594202093219", Some(1.3), Some("SI"), Some("NO"), Some(5), None),
      RcuGasMassivoTech(format.parse("2019-11-26"), format.parse("2100-11-26"), "00594202093219", Some(1.4), Some("SI"), Some("NO"), Some(7), None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2001-08-03"), endDate = format.parse("2100-06-05"), t_codice_pdr = "05260200744780", cat_uso = "C1", t_cod_profilo = "C1E1", t_processo = "", id_regione_climatica = 13, t_comune_istat_pdr = "015146", t_anno_termico = 2020, n_prelievo_annuo = "16490", n_id_pdr = "11"),

      RcuGasMassivo(startDate = format.parse("1995-07-06"), endDate = format.parse("2019-03-31"), t_codice_pdr = "00594202093219", cat_uso = "C3", t_cod_profilo = "C3D1", t_processo = "", id_regione_climatica = 19, t_comune_istat_pdr = "048033", t_anno_termico = 2019, n_prelievo_annuo = "1375", n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2019-04-01"), endDate = format.parse("2100-08-03"), t_codice_pdr = "00594202093219", cat_uso = "C3", t_cod_profilo = "C3D1", t_processo = "SWG", id_regione_climatica = 19, t_comune_istat_pdr = "048033", t_anno_termico = 2020, n_prelievo_annuo = "1190", n_id_pdr = "22")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)
    //    consumptions.foreach(f => {Assert.assertEquals(f.tipoCoeff,CoeffLabel.CM.toString)})
    consumptions.map(v => (v.pdr, v.startService, v.endService, v.nCoeffCorrezione, v.startvalue, v.endvalue)).foreach(println)

    val c1 = consumptions.filter(_.pdr == "05260200744780")

    Assert.assertEquals("TGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)

    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("IM1PRE", c1(1).endService)

    Assert.assertEquals("IM1POST", c1(2).startService)
    Assert.assertEquals("TGL", c1(2).endService)


    val c2 = consumptions.filter(_.pdr == "00594202093219")

    Assert.assertEquals("RSL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)

    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("IM1PRE", c2(1).endService)

    Assert.assertEquals("IM1POST", c2(2).startService)
    Assert.assertEquals("TML", c2(2).endService)
  }


  def testExecuteForPCMeasuresIM1CoeffCor(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    val measures = Environment.getSparkContext.parallelize(List(
      Im1Post("IM1POST", "15441000288457", Some(format.parse("2020-05-01")), None, None, Some('E'), Some(5875703.0), None, Some("83045912"), None, Some(5.459608), Some(6), Some(3),
        Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0604/01791490343_01178580997_IM10306_20200604151741_1307.xml"), None, None, isNewRoute = false),
      Im1Pre("IM1PRE", "15441000288457", Some(format.parse("2020-05-01")), None, None, Some('E'), Some(5875703.0), Some(1.7781092E7), Some("83045912"), Some("MIT0030009227736"), Some(1.0), Some(6), Some(3),
        Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0604/01791490343_01178580997_IM10306_20200604151741_1307.xml"), None, None, isNewRoute = false),
      Rgl("RGL", "15441000288457", Some(format.parse("2020-05-02")), None, None, Some(5875707.0), Some(1.7781116E7), Some("83045912"), Some("MIT0030009227736"), collected = Some("AC"), Some(2),
        Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0609/01791490343_01178580997_202006_RGL0055_20200609142447_62.xml"), None, None, isNewRoute = false),

      Rgl("RGL", "15441000288457", Some(format.parse("2020-05-03")), None, None, Some(5875709.0), Some(1.7781128E7), Some("83045912"), Some("MIT0030009227736"), collected = Some("AC"), Some(2),
        Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0609/01791490343_01178580997_202006_RGL0055_20200609142447_62.xml"), None, None, isNewRoute = false),
      Im1Post("IM1POST", "15441000288457", Some(format.parse("2020-05-04")), None, None, Some('E'), Some(5875710.0), Some(1.7781092E7), Some("83045912"), Some("MIT0030009227736"), Some(1.0), Some(6), Some(5),
        Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0825/01791490343_01178580997_IM10306_20200820151954_30.xml"), None, None, isNewRoute = false),
      Im1Pre("IM1PRE", "15441000288457", Some(format.parse("2020-05-04")), None, None, Some('E'), Some(5875710.0), None, Some("83045912"), None, Some(5.459608), Some(6), Some(5),
        Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0825/01791490343_01178580997_IM10306_20200820151954_30.xml"), None, None, isNewRoute = false),
      Rgl("RGL", "15441000288457", Some(format.parse("2020-05-05")), None, None, Some(5875710.0), Some(1.7781092E7), Some("83045912"), Some("MIT0030009227736"), collected = Some("AC"), Some(2),
        Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0609/01791490343_01178580997_202006_RGL0055_20200609142447_62.xml"), None, None, isNewRoute = false)
    )).map(_.asInstanceOf[Flow])

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(format.parse("2019-12-17"), format.parse("2100-05-03"), "15441000288457", None, Some("NO"), Some("SI"), Some(8), Some(9))
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(format.parse("2013-02-15"), format.parse("2100-05-03"), "15441000288457", "T2", "T2E1", "", 11, "034027", 2020, "3926656", "11")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)
    //    consumptions.foreach(f => {Assert.assertEquals(f.tipoCoeff,CoeffLabel.CM.toString)})
    consumptions.foreach(println)

    //    //P-P
    //    Assert.assertEquals(1.0, consumptions.head.startvalue, 0)
    //    Assert.assertEquals(2.0, consumptions.head.endvalue, 0)
    //
    //    //P-C
    //    Assert.assertEquals(20.0, consumptions(1).startvalue, 0)
    //    Assert.assertEquals(30.0, consumptions(1).endvalue, 0)
    //
    //    //C-C
    //    Assert.assertEquals(30.0, consumptions(2).startvalue, 0)
    //    Assert.assertEquals(40.0, consumptions(2).endvalue, 0)
    //
    //    //C-P
    //    Assert.assertEquals(40.0, consumptions(3).startvalue, 0)
    //    Assert.assertEquals(50.0, consumptions(3).endvalue, 0)
  }

  def testIntegrazioneCoerenzaDimensionale(): Unit = {

    val prel = rcu_misint_preconv_prels(rnd.nextInt(rcu_misint_preconv_prels.length))
    val conv = rcu_misint_preconv_convs(rnd.nextInt(rcu_misint_preconv_convs.length))
    val pk = rcu_misint_preconv_pks(rnd.nextInt(rcu_misint_preconv_pks.length))

    val format = new SimpleDateFormat("yyyy-MM-dd")

    val measures = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('a'), isValid = None, pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-02")), readType = Some('a'), isValid = None, pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(2), converted = Some(20), serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-03")), readType = Some('a'), isValid = None, pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(3), converted = Some(30), serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-04")), readType = Some('a'), isValid = None, pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(40), serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false),
      Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2020-01-05")), readType = Some('a'), isValid = None, pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false),

      Tgl(pdr = "2", service = "TGL", date = Some(format.parse("2020-01-01")), readType = Some('a'), isValid = None, pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false),
      Tgl(pdr = "2", service = "TGL", date = Some(format.parse("2020-01-02")), readType = Some('a'), isValid = None, pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(2), converted = Some(20), serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false),
      Tgl(pdr = "2", service = "TGL", date = Some(format.parse("2020-01-03")), readType = Some('a'), isValid = None, pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(3), converted = None, serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false),
      Tgl(pdr = "2", service = "TGL", date = Some(format.parse("2020-01-04")), readType = Some('a'), isValid = None, pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None, serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false),
      Tgl(pdr = "2", service = "TGL", date = Some(format.parse("2020-01-05")), readType = Some('a'), isValid = None, pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = None, serialNumberMis = Some("1"), local_file = None, d_caricamento = None, isNewRoute = false)
    )).map(_.asInstanceOf[Flow])

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2020-01-01"), endDate = format.parse("2020-01-01"), t_codice_pdr = "1", n_coeff_correzione = Some(1),
        t_misuratore_integrato = prel._1, t_pre_conv = prel._2, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-02"), endDate = format.parse("2020-01-02"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = conv._1, t_pre_conv = conv._2, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-03"), endDate = format.parse("2020-01-03"), t_codice_pdr = "1", n_coeff_correzione = Some(3),
        t_misuratore_integrato = pk._1, t_pre_conv = pk._2, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-04"), endDate = format.parse("2020-01-04"), t_codice_pdr = "1", n_coeff_correzione = Some(4),
        t_misuratore_integrato = prel._1, t_pre_conv = prel._2, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-05"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(5),
        t_misuratore_integrato = conv._1, t_pre_conv = conv._2, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),

      RcuGasMassivoTech(startDate = format.parse("2020-01-01"), endDate = format.parse("2020-01-01"), t_codice_pdr = "2", n_coeff_correzione = Some(1),
        t_misuratore_integrato = prel._1, t_pre_conv = prel._2, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-02"), endDate = format.parse("2020-01-02"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = conv._1, t_pre_conv = conv._2, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-03"), endDate = format.parse("2020-01-03"), t_codice_pdr = "2", n_coeff_correzione = Some(3),
        t_misuratore_integrato = pk._1, t_pre_conv = pk._2, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-04"), endDate = format.parse("2020-01-04"), t_codice_pdr = "2", n_coeff_correzione = Some(4),
        t_misuratore_integrato = prel._1, t_pre_conv = prel._2, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2020-01-05"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(5),
        t_misuratore_integrato = conv._1, t_pre_conv = conv._2, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2019-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2019-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).filter(_.pdr == "1").collect.sortBy(_.startSegment.getTime)
    val consumptions2 = result.flatMap(_._2._1).filter(_.pdr == "2").collect.sortBy(_.startSegment.getTime)
    consumptions.foreach(println)

    consumptions.foreach(f => {
      Assert.assertEquals(CoeffLabel.RCU.toString, f.tipoCoeff)
    })

    //P C -> C (esiste)
    Assert.assertEquals(10, consumptions.head.startvalue, 0)
    Assert.assertEquals(20, consumptions.head.endvalue, 0)

    //C K -> C (esiste)
    Assert.assertEquals(20, consumptions(1).startvalue, 0)
    Assert.assertEquals(30, consumptions(1).endvalue, 0)

    //K P -> K coefficiente più recente RCUGAS
    Assert.assertEquals(3 * 4, consumptions(2).startvalue, 0)
    Assert.assertEquals(4 * 4, consumptions(2).endvalue, 0)

    //P C -> C (esiste)
    Assert.assertEquals(40, consumptions(3).startvalue, 0)
    Assert.assertEquals(50, consumptions(3).endvalue, 0)

    consumptions2.foreach(f => {
      Assert.assertEquals(f.tipoCoeff, CoeffLabel.RCU.toString)
    })

    //P C -> P (non esiste)
    Assert.assertEquals(1, consumptions2.head.startvalue, 0)
    Assert.assertEquals(2, consumptions2.head.endvalue, 0)

    //C K -> K (non esiste) coefficiente più recente RCUGAS
    Assert.assertEquals(2 * 3, consumptions2(1).startvalue, 0)
    Assert.assertEquals(3 * 3, consumptions2(1).endvalue, 0)

    //K P -> K coefficiente più recente RCUGAS
    Assert.assertEquals(3 * 4, consumptions2(2).startvalue, 0)
    Assert.assertEquals(4 * 4, consumptions2(2).endvalue, 0)

    //P C -> P (non esiste)
    Assert.assertEquals(4, consumptions2(3).startvalue, 0)
    Assert.assertEquals(5, consumptions2(3).endvalue, 0)
  }

  def testApplyDimensionalCoerence(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    val coeff = 2.0
    val measure1 = Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-02")), collected = Some("AC"), measure = Some(2), pivaDistr = None, pivaUtente = None, ammissibilita = None,
      converted = Some(20), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false)
    val measure2 = Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-03")), collected = Some("AC"), measure = Some(3), pivaDistr = None, pivaUtente = None, ammissibilita = None,
      converted = Some(30), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false)

    Assert.assertEquals((2, 3, "PPP"), ConsumptionController.applyDimensionalCoerence(measure1, measure2, coeff, P, P))
    Assert.assertEquals((20, 30, "PCC"), ConsumptionController.applyDimensionalCoerence(measure1, measure2, coeff, P, C))
    Assert.assertEquals((2, 3, "PCP"), ConsumptionController.applyDimensionalCoerence(measure1.copy(converted = None), measure2, coeff, P, C))
    Assert.assertEquals((2, 3, "PCP"), ConsumptionController.applyDimensionalCoerence(measure1, measure2.copy(converted = None), coeff, P, C))
    Assert.assertEquals((2, 3, "PCP"), ConsumptionController.applyDimensionalCoerence(measure1.copy(converted = None), measure2.copy(converted = None), coeff, P, C))
    Assert.assertEquals((2 * coeff, 3 * coeff, "PKK"), ConsumptionController.applyDimensionalCoerence(measure1, measure2, coeff, P, K))
    Assert.assertEquals((20, 30, "CCC"), ConsumptionController.applyDimensionalCoerence(measure1, measure2, coeff, C, C))
    Assert.assertEquals((2 * coeff, 3 * coeff, "CCK"), ConsumptionController.applyDimensionalCoerence(measure1.copy(converted = None), measure2, coeff, C, C))
    Assert.assertEquals((2 * coeff, 3 * coeff, "CCK"), ConsumptionController.applyDimensionalCoerence(measure1, measure2.copy(converted = None), coeff, C, C))
    Assert.assertEquals((2 * coeff, 3 * coeff, "CCK"), ConsumptionController.applyDimensionalCoerence(measure1.copy(converted = None), measure2.copy(converted = None), coeff, C, C))
    Assert.assertEquals((20, 30, "CPC"), ConsumptionController.applyDimensionalCoerence(measure1, measure2, coeff, C, P))
    Assert.assertEquals((2, 3, "CPP"), ConsumptionController.applyDimensionalCoerence(measure1.copy(converted = None), measure2, coeff, C, P))
    Assert.assertEquals((2, 3, "CPP"), ConsumptionController.applyDimensionalCoerence(measure1, measure2.copy(converted = None), coeff, C, P))
    Assert.assertEquals((2, 3, "CPP"), ConsumptionController.applyDimensionalCoerence(measure1.copy(converted = None), measure2.copy(converted = None), coeff, C, P))
    Assert.assertEquals((20, 30, "CKC"), ConsumptionController.applyDimensionalCoerence(measure1, measure2, coeff, C, K))
    Assert.assertEquals((2 * coeff, 3 * coeff, "CKK"), ConsumptionController.applyDimensionalCoerence(measure1.copy(converted = None), measure2, coeff, C, K))
    Assert.assertEquals((2 * coeff, 3 * coeff, "CKK"), ConsumptionController.applyDimensionalCoerence(measure1, measure2.copy(converted = None), coeff, C, K))
    Assert.assertEquals((2 * coeff, 3 * coeff, "CKK"), ConsumptionController.applyDimensionalCoerence(measure1.copy(converted = None), measure2.copy(converted = None), coeff, C, K))
    Assert.assertEquals((2 * coeff, 3 * coeff, "KKK"), ConsumptionController.applyDimensionalCoerence(measure1, measure2, coeff, K, K))
    Assert.assertEquals((2 * coeff, 3 * coeff, "KPK"), ConsumptionController.applyDimensionalCoerence(measure1, measure2, coeff, K, P))
    Assert.assertEquals((20, 30, "KCC"), ConsumptionController.applyDimensionalCoerence(measure1, measure2, coeff, K, C))
    Assert.assertEquals((2 * coeff, 3 * coeff, "KCK"), ConsumptionController.applyDimensionalCoerence(measure1.copy(converted = None), measure2, coeff, K, C))
    Assert.assertEquals((2 * coeff, 3 * coeff, "KCK"), ConsumptionController.applyDimensionalCoerence(measure1, measure2.copy(converted = None), coeff, K, C))
    Assert.assertEquals((2 * coeff, 3 * coeff, "KCK"), ConsumptionController.applyDimensionalCoerence(measure1.copy(converted = None), measure2.copy(converted = None), coeff, K, C))
    Assert.assertTrue(ConsumptionController.applyDimensionalCoerence(measure1, measure2, coeff, null, K)._3.equals("ERROR"))
    Assert.assertTrue(ConsumptionController.applyDimensionalCoerence(measure1.copy(converted = None), measure2.copy(converted = None), coeff, K, null)._3.equals("ERROR"))
    Assert.assertTrue(ConsumptionController.applyDimensionalCoerence(measure1.copy(converted = None), measure2.copy(converted = None), coeff, null, null)._3.equals("ERROR"))

    Assert.assertTrue(ConsumptionController.applyDimensionalCoerence(measure1.copy(measure = None, converted = None), measure2.copy(measure = None, converted = None), coeff, P, P)._3.equals("ERROR"))
    Assert.assertTrue(ConsumptionController.applyDimensionalCoerence(measure1.copy(measure = None, converted = None), measure2.copy(measure = None, converted = None), coeff, P, C)._3.equals("ERROR"))
    Assert.assertTrue(ConsumptionController.applyDimensionalCoerence(measure1.copy(measure = None, converted = None), measure2.copy(measure = None, converted = None), coeff, P, K)._3.equals("ERROR"))
    Assert.assertTrue(ConsumptionController.applyDimensionalCoerence(measure1.copy(measure = None, converted = None), measure2.copy(measure = None, converted = None), coeff, C, K)._3.equals("ERROR"))
    Assert.assertTrue(ConsumptionController.applyDimensionalCoerence(measure1.copy(measure = None, converted = None), measure2.copy(measure = None, converted = None), coeff, null, P)._3.equals("ERROR"))
    Assert.assertTrue(ConsumptionController.applyDimensionalCoerence(measure1.copy(measure = None, converted = None), measure2.copy(measure = None, converted = None), coeff, P, null)._3.equals("ERROR"))
    Assert.assertTrue(ConsumptionController.applyDimensionalCoerence(measure1.copy(measure = None, converted = None), measure2.copy(measure = None, converted = None), coeff, null, null)._3.equals("ERROR"))
  }

}

object ConsumptionControllerTest {
  val rnd: Random = new Random()

  val rcuGasProfilo = Environment.getSparkContext.parallelize(List(
    RcuGasProfilo(n_id_pdr = "11", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None"),
    RcuGasProfilo(n_id_pdr = "22", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None")
  ))

  // combinazioni (GRUPPO_MIS_INT,PRE_CONV) per cui per il segmento Post considero segnante Prelevata
  val rcu_misint_preconv_prels: List[(Option[String], Option[String])] = List((Some("SI"), None), (Some("SI"), Some("NO")),
    (Some("SI"), Some("SI")), (Some("S"), Some("NO")), (Some(""), Some("NO")))

  // combinazioni (GRUPPO_MIS_INT,PRE_CONV) per cui per il segmento Post considero segnante Convertita
  val rcu_misint_preconv_convs: List[(Option[String], Option[String])] = List((Some("NO"), Some("SI")),
    (None, Some("SI")), (Some("N"), Some("SI")), (Some(""), Some("SI")), (None, None))

  // combinazioni (GRUPPO_MIS_INT,PRE_CONV) per cui per il segmento Post considero segnante Prelevata*coeff
  val rcu_misint_preconv_pks: List[(Option[String], Option[String])] = List(
    (Some("NO"), Some("NO")), (Some("N"), Some("NO")))*/
}
