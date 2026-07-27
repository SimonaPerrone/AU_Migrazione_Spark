package it.sferanet.au.controller.coeffCorr

import it.sferanet.au.EnvironmentSparkTest/*
import it.sferanet.au.controller.ca.ConsumptionController
import it.sferanet.au.controller.coeffCorr.CoeffCorrControllerTestIntegrazioneIgmg.rcuGasProfilo
import it.sferanet.au.model._
import it.sferanet.au.model.periodico._
import it.sferanet.au.model.prestazionale._
import it.sferanet.au.model.rettifica._
import it.sferanet.au.utilities.{Constants, Environment}
import org.junit.Assert
*/
import java.text.SimpleDateFormat

/**
 * classe che contiene i test di integrazione del componente CoeffCorrController con ConsuptionController
 * per il calcolo dei consumi
 */
class CoeffCorrControllerTestIntegrazioneIgmg extends EnvironmentSparkTest {/*
  //Environment.resetProperty()


  /**
   * Test scenario 2 con un IGMG all'inizio paragrafo 6.3 documento
   */

  def testExecuteForPCMeasuresIGMG(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2017-01-02")

    val measures = Environment.getSparkContext.parallelize(List(
      IgmgPre(pdr = "1", service = "IGMGPRE", date = Some(format.parse("2017-01-01")), readType = Some('E'), measure = Some(9),
        converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None, cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(pdr = "1", service = "IGMGPOST", date = Some(format.parse("2017-01-01")), readType = Some('E'), measure = Some(1),
        converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-02")), collected = Some("AC"), measure = Some(2),
        converted = Some(20), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-03")), collected = Some("AC"), measure = Some(3),
        converted = Some(30), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-04")), collected = Some("AC"), measure = Some(4),
        converted = Some(40), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), measure = Some(5),
        converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),

      Tgl("TGL", "00881407814753", Some(format.parse("2019-10-26")), None, None, Some('E'), Some("SI"), Some(4.648104E7), Some(1.3545493E7), Some("0039213617"),
        Some("SC10000007936"), Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), None, None, false),
      IgmgPre(service = "IGMGPRE", pdr = "00881407814753", date = Some(format.parse("2019-10-27")), readType = Some('E'), measure = Some(4.648261E7), converted = Some(1.3547207E7), serialNumberMis = Some("0039213617"), serialNumberConv = Some("SC10000007936"),
        coefCorr = Some(1.3), cau_int_mis = Some(4), cau_int_cor = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2020/0529/00489490011_03707740233_IGMG0306_20200529124819_28.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),

      RcuGasMassivoTech(format.parse("2019-10-28"), format.parse("2019-10-27"), "00881407814753", Some(0.1), Some("NO"), Some("SI"), Some(8), Some(8)),
      RcuGasMassivoTech(format.parse("2019-10-28"), format.parse("2040-10-28"), "00881407814753", Some(0.2), Some("NO"), Some("SI"), Some(8), Some(8)),
      RcuGasMassivoTech(format.parse("2018-06-01"), format.parse("2019-10-27"), "00881407814753", Some(0.3), Some("NO"), Some("SI"), Some(8), Some(8))
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)
    consumptions.foreach(c => {
      print(c.nCoeffCorrezione);
      print(" - ");
      println(c)
    })

    consumptions.foreach(f => {
      Assert.assertEquals(f.tipoCoeff, CoeffLabel.CM.toString)
    })

    //coeff
    Assert.assertEquals(1.2, consumptions.head.nCoeffCorrezione.get, 0)

    //coeff
    Assert.assertEquals(1.2, consumptions(1).nCoeffCorrezione.get, 0)

    //coeff
    Assert.assertEquals(1.2, consumptions(2).nCoeffCorrezione.get, 0)

    //coeff
    Assert.assertEquals(1.2, consumptions(3).nCoeffCorrezione.get, 0)

    //coeff
    Assert.assertEquals(1.3, consumptions(4).nCoeffCorrezione.get, 0)
  }

  /**
   * Test Scenario 2 un Igmg nel mezzo di altre misure paragrafo 6.3 documento
   */
  def testExecuteForPCMeasuresOneIGMGMiddle(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2016-05-01")

    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-02")), collected = Some("AC"), measure = Some(2),
        converted = Some(20), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-03")), collected = Some("AC"), measure = Some(3),
        converted = Some(30), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(pdr = "1", service = "IGMGPRE", date = Some(format.parse("2017-01-04")), readType = Some('E'), measure = Some(9),
        converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None, cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(pdr = "1", service = "IGMGPOST", date = Some(format.parse("2017-01-04")), readType = Some('E'), measure = Some(1),
        converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), measure = Some(4),
        converted = Some(40), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-06")), collected = Some("AC"), measure = Some(5),
        converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),

      Tgl("TGL", "00881407814753", Some(format.parse("2019-10-26")), None, None, Some('E'), Some("SI"), Some(4.648104E7), Some(1.3545493E7), Some("0039213617"),
        Some("SC10000007936"), Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), None, None, false),
      IgmgPre(service = "IGMGPRE", pdr = "00881407814753", date = Some(format.parse("2019-10-27")), readType = Some('E'), measure = Some(4.648261E7), converted = Some(1.3547207E7), serialNumberMis = Some("0039213617"), serialNumberConv = Some("SC10000007936"),
        coefCorr = Some(1.3), cau_int_mis = Some(4), cau_int_cor = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2020/0529/00489490011_03707740233_IGMG0306_20200529124819_28.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),

      RcuGasMassivoTech(format.parse("2019-10-28"), format.parse("2019-10-27"), "00881407814753", Some(0.1), Some("NO"), Some("SI"), Some(8), Some(8)),
      RcuGasMassivoTech(format.parse("2019-10-28"), format.parse("2040-10-28"), "00881407814753", Some(0.2), Some("NO"), Some("SI"), Some(8), Some(8)),
      RcuGasMassivoTech(format.parse("2018-06-01"), format.parse("2019-10-27"), "00881407814753", Some(0.3), Some("NO"), Some("SI"), Some(8), Some(8))
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, "11"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)
    consumptions.foreach(c => {
      print(c.nCoeffCorrezione);
      print(" - ");
      println(c)
    })
    consumptions.foreach(f => {
      Assert.assertEquals(f.tipoCoeff, CoeffLabel.CM.toString)
    })

    //coeff RGL - RGL c_pre (02Gen - 03Gen)
    Assert.assertEquals("RGL", consumptions.head.startService)
    Assert.assertEquals("RGL", consumptions.head.endService)
    Assert.assertEquals("2017-01-02", format.format(consumptions.head.startSegment))
    Assert.assertEquals("2017-01-03", format.format(consumptions.head.endSegment))
    Assert.assertEquals(1.1, consumptions.head.nCoeffCorrezione.get, 0)

    //coeff RGL - IGMGPRE c_pre (03Gen - 04Gen)
    Assert.assertEquals("RGL", consumptions(1).startService)
    Assert.assertEquals("IGMGPRE", consumptions(1).endService)
    Assert.assertEquals("2017-01-03", format.format(consumptions(1).startSegment))
    Assert.assertEquals("2017-01-04", format.format(consumptions(1).endSegment))
    Assert.assertEquals(1.1, consumptions(1).nCoeffCorrezione.get, 0)

    //coeff IGMGPOST - RGL c_post (04Gen - 05Gen)
    Assert.assertEquals("IGMGPOST", consumptions(2).startService)
    Assert.assertEquals("RGL", consumptions(2).endService)
    Assert.assertEquals("2017-01-04", format.format(consumptions(2).startSegment))
    Assert.assertEquals("2017-01-05", format.format(consumptions(2).endSegment))
    Assert.assertEquals(1.2, consumptions(2).nCoeffCorrezione.get, 0)

    //coeff RGL - RGL c_post (05Gen -06Gen)
    Assert.assertEquals("RGL", consumptions(3).startService)
    Assert.assertEquals("RGL", consumptions(3).endService)
    Assert.assertEquals("2017-01-05", format.format(consumptions(3).startSegment))
    Assert.assertEquals("2017-01-06", format.format(consumptions(3).endSegment))
    Assert.assertEquals(1.2, consumptions(3).nCoeffCorrezione.get, 0)


    //coeff ALTRO PDR - Come test Precedente
    Assert.assertEquals(1.3, consumptions(4).nCoeffCorrezione.get, 0)
  }

  /**
   * Test Scenario 2 Igmg come ultima misura paragrafo 6.3 documento
   */
  def testExecuteForPCMeasuresOneIGMGEnd(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2016-05-01")

    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-02")), collected = Some("AC"), measure = Some(2),
        converted = Some(20), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-03")), collected = Some("AC"), measure = Some(3),
        converted = Some(30), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-04")), collected = Some("AC"), measure = Some(4),
        converted = Some(40), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), measure = Some(5),
        converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(pdr = "1", service = "IGMGPRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), measure = Some(9),
        converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None, cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(pdr = "1", service = "IGMGPOST", date = Some(format.parse("2017-01-06")), readType = Some('E'), measure = Some(1),
        converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),

      Tgl("TGL", "00881407814753", Some(format.parse("2019-10-26")), None, None, Some('E'), Some("SI"), Some(4.648104E7), Some(1.3545493E7), Some("0039213617"),
        Some("SC10000007936"), Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), None, None, false),
      IgmgPre(service = "IGMGPRE", pdr = "00881407814753", date = Some(format.parse("2019-10-27")), readType = Some('E'), measure = Some(4.648261E7), converted = Some(1.3547207E7), serialNumberMis = Some("0039213617"), serialNumberConv = Some("SC10000007936"),
        coefCorr = Some(1.3), cau_int_mis = Some(4), cau_int_cor = None,
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2020/0529/00489490011_03707740233_IGMG0306_20200529124819_28.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),

      RcuGasMassivoTech(format.parse("2019-10-28"), format.parse("2019-10-27"), "00881407814753", Some(0.1), Some("NO"), Some("SI"), Some(8), Some(8)),
      RcuGasMassivoTech(format.parse("2019-10-28"), format.parse("2040-10-28"), "00881407814753", Some(0.2), Some("NO"), Some("SI"), Some(8), Some(8)),
      RcuGasMassivoTech(format.parse("2018-06-01"), format.parse("2019-10-27"), "00881407814753", Some(0.3), Some("NO"), Some("SI"), Some(8), Some(8))
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, "11"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "22")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)
    consumptions.foreach(c => {
      print(c.nCoeffCorrezione);
      print(" - ");
      println(c)
    })
    consumptions.foreach(f => {
      Assert.assertEquals(f.tipoCoeff, CoeffLabel.CM.toString)
    })

    //coeff RGL - RGL c_pre (02Gen - 03Gen)
    Assert.assertEquals("RGL", consumptions.head.startService)
    Assert.assertEquals("RGL", consumptions.head.endService)
    Assert.assertEquals("2017-01-02", format.format(consumptions.head.startSegment))
    Assert.assertEquals("2017-01-03", format.format(consumptions.head.endSegment))
    Assert.assertEquals(1.1, consumptions.head.nCoeffCorrezione.get, 0)

    //coeff RGL - RGL c_pre (03Gen - 04Gen)
    Assert.assertEquals("RGL", consumptions(1).startService)
    Assert.assertEquals("RGL", consumptions(1).endService)
    Assert.assertEquals("2017-01-03", format.format(consumptions(1).startSegment))
    Assert.assertEquals("2017-01-04", format.format(consumptions(1).endSegment))
    Assert.assertEquals(1.1, consumptions(1).nCoeffCorrezione.get, 0)

    //coeff RGL - RGL c_pre (04Gen - 05Gen)
    Assert.assertEquals("RGL", consumptions(2).startService)
    Assert.assertEquals("RGL", consumptions(2).endService)
    Assert.assertEquals("2017-01-04", format.format(consumptions(2).startSegment))
    Assert.assertEquals("2017-01-05", format.format(consumptions(2).endSegment))
    Assert.assertEquals(1.1, consumptions(2).nCoeffCorrezione.get, 0)

    //coeff RGL - IGMGPRE c_pre (05Gen - 06Gen)
    Assert.assertEquals("RGL", consumptions(3).startService)
    Assert.assertEquals("IGMGPRE", consumptions(3).endService)
    Assert.assertEquals("2017-01-05", format.format(consumptions(3).startSegment))
    Assert.assertEquals("2017-01-06", format.format(consumptions(3).endSegment))
    Assert.assertEquals(1.1, consumptions(3).nCoeffCorrezione.get, 0)

    //coeff ALTRO PDR - Come test Precedente
    Assert.assertEquals(1.3, consumptions(4).nCoeffCorrezione.get, 0)
  }

  /**
   * Scenario 3 del paragrafo 6.3 del documento. Due Igmg nel mezzo delle misure di un PDR
   */
  def testExecuteIgmgIgmg(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2016-05-01")

    val measures = Environment.getSparkContext.parallelize(List(
      Tgl(service = "TGL", pdr = "05260200744780", date = Some(format.parse("2019-06-05")), readType = Some('E'), isValid = Some("SI"),
        measure = Some(427846.0), converted = Some(161411.0), serialNumberMis = Some("59020747"), serialNumberConv = Some("1108007195"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2019/0707/12883450152_12883420155_201906_TGL0050_20190707043754_90.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(service = "IGMGPRE", pdr = "05260200744780", date = Some(format.parse("2019-06-06")), readType = Some('E'),
        measure = Some(427845.0), converted = Some(161411.0), serialNumberMis = Some("59020747"), serialNumberConv = Some("1108007195"),
        coefCorr = Some(0.1), cau_int_mis = Some(6), cau_int_cor = Some(3), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2020/0713/12883450152_12883420155_201906_IGMG0306_20200713160137_462.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "05260200744780", date = Some(format.parse("2019-06-06")), readType = Some('E'),
        measure = Some(0.0), converted = Some(161411.0), serialNumberMis = Some("16761709"), serialNumberConv = Some("1108007195"),
        coefCorr = Some(0.2), cau_int_mis = Some(6), cau_int_cor = Some(3), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2020/0713/12883450152_12883420155_201906_IGMG0306_20200713160137_462.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(service = "IGMGPRE", pdr = "05260200744780", date = Some(format.parse("2019-06-19")), readType = Some('E'),
        measure = Some(2.0), converted = Some(161411.0), serialNumberMis = Some("16761709"), serialNumberConv = Some("1108007195"),
        coefCorr = Some(0.3), cau_int_mis = Some(6), cau_int_cor = Some(5), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2020/0402/12883450152_12883420155_201906_IGMG0306_20200402163043_62.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "05260200744780", date = Some(format.parse("2019-06-19")), readType = Some('E'),
        measure = Some(2.0), converted = Some(161411.0), serialNumberMis = Some("16761709"), serialNumberConv = Some("1108007195"),
        coefCorr = Some(0.4), cau_int_mis = Some(6), cau_int_cor = Some(5), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2020/0402/12883450152_12883420155_201906_IGMG0306_20200402163043_62.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(service = "TGL", pdr = "05260200744780", date = Some(format.parse("2019-06-20")), readType = Some('E'), isValid = Some("SI"),
        measure = Some(2.0), converted = Some(161411.0), serialNumberMis = Some("16761709"), serialNumberConv = Some("1108007195"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2019/0707/12883450152_12883420155_201906_TGL0050_20190707043754_90.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),

      Rsl(service = "RSL", pdr = "00594202093219", date = Some(format.parse("2019-04-01")),
        measure = Some(3680.0), converted = None, serialNumberMis = Some("SMGR034115517615"), serialNumberConv = None, collected = Some("S"), motivation = Some(1), local_file = Some("/mnt/isilonshare1/GAS_INJ_RECUPERO/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12300020158/2019/1107/05608890488_12300020158_201910_RSL0400_20191107143000_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(service = "IGMGPRE", pdr = "00594202093219", date = Some(format.parse("2019-10-31")), readType = Some('E'),
        measure = Some(3911.0), converted = Some(0.0), serialNumberMis = Some("SMGR034115517615"), serialNumberConv = None,
        coefCorr = Some(0.5), cau_int_mis = Some(3), cau_int_cor = None, local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2020/0526/05608890488_06655971007_IGMG0306_20200526225540_591.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "00594202093219", date = Some(format.parse("2019-10-31")), readType = Some('E'),
        measure = Some(1.0), converted = None, serialNumberMis = Some("FIOR034119419709"), serialNumberConv = None,
        coefCorr = Some(0.6), cau_int_mis = Some(3), cau_int_cor = None, local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2020/0526/05608890488_06655971007_IGMG0306_20200526225540_591.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(service = "IGMGPRE", pdr = "00594202093219", date = Some(format.parse("2019-11-25")), readType = Some('S'),
        measure = Some(113.0), converted = Some(0.0), serialNumberMis = Some("FIOR034119419709"), serialNumberConv = None,
        coefCorr = Some(0.7), cau_int_mis = Some(3), cau_int_cor = None, local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2020/0526/05608890488_06655971007_IGMG0306_20200526220839_6058.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "00594202093219", date = Some(format.parse("2019-11-25")), readType = Some('S'),
        measure = Some(2.0), converted = None, serialNumberMis = Some("MIT0032013026454"), serialNumberConv = None,
        coefCorr = Some(0.8), cau_int_mis = Some(3), cau_int_cor = None, local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2020/0526/05608890488_06655971007_IGMG0306_20200526220839_6058.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(service = "TML", pdr = "00594202093219", date = Some(format.parse("2019-11-30")), readType = Some('E'), isValid = Some("SI"),
        measure = Some(15.0), converted = None, serialNumberMis = Some("MIT0032013026454"), serialNumberConv = None, local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2019/1206/05608890488_06655971007_201911_TML0050_20191206114500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)

    )).map(_.asInstanceOf[Flow])

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(format.parse("2019-06-06"), format.parse("2020-03-26"), "05260200744780", None, None, Some("SI"), Some(6), Some(9)),
      RcuGasMassivoTech(format.parse("2020-03-27"), format.parse("2100-03-26"), "05260200744780", Some(1.1), Some("NO"), Some("SI"), Some(6), Some(9)),

      RcuGasMassivoTech(format.parse("2019-11-26"), format.parse("2019-11-25"), "00594202093219", Some(1.2), Some("SI"), Some("NO"), Some(7), None),
      RcuGasMassivoTech(format.parse("2019-10-31"), format.parse("2019-11-25"), "00594202093219", Some(1.3), Some("SI"), Some("NO"), Some(5), None),
      RcuGasMassivoTech(format.parse("2019-11-26"), format.parse("2100-11-26"), "00594202093219", Some(1.4), Some("SI"), Some("NO"), Some(7), None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(format.parse("2001-08-03"), format.parse("2100-06-05"), "05260200744780", "C1", "C1E1", "", 13, "015146", 2020, "16490", "11"),

      RcuGasMassivo(format.parse("1995-07-06"), format.parse("2019-03-31"), "00594202093219", "C3", "C3D1", "", 19, "048033", 2019, "1375", "22"),
      RcuGasMassivo(format.parse("2019-04-01"), format.parse("2100-08-03"), "00594202093219", "C3", "C3D1", "SWG", 19, "048033", 2020, "1190", "22")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)
    consumptions.map(v => (v.pdr, v.startService, v.endService, v.nCoeffCorrezione, v.startvalue, v.endvalue)).foreach(println)
    consumptions.foreach(c => {
      print(c.nCoeffCorrezione);
      print(" - ");
      println(c)
    })
    consumptions.foreach(f => {
      Assert.assertEquals(f.tipoCoeff, CoeffLabel.CM.toString)
    })

    val c1 = consumptions.filter(_.pdr == "05260200744780")

    //coeff TGL - IGMGPRE c1_pre (05Giu - 06Giu)
    Assert.assertEquals("TGL", c1.head.startService)
    Assert.assertEquals("IGMGPRE", c1.head.endService)
    Assert.assertEquals("2019-06-05", format.format(c1.head.startSegment))
    Assert.assertEquals("2019-06-06", format.format(c1.head.endSegment))
    Assert.assertEquals(0.1, c1.head.nCoeffCorrezione.get, 0)

    //coeff IGMGPOST - IGMGPRE c2_pre (06Giu - 19Giu)
    Assert.assertEquals("IGMGPOST", c1(1).startService)
    Assert.assertEquals("IGMGPRE", c1(1).endService)
    Assert.assertEquals("2019-06-06", format.format(c1(1).startSegment))
    Assert.assertEquals("2019-06-19", format.format(c1(1).endSegment))
    Assert.assertEquals(0.3, c1(1).nCoeffCorrezione.get, 0)

    //coeff IGMGPOST - TGL c2_post (19Giu - 20Giu)
    Assert.assertEquals("IGMGPOST", c1(2).startService)
    Assert.assertEquals("TGL", c1(2).endService)
    Assert.assertEquals("2019-06-19", format.format(c1(2).startSegment))
    Assert.assertEquals("2019-06-20", format.format(c1(2).endSegment))
    Assert.assertEquals(0.4, c1(2).nCoeffCorrezione.get, 0)


    val c2 = consumptions.filter(_.pdr == "00594202093219")

    //coeff RSL IGMGPRE c1_pre (01Apr - 31Ott)
    Assert.assertEquals("RSL", c2.head.startService)
    Assert.assertEquals("IGMGPRE", c2.head.endService)
    Assert.assertEquals("2019-04-01", format.format(c2.head.startSegment))
    Assert.assertEquals("2019-10-31", format.format(c2.head.endSegment))
    Assert.assertEquals(0.5, c2.head.nCoeffCorrezione.get, 0)

    //coeff IGMGPOST IGMGPRE c2_pre (31Ott - 25Nov)
    Assert.assertEquals("IGMGPOST", c2(1).startService)
    Assert.assertEquals("IGMGPRE", c2(1).endService)
    Assert.assertEquals("2019-10-31", format.format(c2(1).startSegment))
    Assert.assertEquals("2019-11-25", format.format(c2(1).endSegment))
    Assert.assertEquals(0.7, c2(1).nCoeffCorrezione.get, 0)

    //coeff IGMGPOST TML c2_post (25Nov - 30Nov)
    Assert.assertEquals("IGMGPOST", c2(2).startService)
    Assert.assertEquals("TML", c2(2).endService)
    Assert.assertEquals("2019-11-25", format.format(c2(2).startSegment))
    Assert.assertEquals("2019-11-30", format.format(c2(2).endSegment))
    Assert.assertEquals(0.8, c2(2).nCoeffCorrezione.get, 0)
  }


  /**
   * Scenario 4 del paragrafo 6.3 del documento. Due Igmg consecutivi con Due misure in mezzo
   */
  def testExecuteForPCMeasuresIGMGCoeffCor(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2016-05-01")

    val measures = Environment.getSparkContext.parallelize(List(
      Tml(service = "TML", pdr = "15441000288457", date = Some(format.parse("2020-04-30")), readType = Some('A'), isValid = None,
        measure = Some(4875709.0), converted = Some(2.7781128E7), serialNumberMis = Some("83045912"), serialNumberConv = Some("MIT0030009227736"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0609/01791490343_01178580997_202006_RGL0055_20200609142447_62.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(service = "IGMGPRE", pdr = "15441000288457", date = Some(format.parse("2020-05-01")), readType = Some('E'),
        measure = Some(5875703.0), converted = Some(1.7781092E7), serialNumberMis = Some("83045912"), serialNumberConv = Some("MIT0030009227736"),
        coefCorr = Some(0.1), cau_int_mis = Some(6), cau_int_cor = Some(3),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0604/01791490343_01178580997_IGMG0306_20200604151741_1307.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "15441000288457", date = Some(format.parse("2020-05-01")), readType = Some('E'),
        measure = Some(5875703.0), converted = None, serialNumberMis = Some("83045912"), serialNumberConv = None,
        coefCorr = Some(0.2), cau_int_mis = Some(6), cau_int_cor = Some(3),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0604/01791490343_01178580997_IGMG0306_20200604151741_1307.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(service = "RGL", pdr = "15441000288457", date = Some(format.parse("2020-05-02")),
        measure = Some(5875707.0), converted = Some(1.7781116E7), serialNumberMis = Some("83045912"), serialNumberConv = Some("MIT0030009227736"), collected = Some("AC"), motivation = Some(2),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0609/01791490343_01178580997_202006_RGL0055_20200609142447_62.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(service = "RGL", pdr = "15441000288457", date = Some(format.parse("2020-05-03")),
        measure = Some(5875709.0), converted = Some(1.7781128E7), serialNumberMis = Some("83045912"), serialNumberConv = Some("MIT0030009227736"), collected = Some("AC"), motivation = Some(2),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0609/01791490343_01178580997_202006_RGL0055_20200609142447_62.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(service = "IGMGPRE", pdr = "15441000288457", date = Some(format.parse("2020-05-04")), readType = Some('E'),
        measure = Some(5875710.0), converted = None, serialNumberMis = Some("83045912"), serialNumberConv = None,
        coefCorr = Some(0.3), cau_int_mis = Some(6), cau_int_cor = Some(5),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0825/01791490343_01178580997_IGMG0306_20200820151954_30.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "15441000288457", date = Some(format.parse("2020-05-04")), readType = Some('E'),
        measure = Some(5875710.0), converted = Some(1.7781092E7), serialNumberMis = Some("83045912"), serialNumberConv = Some("MIT0030009227736"),
        coefCorr = Some(0.4), cau_int_mis = Some(6), cau_int_cor = Some(5),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0825/01791490343_01178580997_IGMG0306_20200820151954_30.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(service = "RGL", pdr = "15441000288457", date = Some(format.parse("2020-05-05")),
        measure = Some(5875710.0), converted = Some(1.7781092E7), serialNumberMis = Some("83045912"), serialNumberConv = Some("MIT0030009227736"), collected = Some("AC"), motivation = Some(2),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0609/01791490343_01178580997_202006_RGL0055_20200609142447_62.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(format.parse("2019-12-17"), format.parse("2100-05-03"), "15441000288457", None, Some("NO"), Some("SI"), Some(8), Some(9))
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(format.parse("2013-02-15"), format.parse("2100-05-03"), "15441000288457", "T2", "T2E1", "", 11, "034027", 2020, "3926656", "11")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)
    consumptions.foreach(println)
    consumptions.foreach(c => {
      print(c.nCoeffCorrezione);
      print(" - ");
      println(c)
    })
    consumptions.foreach(f => {
      Assert.assertEquals(f.tipoCoeff, CoeffLabel.CM.toString)
    })

    val c1 = consumptions.filter(_.pdr == "15441000288457")

    //coeff TML - IGMGPRE c1_pre (30Apr - 01Mag)
    Assert.assertEquals("TML", c1.head.startService)
    Assert.assertEquals("IGMGPRE", c1.head.endService)
    Assert.assertEquals("2020-04-30", format.format(c1.head.startSegment))
    Assert.assertEquals("2020-05-01", format.format(c1.head.endSegment))
    Assert.assertEquals(0.1, c1.head.nCoeffCorrezione.get, 0)

    //coeff IGMGPOST - RGL c2_pre (01Mag - 02Mag)
    Assert.assertEquals("IGMGPOST", c1(1).startService)
    Assert.assertEquals("RGL", c1(1).endService)
    Assert.assertEquals("2020-05-01", format.format(c1(1).startSegment))
    Assert.assertEquals("2020-05-02", format.format(c1(1).endSegment))
    Assert.assertEquals(0.3, c1(1).nCoeffCorrezione.get, 0)

    //coeff RGL - RGL c2_pre (02Mag - 03Mag)
    Assert.assertEquals("RGL", c1(2).startService)
    Assert.assertEquals("RGL", c1(2).endService)
    Assert.assertEquals("2020-05-02", format.format(c1(2).startSegment))
    Assert.assertEquals("2020-05-03", format.format(c1(2).endSegment))
    Assert.assertEquals(0.3, c1(2).nCoeffCorrezione.get, 0)

    //coeff RGL - IGMGPRE c2_pre (03Mag - 04Mag)
    Assert.assertEquals("RGL", c1(3).startService)
    Assert.assertEquals("IGMGPRE", c1(3).endService)
    Assert.assertEquals("2020-05-03", format.format(c1(3).startSegment))
    Assert.assertEquals("2020-05-04", format.format(c1(3).endSegment))
    Assert.assertEquals(0.3, c1(3).nCoeffCorrezione.get, 0)

    //coeff IGMGPOST - RGL c2_pre (04Mag - 05Mag)
    Assert.assertEquals("IGMGPOST", c1(4).startService)
    Assert.assertEquals("RGL", c1(4).endService)
    Assert.assertEquals("2020-05-04", format.format(c1(4).startSegment))
    Assert.assertEquals("2020-05-05", format.format(c1(4).endSegment))
    Assert.assertEquals(0.4, c1(4).nCoeffCorrezione.get, 0)
  }

  /**
   * Scenario 3 bis del paragrafo 6.3 del documento. Un Im1 poi un Igmg poi altre misure per un pdr
   * e Igmg poi Im1 poi misure pe un altro pdr
   */
  def testExecuteIm1Igmg(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2016-05-01")

    val measures = Environment.getSparkContext.parallelize(List(
      Tgl(service = "TGL", pdr = "05260200744780", date = Some(format.parse("2019-06-05")), readType = Some('E'), isValid = Some("SI"),
        measure = Some(427846.0), converted = Some(161411.0), serialNumberMis = Some("59020747"), serialNumberConv = Some("1108007195"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2019/0707/12883450152_12883420155_201906_TGL0050_20190707043754_90.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(service = "IM1PRE", pdr = "05260200744780", date = Some(format.parse("2019-06-06")), readType = Some('E'),
        measure = Some(427845.0), converted = Some(161411.0), serialNumberMis = Some("59020747"), serialNumberConv = Some("1108007195"),
        coefCorr = Some(0.1), cau_int_mis = Some(6), cau_int_cor = Some(3), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2020/0713/12883450152_12883420155_201906_IGMG0306_20200713160137_462.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "05260200744780", date = Some(format.parse("2019-06-06")), readType = Some('E'),
        measure = Some(0.0), converted = Some(161411.0), serialNumberMis = Some("16761709"), serialNumberConv = Some("1108007195"),
        coefCorr = Some(0.2), cau_int_mis = Some(6), cau_int_cor = Some(3), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2020/0713/12883450152_12883420155_201906_IGMG0306_20200713160137_462.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(service = "IGMGPRE", pdr = "05260200744780", date = Some(format.parse("2019-06-19")), readType = Some('E'),
        measure = Some(2.0), converted = Some(161411.0), serialNumberMis = Some("16761709"), serialNumberConv = Some("1108007195"),
        coefCorr = Some(0.3), cau_int_mis = Some(6), cau_int_cor = Some(5), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2020/0402/12883450152_12883420155_201906_IGMG0306_20200402163043_62.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "05260200744780", date = Some(format.parse("2019-06-19")), readType = Some('E'),
        measure = Some(2.0), converted = Some(161411.0), serialNumberMis = Some("16761709"), serialNumberConv = Some("1108007195"),
        coefCorr = Some(0.4), cau_int_mis = Some(6), cau_int_cor = Some(5), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2020/0402/12883450152_12883420155_201906_IGMG0306_20200402163043_62.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(service = "TGL", pdr = "05260200744780", date = Some(format.parse("2019-06-20")), readType = Some('E'), isValid = Some("SI"),
        measure = Some(2.0), converted = Some(161411.0), serialNumberMis = Some("16761709"), serialNumberConv = Some("1108007195"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2019/0707/12883450152_12883420155_201906_TGL0050_20190707043754_90.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),

      Rsl(service = "RSL", pdr = "00594202093219", date = Some(format.parse("2019-04-01")),
        measure = Some(3680.0), converted = None, serialNumberMis = Some("SMGR034115517615"), serialNumberConv = None, collected = Some("S"), motivation = Some(1), local_file = Some("/mnt/isilonshare1/GAS_INJ_RECUPERO/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12300020158/2019/1107/05608890488_12300020158_201910_RSL0400_20191107143000_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(service = "IGMGPRE", pdr = "00594202093219", date = Some(format.parse("2019-10-31")), readType = Some('E'),
        measure = Some(3911.0), converted = Some(0.0), serialNumberMis = Some("SMGR034115517615"), serialNumberConv = None,
        coefCorr = Some(0.5), cau_int_mis = Some(3), cau_int_cor = None, local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2020/0526/05608890488_06655971007_IGMG0306_20200526225540_591.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "00594202093219", date = Some(format.parse("2019-10-31")), readType = Some('E'),
        measure = Some(1.0), converted = None, serialNumberMis = Some("FIOR034119419709"), serialNumberConv = None,
        coefCorr = Some(0.6), cau_int_mis = Some(3), cau_int_cor = None, local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2020/0526/05608890488_06655971007_IGMG0306_20200526225540_591.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(service = "IM1PRE", pdr = "00594202093219", date = Some(format.parse("2019-11-25")), readType = Some('S'),
        measure = Some(113.0), converted = Some(0.0), serialNumberMis = Some("FIOR034119419709"), serialNumberConv = None,
        coefCorr = Some(0.7), cau_int_mis = Some(3), cau_int_cor = None, local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2020/0526/05608890488_06655971007_IGMG0306_20200526220839_6058.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "00594202093219", date = Some(format.parse("2019-11-25")), readType = Some('S'),
        measure = Some(2.0), converted = None, serialNumberMis = Some("MIT0032013026454"), serialNumberConv = None,
        coefCorr = Some(0.8), cau_int_mis = Some(3), cau_int_cor = None, local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2020/0526/05608890488_06655971007_IGMG0306_20200526220839_6058.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tml(service = "TML", pdr = "00594202093219", date = Some(format.parse("2019-11-30")), readType = Some('E'), isValid = Some("SI"),
        measure = Some(15.0), converted = None, serialNumberMis = Some("MIT0032013026454"), serialNumberConv = None, local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2019/1206/05608890488_06655971007_201911_TML0050_20191206114500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)

    )).map(_.asInstanceOf[Flow])

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(format.parse("2019-06-06"), format.parse("2020-03-26"), "05260200744780", None, None, Some("SI"), Some(6), Some(9)),
      RcuGasMassivoTech(format.parse("2020-03-27"), format.parse("2100-03-26"), "05260200744780", Some(1.1), Some("NO"), Some("SI"), Some(6), Some(9)),

      RcuGasMassivoTech(format.parse("2019-11-26"), format.parse("2019-11-25"), "00594202093219", Some(1.2), Some("SI"), Some("NO"), Some(7), None),
      RcuGasMassivoTech(format.parse("2019-10-31"), format.parse("2019-11-25"), "00594202093219", Some(1.3), Some("SI"), Some("NO"), Some(5), None),
      RcuGasMassivoTech(format.parse("2019-11-26"), format.parse("2100-11-26"), "00594202093219", Some(1.4), Some("SI"), Some("NO"), Some(7), None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(format.parse("2001-08-03"), format.parse("2100-06-05"), "05260200744780", "C1", "C1E1", "", 13, "015146", 2020, "16490", "11"),

      RcuGasMassivo(format.parse("1995-07-06"), format.parse("2019-03-31"), "00594202093219", "C3", "C3D1", "", 19, "048033", 2019, "1375", "22"),
      RcuGasMassivo(format.parse("2019-04-01"), format.parse("2100-08-03"), "00594202093219", "C3", "C3D1", "SWG", 19, "048033", 2020, "1190", "22")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)
    consumptions.map(v => (v.pdr, v.startService, v.endService, v.nCoeffCorrezione, v.startvalue, v.endvalue)).foreach(println)
    consumptions.foreach(c => {
      print(c.nCoeffCorrezione);
      print(" - ");
      println(c)
    })
    consumptions.foreach(f => {
      Assert.assertEquals(f.tipoCoeff, CoeffLabel.CM.toString)
    })

    val c1 = consumptions.filter(_.pdr == "05260200744780")

    //coeff TGL - IM1PRE c1_pre (05Giu - 06Giu)
    Assert.assertEquals("TGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals("2019-06-05", format.format(c1.head.startSegment))
    Assert.assertEquals("2019-06-06", format.format(c1.head.endSegment))
    Assert.assertEquals(0.1, c1.head.nCoeffCorrezione.get, 0)

    //coeff IM1POST - IGMGPRE c2_pre (06Giu - 19Giu)
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("IGMGPRE", c1(1).endService)
    Assert.assertEquals("2019-06-06", format.format(c1(1).startSegment))
    Assert.assertEquals("2019-06-19", format.format(c1(1).endSegment))
    Assert.assertEquals(0.3, c1(1).nCoeffCorrezione.get, 0)

    //coeff IGMGPOST - TGL c2_post (19Giu - 20Giu)
    Assert.assertEquals("IGMGPOST", c1(2).startService)
    Assert.assertEquals("TGL", c1(2).endService)
    Assert.assertEquals("2019-06-19", format.format(c1(2).startSegment))
    Assert.assertEquals("2019-06-20", format.format(c1(2).endSegment))
    Assert.assertEquals(0.4, c1(2).nCoeffCorrezione.get, 0)


    val c2 = consumptions.filter(_.pdr == "00594202093219")

    //coeff RSL IGMGPRE c1_pre (01Apr - 31Ott)
    Assert.assertEquals("RSL", c2.head.startService)
    Assert.assertEquals("IGMGPRE", c2.head.endService)
    Assert.assertEquals("2019-04-01", format.format(c2.head.startSegment))
    Assert.assertEquals("2019-10-31", format.format(c2.head.endSegment))
    Assert.assertEquals(0.5, c2.head.nCoeffCorrezione.get, 0)

    //coeff IGMGPOST IM1PRE c2_pre (31Ott - 25Nov)
    Assert.assertEquals("IGMGPOST", c2(1).startService)
    Assert.assertEquals("IM1PRE", c2(1).endService)
    Assert.assertEquals("2019-10-31", format.format(c2(1).startSegment))
    Assert.assertEquals("2019-11-25", format.format(c2(1).endSegment))
    Assert.assertEquals(0.7, c2(1).nCoeffCorrezione.get, 0)

    //coeff IM1POST TML c2_post (25Nov - 30Nov)
    Assert.assertEquals("IM1POST", c2(2).startService)
    Assert.assertEquals("TML", c2(2).endService)
    Assert.assertEquals("2019-11-25", format.format(c2(2).startSegment))
    Assert.assertEquals("2019-11-30", format.format(c2(2).endSegment))
    Assert.assertEquals(0.8, c2(2).nCoeffCorrezione.get, 0)
  }


  /**
   * Scenario 4 bis del paragrafo 6.3 del documento. Due Un Im1 due misure e un Igmg
   */
  def testExecuteForPCMeasuresIm1IGMGCoeffCor(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2016-05-01")
    val measures = Environment.getSparkContext.parallelize(List(
      Tml(service = "TML", pdr = "15441000288457", date = Some(format.parse("2020-04-30")), readType = Some('A'), isValid = None,
        measure = Some(4875709.0), converted = Some(2.7781128E7), serialNumberMis = Some("83045912"), serialNumberConv = Some("MIT0030009227736"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0609/01791490343_01178580997_202006_RGL0055_20200609142447_62.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(service = "IM1PRE", pdr = "15441000288457", date = Some(format.parse("2020-05-01")), readType = Some('E'),
        measure = Some(5875703.0), converted = Some(1.7781092E7), serialNumberMis = Some("83045912"), serialNumberConv = Some("MIT0030009227736"),
        coefCorr = Some(0.1), cau_int_mis = Some(6), cau_int_cor = Some(3),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0604/01791490343_01178580997_IGMG0306_20200604151741_1307.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(service = "IM1POST", pdr = "15441000288457", date = Some(format.parse("2020-05-01")), readType = Some('E'),
        measure = Some(5875703.0), converted = None, serialNumberMis = Some("83045912"), serialNumberConv = None,
        coefCorr = Some(0.2), cau_int_mis = Some(6), cau_int_cor = Some(3),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0604/01791490343_01178580997_IGMG0306_20200604151741_1307.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(service = "RGL", pdr = "15441000288457", date = Some(format.parse("2020-05-02")),
        measure = Some(5875707.0), converted = Some(1.7781116E7), serialNumberMis = Some("83045912"), serialNumberConv = Some("MIT0030009227736"), collected = Some("AC"), motivation = Some(2),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0609/01791490343_01178580997_202006_RGL0055_20200609142447_62.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(service = "RGL", pdr = "15441000288457", date = Some(format.parse("2020-05-03")),
        measure = Some(5875709.0), converted = Some(1.7781128E7), serialNumberMis = Some("83045912"), serialNumberConv = Some("MIT0030009227736"), collected = Some("AC"), motivation = Some(2),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0609/01791490343_01178580997_202006_RGL0055_20200609142447_62.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPre(service = "IGMGPRE", pdr = "15441000288457", date = Some(format.parse("2020-05-04")), readType = Some('E'),
        measure = Some(5875710.0), converted = None, serialNumberMis = Some("83045912"), serialNumberConv = None,
        coefCorr = Some(0.3), cau_int_mis = Some(6), cau_int_cor = Some(5),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0825/01791490343_01178580997_IGMG0306_20200820151954_30.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      IgmgPost(service = "IGMGPOST", pdr = "15441000288457", date = Some(format.parse("2020-05-04")), readType = Some('E'),
        measure = Some(5875710.0), converted = Some(1.7781092E7), serialNumberMis = Some("83045912"), serialNumberConv = Some("MIT0030009227736"),
        coefCorr = Some(0.4), cau_int_mis = Some(6), cau_int_cor = Some(5),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0825/01791490343_01178580997_IGMG0306_20200820151954_30.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(service = "RGL", pdr = "15441000288457", date = Some(format.parse("2020-05-05")),
        measure = Some(5875710.0), converted = Some(1.7781092E7), serialNumberMis = Some("83045912"), serialNumberConv = Some("MIT0030009227736"), collected = Some("AC"), motivation = Some(2),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2020/0609/01791490343_01178580997_202006_RGL0055_20200609142447_62.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(format.parse("2019-12-17"), format.parse("2100-05-03"), "15441000288457", None, Some("NO"), Some("SI"), Some(8), Some(9))
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(format.parse("2013-02-15"), format.parse("2100-05-03"), "15441000288457", "T2", "T2E1", "", 11, "034027", 2020, "3926656", "11")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)
    consumptions.foreach(println)
    consumptions.foreach(c => {
      print(c.nCoeffCorrezione);
      print(" - ");
      println(c)
    })
    consumptions.foreach(f => {
      Assert.assertEquals(f.tipoCoeff, CoeffLabel.CM.toString)
    })

    val c1 = consumptions.filter(_.pdr == "15441000288457")

    //coeff TML - IM1PRE c1_pre (30Apr - 01Mag)
    Assert.assertEquals("TML", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals("2020-04-30", format.format(c1.head.startSegment))
    Assert.assertEquals("2020-05-01", format.format(c1.head.endSegment))
    Assert.assertEquals(0.1, c1.head.nCoeffCorrezione.get, 0)

    //coeff IM1POST - RGL c2_pre (01Mag - 02Mag)
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("RGL", c1(1).endService)
    Assert.assertEquals("2020-05-01", format.format(c1(1).startSegment))
    Assert.assertEquals("2020-05-02", format.format(c1(1).endSegment))
    Assert.assertEquals(0.3, c1(1).nCoeffCorrezione.get, 0)

    //coeff RGL - RGL c2_pre (02Mag - 03Mag)
    Assert.assertEquals("RGL", c1(2).startService)
    Assert.assertEquals("RGL", c1(2).endService)
    Assert.assertEquals("2020-05-02", format.format(c1(2).startSegment))
    Assert.assertEquals("2020-05-03", format.format(c1(2).endSegment))
    Assert.assertEquals(0.3, c1(2).nCoeffCorrezione.get, 0)

    //coeff RGL - IGMGPRE c2_pre (03Mag - 04Mag)
    Assert.assertEquals("RGL", c1(3).startService)
    Assert.assertEquals("IGMGPRE", c1(3).endService)
    Assert.assertEquals("2020-05-03", format.format(c1(3).startSegment))
    Assert.assertEquals("2020-05-04", format.format(c1(3).endSegment))
    Assert.assertEquals(0.3, c1(3).nCoeffCorrezione.get, 0)

    //coeff IGMGPOST - RGL c2_pre (04Mag - 05Mag)
    Assert.assertEquals("IGMGPOST", c1(4).startService)
    Assert.assertEquals("RGL", c1(4).endService)
    Assert.assertEquals("2020-05-04", format.format(c1(4).startSegment))
    Assert.assertEquals("2020-05-05", format.format(c1(4).endSegment))
    Assert.assertEquals(0.4, c1(4).nCoeffCorrezione.get, 0)
  }

}

object CoeffCorrControllerTestIntegrazioneIgmg {
  val rcuGasProfilo = Environment.getSparkContext.parallelize(List(
    RcuGasProfilo(n_id_pdr = "11", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None"),
    RcuGasProfilo(n_id_pdr = "22", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None"),
    RcuGasProfilo(n_id_pdr = "33", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None"),
    RcuGasProfilo(n_id_pdr = "44", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None"),
    RcuGasProfilo(n_id_pdr = "55", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None")
  ))*/
}