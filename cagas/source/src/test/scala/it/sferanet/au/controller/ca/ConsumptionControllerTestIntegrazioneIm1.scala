package it.sferanet.au.controller.ca

import it.sferanet.au.EnvironmentSparkTest/*
import it.sferanet.au.controller.ca.ConsumptionControllerTestIntegrazioneIm1.rcuGasProfilo
import it.sferanet.au.model.periodico.Tgl
import it.sferanet.au.model.prestazionale.{Im1Post, Im1Pre}
import it.sferanet.au.model.rettifica.Rgl
import it.sferanet.au.model.{Flow, RcuGasMassivo, RcuGasMassivoTech, RcuGasProfilo}
import it.sferanet.au.utilities.{Constants, Environment}
import org.junit.Assert

import java.text.SimpleDateFormat*/

class ConsumptionControllerTestIntegrazioneIm1 extends EnvironmentSparkTest {/*
  Environment.setProperty("z.sup.date", "2020-05-31")
  Environment.setProperty("z.inf.date", "2017-01-06")

  /**
   * Se cau_int_mis= 1 e cau_int_cor = non valorizzato (installazione solo misuratore)
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   *
   * Se dovesse essere presente una situazione “Pre”, vale la tabella:
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   */
  def testmis1corNull(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None, cau_int_mis = Some(1), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, cau_int_mis = Some(1), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None, cau_int_mis = Some(1), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, cau_int_mis = Some(1), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None, cau_int_mis = Some(1), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, cau_int_mis = Some(1), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None, cau_int_mis = Some(1), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, cau_int_mis = Some(1), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])

    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 1 e cau_int_cor = 1 (installazione misuratore e correttore)
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   */
  def testmis1cor1(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 1 e cau_int_cor = 2 (installazione misuratore e sostituzione correttore)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis1cor2(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 1 e cau_int_cor = 3 (installazione misuratore e rimozione virtuale convertitore)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis1cor3(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(1 * 1.2, c1(1).startvalue, 0)
    Assert.assertEquals(4.648104E7 * 1.2, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 1 e cau_int_cor = 4 (installazione misuratore e rimozione fisica convertitore)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis1cor4(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(1 * 1.2, c1(1).startvalue, 0)
    Assert.assertEquals(4.648104E7 * 1.2, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 2 e cau_int_cor = Non valorizzato (rimozione misuratore e assenza convertitore)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis2corNull(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 2 e cau_int_cor = 1 (rimozione, installazione)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis2cor1(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 2 e cau_int_cor = 2 (rimozione, sostituzione)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis2cor2(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 2 e cau_int_cor = 3 (rimozione misuratore e rimozione virtuale convertitore)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis2cor3(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(1 * 1.2, c1(1).startvalue, 0)
    Assert.assertEquals(4.648104E7 * 1.2, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 2 e cau_int_cor = 4 (rimozione, rimozione)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis2cor4(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(1 * 1.2, c1(1).startvalue, 0)
    Assert.assertEquals(4.648104E7 * 1.2, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 3 e cau_int_cor = Non valorizzato
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis3corNull(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 4 e cau_int_cor = Non valorizzato
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis4corNull(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 5 e cau_int_cor = Non valorizzato
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis5corNull(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 6 e cau_int_cor = Non valorizzato
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis6corNull(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = None,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = None,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 3 e cau_int_cor = 1 (sost. misuratore ed inst. convertitore)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis3cor1(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 4 e cau_int_cor = 1 (sost. misuratore ed inst. convertitore)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis4cor1(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 5 e cau_int_cor = 1 (sost. misuratore ed inst. convertitore)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis5cor1(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 6 e cau_int_cor = 1 (sost. misuratore ed inst. convertitore)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis6cor1(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(1),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(1),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 3 e cau_int_cor = 2 (sostituzione del misuratore e del convertitore)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis3cor2(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 4 e cau_int_cor = 2 (sostituzione del misuratore e del convertitore)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis4cor2(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 5 e cau_int_cor = 2 (sostituzione del misuratore e del convertitore)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis5cor2(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 6 e cau_int_cor = 2 (sostituzione del misuratore e del convertitore)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis6cor2(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 3 e cau_int_cor = 3 (sostituzione misuratore, rimozione virt conv.)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis3cor3(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(1 * 1.2, c1(1).startvalue, 0)
    Assert.assertEquals(4.648104E7 * 1.2, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 4 e cau_int_cor = 3 (sostituzione misuratore, rimozione virt conv.)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis4cor3(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(1 * 1.2, c1(1).startvalue, 0)
    Assert.assertEquals(4.648104E7 * 1.2, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 5 e cau_int_cor = 3 (sostituzione misuratore, rimozione virt conv.)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis5cor3(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(1 * 1.2, c1(1).startvalue, 0)
    Assert.assertEquals(4.648104E7 * 1.2, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 6 e cau_int_cor = 3 (sostituzione misuratore, rimozione virt conv.)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis6cor3(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(3),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(1 * 1.2, c1(1).startvalue, 0)
    Assert.assertEquals(4.648104E7 * 1.2, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 3 e cau_int_cor = 4 (sostituzione misuratore, rimozione conv.)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis3cor4(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(1 * 1.2, c1(1).startvalue, 0)
    Assert.assertEquals(4.648104E7 * 1.2, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 4 e cau_int_cor = 4 (sostituzione misuratore, rimozione conv.)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis4cor4(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(1 * 1.2, c1(1).startvalue, 0)
    Assert.assertEquals(4.648104E7 * 1.2, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 5 e cau_int_cor = 4 (sostituzione misuratore, rimozione conv.)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis5cor4(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(1 * 1.2, c1(1).startvalue, 0)
    Assert.assertEquals(4.648104E7 * 1.2, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 6 e cau_int_cor = 4 (sostituzione misuratore, rimozione conv.)
   *
   * PRE IM1                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis6cor4(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(4),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(4),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Convertita - Convertita
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(50, c1.head.startvalue, 0)
    Assert.assertEquals(900, c1.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(1 * 1.2, c1(1).startvalue, 0)
    Assert.assertEquals(4.648104E7 * 1.2, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 1 e cau_int_cor = 5 (allineamento correttore)
   *
   * PRE IM1                                              * PRE MISURA
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis1cor5(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(1), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(5 * 1.1, c1.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 2 e cau_int_cor = 5 (allineamento correttore)
   *
   * PRE IM1                                              * PRE MISURA
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis2cor5(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(2), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(5 * 1.1, c1.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 3 e cau_int_cor = 5 (allineamento correttore)
   *
   * PRE IM1                                              * PRE MISURA
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis3cor5(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(5 * 1.1, c1.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 4 e cau_int_cor = 5 (allineamento correttore)
   *
   * PRE IM1                                              * PRE MISURA
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis4cor5(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(4), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(5 * 1.1, c1.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 5 e cau_int_cor = 5 (allineamento correttore)
   *
   * PRE IM1                                              * PRE MISURA
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis5cor5(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    //let_correttore assente e let_tot_conv presente pdr 2
    //let_correttore presente e let_tot_conv assente pdr 3
    //let_correttore assente e let_tot_conv assente pdr 4
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(5), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(5 * 1.1, c1.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Se cau_int_mis= 6 e cau_int_cor = 5 (allineamento correttore)
   *
   * PRE IM1                                              * PRE MISURA
   * let_misuratore * Coeff_corr IM1 pre                  * let_tot_prel * Coeff_corr IM1 pre
   *
   * POST IM1                                             * POST MISURA
   * Let_correttore se presente altrimenti                * post Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IM1                      * let_tot_prel * Coeff_corr IM1 post
   */
  def testmis6cor5(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    val measures = Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "2", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "2", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "3", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "3", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "4", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(5),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "4", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(5),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(5 * 1.1, c1.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c1.head.endvalue, 0)
    //IM1POST - TGL Convertita - Convertita
    Assert.assertEquals("IM1POST", c1(1).startService)
    Assert.assertEquals("TGL", c1(1).endService)
    Assert.assertEquals(10, c1(1).startvalue, 0)
    Assert.assertEquals(20, c1(1).endvalue, 0)

    //let_correttore assente e let_tot_conv presente pdr 2,
    val c2 = consumptions.filter(_.pdr == "2")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c2.head.startService)
    Assert.assertEquals("IM1PRE", c2.head.endService)
    Assert.assertEquals(5 * 1.1, c2.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c2.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c2(1).startService)
    Assert.assertEquals("TGL", c2(1).endService)
    Assert.assertEquals(1 * 1.2, c2(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c2(1).endvalue, 0)

    //let_correttore presente e let_tot_conv assente pdr 3,
    val c3 = consumptions.filter(_.pdr == "3")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c3.head.startService)
    Assert.assertEquals("IM1PRE", c3.head.endService)
    Assert.assertEquals(5 * 1.1, c3.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c3.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c3(1).startService)
    Assert.assertEquals("TGL", c3(1).endService)
    Assert.assertEquals(1 * 1.2, c3(1).startvalue, 0)
    Assert.assertEquals(4 * 1.2, c3(1).endvalue, 0)

    //let_correttore assente e let_tot_conv assente pdr 3,
    val c4 = consumptions.filter(_.pdr == "4")
    //RGL - IM1PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("RGL", c4.head.startService)
    Assert.assertEquals("IM1PRE", c4.head.endService)
    Assert.assertEquals(5 * 1.1, c4.head.startvalue, 0)
    Assert.assertEquals(9 * 1.1, c4.head.endvalue, 0)
    //IM1POST - TGL Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c4(1).startService)
    Assert.assertEquals("TGL", c4(1).endService)
    Assert.assertEquals(1 * 1.2, c4(1).startvalue, 0)
    Assert.assertEquals(7 * 1.2, c4(1).endvalue, 0)
  }

  /**
   * Testo che tra due IM1 consecutivi con logiche che vanno in conflitto prevale la logica del secondo IM1
   * (il più recente)
   */
  def testTwoConsecutiveConflictingIm1(): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    //let_correttore presente e let_tot_conv presente pdr 1,
    val measures = Environment.getSparkContext.parallelize(List(
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-05")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(800), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(2),
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-05")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(1000), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(3), cau_int_cor = Some(2),
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(5),
        coefCorr = Some(1.3), d_caricamento = None, isNewRoute = false),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None,
        cau_int_mis = Some(6), cau_int_cor = Some(5),
        coefCorr = Some(1.4), d_caricamento = None, isNewRoute = false)
    )).map(_.asInstanceOf[Flow])


    measures.foreach(println)

    val rcuTech = Environment.getSparkContext.parallelize(List(
      RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
        t_misuratore_integrato = None, t_pre_conv = Some("SI"), n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
    ))

    val rcusCa = Environment.getSparkContext.parallelize(List(
      RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
        t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),

      RcuGasMassivo(format.parse("2019-01-16"), format.parse("2040-01-01"), "00881407814753", "T1", "T1X1", "", 11, "005005", 2020, "2461769", "55")
    ))

    val result = new ConsumptionController().execute(measures, rcuTech, rcusCa, rcuGasProfilo)

    val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

    consumptions.foreach(c => {
      print(c.nCoeffCorrezione); print(" - "); println(c)
    })

    //let_correttore presente e let_tot_conv presente pdr 1,
    val c1 = consumptions.filter(_.pdr == "1")
    //IM1 POST - IM1 PRE Prelevata*coeff - Prelevata*coeff
    Assert.assertEquals("IM1POST", c1.head.startService)
    Assert.assertEquals("IM1PRE", c1.head.endService)
    Assert.assertEquals(1 * 1.3, c1.head.startvalue, 0)
    Assert.assertEquals(9 * 1.3, c1.head.endvalue, 0)


  }
}

object ConsumptionControllerTestIntegrazioneIm1 {
  val rcuGasProfilo = Environment.getSparkContext.parallelize(List(
    RcuGasProfilo(n_id_pdr = "11", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None"),
    RcuGasProfilo(n_id_pdr = "22", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None"),
    RcuGasProfilo(n_id_pdr = "33", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None"),
    RcuGasProfilo(n_id_pdr = "44", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None"),
    RcuGasProfilo(n_id_pdr = "55", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None")
  ))*/
}
