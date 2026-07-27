package it.sferanet.au.controller.ca

import it.sferanet.au.EnvironmentSparkTest/*
import it.sferanet.au.controller.ca.ConsumptionControllerTestIntegrazioneIgmg._
import it.sferanet.au.model.periodico.Tgl
import it.sferanet.au.model.prestazionale.{IgmgPost, IgmgPre}
import it.sferanet.au.model.rettifica.Rgl
import it.sferanet.au.model._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.junit.Assert

import java.text.SimpleDateFormat*/

class ConsumptionControllerTestIntegrazioneIgmg extends EnvironmentSparkTest {/*
  Environment.setProperty("z.sup.date", "2020-05-31")
  Environment.setProperty("z.inf.date", "2017-01-06")

  /**
   * Se cau_int_mis= 1,2,3,4, null e cau_int_cor = (non valorizzato, 1)
   *
   *
   * per la parte Pre-Intervento non viene effettuato il check con l’RCUGAS,
   * per la parte Post-Intervento viene effettuato il check con RCUGAS
   *
   * PRE IGMG                                              * PRE MISURA
   * Let_correttore se presente altrimenti                * Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IGMG pre                  * let_tot_prel * Coeff_corr IGMG pre
   *
   * GRUPPO_MIS_INT       * PRE_CONV                * POST IGMG                  * POST MISURA
   *
   * SI             SI, NO o NULL              let_misuratore                let_tot_prel
   * NULL                    NULL              let_misuratore                let_tot_prel
   * S                 NO                      let_misuratore                let_tot_prel
   * NO                      let_misuratore                let_tot_prel
   *
   * NO              SI                        let correttore                let_tot_conv
   * NULL            SI                        let correttore                let_tot_conv
   * N               SI                        let correttore                let_tot_conv
   * SI                        let correttore                let_tot_conv
   *
   * NO              NO                    let_misuratore * coeff        let_tot_prel * coeff
   * N               NO                    let_misuratore * coeff        let_tot_prel * coeff
   */
  def testmis1234NullcorNull1(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2017-01-06")
    //Testo tutte le combinazioni di queste casistiche
    val cau_int_mis = List(Some(1), Some(2), Some(3), Some(4), None)
    val cau_int_cor = List(None, Some(1))

    // combinazioni (GRUPPO_MIS_INT,PRE_CONV) per cui per il segmento Post considero segnante Prelevata
    val rcu_misint_preconv_prel: List[(Option[String], Option[String])] = List((Some("SI"), None), (Some("SI"), Some("NO")),
      (Some("SI"), Some("SI")), (None, None), (Some("S"), Some("NO")), (Some(""), Some("NO")))

    // combinazioni (GRUPPO_MIS_INT,PRE_CONV) per cui per il segmento Post considero segnante Convertita
    val rcu_misint_preconv_conv: List[(Option[String], Option[String])] = List((Some("NO"), Some("SI")),
      (None, Some("SI")), (Some("N"), Some("SI")), (Some(""), Some("SI")))

    // combinazioni (GRUPPO_MIS_INT,PRE_CONV) per cui per il segmento Post considero segnante Prelevata*coeff
    val rcu_misint_preconv_pk: List[(Option[String], Option[String])] = List(
      (Some("NO"), Some("NO")), (Some("N"), Some("NO")))

    for (
      mis <- cau_int_mis;
      cor <- cau_int_cor
    ) {

      val measuresval = measures(mis, cor)

      //per il pre devo considerare la convertita se presente (altrimenti prel*k),
      // Per il Post devo considerare la prelevata
      for (misint_preconv <- rcu_misint_preconv_prel) {

        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresval, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

        //let_correttore presente e let_tot_conv presente pdr 1,
        val c1 = consumptions.filter(_.pdr == "1")

        //RGL - IGMGPRE Convertita - Convertita
        checkConsumption(c1.head, "RGL", "IGMGPRE", 50, 900)

        //IGMGPOST - TGL Prelevata - Prelevata
        checkConsumption(c1(1), "IGMGPOST", "TGL", 1, 4.648104E7)

        //let_correttore assente e let_tot_conv presente pdr 2,
        val c2 = consumptions.filter(_.pdr == "2")

        //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
        checkConsumption(c2.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)

        //IGMGPOST - TGL Prelevata - Prelevata
        checkConsumption(c2(1), "IGMGPOST", "TGL", 1, 4)

        //let_correttore presente e let_tot_conv assente pdr 3,
        val c3 = consumptions.filter(_.pdr == "3")

        //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
        checkConsumption(c3.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)

        //IGMGPOST - TGL Prelevata - Prelevata
        checkConsumption(c3(1), "IGMGPOST", "TGL", 1, 4)

        //let_correttore assente e let_tot_conv assente pdr 4,
        val c4 = consumptions.filter(_.pdr == "4")

        //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
        checkConsumption(c4.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)

        //IGMGPOST - TGL Prelevata - Prelevata
        checkConsumption(c4(1), "IGMGPOST", "TGL", 1, 7)

      }

      //per il pre devo considerare la convertita se presente (altrimenti prel*k),
      // Per il Post devo considerare la convertita (Deve essere presente)
      for (misint_preconv <- rcu_misint_preconv_conv) {

        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresval, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

        //let_correttore presente e let_tot_conv presente pdr 1,
        val c1 = consumptions.filter(_.pdr == "1")

        //RGL - IGMGPRE Convertita - Convertita
        checkConsumption(c1.head, "RGL", "IGMGPRE", 50, 900)

        //IGMGPOST - TGL Convertita - Convertita
        checkConsumption(c1(1), "IGMGPOST", "TGL", 10, 20)


        //let_correttore assente e let_tot_conv presente pdr 2,
        val c2 = consumptions.filter(_.pdr == "2")
        //let_correttore presente e let_tot_conv assente pdr 3,
        val c3 = consumptions.filter(_.pdr == "3")
        //let_correttore assente e let_tot_conv assente pdr 4,
        val c4 = consumptions.filter(_.pdr == "4")

        //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
        checkConsumption(c2.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)

        //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
        checkConsumption(c3.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)

        //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
        checkConsumption(c4.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)


        //in questo caso non devo testare pdr2, pdr3, pdr4 sez POST perchè l'assenza del segnale convertito
        // è incongruente con i dati tecnici in RCUGAS

        //IGMGPOST - TGL Prelevata*coeff - Prelevata*coeff
        checkConsumption(c2(1), "IGMGPOST", "TGL", 1.2, 4 * 1.2)
        //IGMGPOST - TGL Prelevata*coeff - Prelevata*coeff
        checkConsumption(c3(1), "IGMGPOST", "TGL", 1.2, 4 * 1.2)
        //IGMGPOST - TGL Prelevata*coeff - Prelevata*coeff
        checkConsumption(c4(1), "IGMGPOST", "TGL", 1.2, 7 * 1.2)

      }

      //per il pre devo considerare la convertita se presente (altrimenti prel*k),
      // Per il Post devo considerare la prelevata*coeff
      for (misint_preconv <- rcu_misint_preconv_pk) {

        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresval, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

        //let_correttore presente e let_tot_conv presente pdr 1,
        val c1 = consumptions.filter(_.pdr == "1")

        //RGL - IGMGPRE Convertita - Convertita
        checkConsumption(c1.head, "RGL", "IGMGPRE", 50, 900)

        //IGMGPOST - TGL Prelevata*k - Prelevata*k
        checkConsumption(c1(1), "IGMGPOST", "TGL", 1 * 1.2, 4.648104E7 * 1.2)

        //let_correttore assente e let_tot_conv presente pdr 2,
        val c2 = consumptions.filter(_.pdr == "2")

        //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
        checkConsumption(c2.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)

        //IGMGPOST - TGL Prelevata*coeff - Prelevata*coeff
        checkConsumption(c2(1), "IGMGPOST", "TGL", 1 * 1.2, 4 * 1.2)

        //let_correttore presente e let_tot_conv assente pdr 3,
        val c3 = consumptions.filter(_.pdr == "3")

        //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
        checkConsumption(c3.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)

        //IGMGPOST - TGL Prelevata*coeff - Prelevata*coeff
        checkConsumption(c3(1), "IGMGPOST", "TGL", 1 * 1.2, 4 * 1.2)

        //let_correttore assente e let_tot_conv assente pdr 4,
        val c4 = consumptions.filter(_.pdr == "4")

        //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
        checkConsumption(c4.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)

        //IGMGPOST - TGL Prelevata*coeff - Prelevata*coeff
        checkConsumption(c4(1), "IGMGPOST", "TGL", 1 * 1.2, 7 * 1.2)

      }


    }

  }

  /**
   * Se cau_int_mis= 1,2,3,4, null e se cau_int_cor = 2 Rimozione virtuale del correttore
   *
   * PRE IGMG                                              PRE MISURA
   * Let_correttore se presente altrimenti                Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IGMG pre                 let_tot_prel * Coeff_corr IGMG pre
   *
   * POST IGMG                                             POST MISURA
   * let_misuratore * Coeff_corr IGMG pre                 let_tot_prel * Coeff_corr IGMG pre
   *
   */
  def testmis1234Nullcor2(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2017-01-06")
    //Testo tutte le combinazioni di queste casistiche
    val cau_int_mis = List(Some(1), Some(2), Some(3), Some(4), None)
    val cau_int_cor = List(Some(2))

    for (
      mis <- cau_int_mis;
      cor <- cau_int_cor
    ) {

      val measuresval = measures(mis, cor)

      val result = new ConsumptionController().execute(measuresval, rcuTech(), rcusCa, rcuGasProfilo)
      val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

      //let_correttore presente e let_tot_conv presente pdr 1,
      val c1 = consumptions.filter(_.pdr == "1")

      //RGL - IGMGPRE Convertita - Convertita
      checkConsumption(c1.head, "RGL", "IGMGPRE", 50, 900)

      //IGMGPOST - TGL Prelevata*coeff - Prelevata*coeff
      checkConsumption(c1(1), "IGMGPOST", "TGL", 1 * 1.2, 4.648104E7 * 1.2)

      //let_correttore assente e let_tot_conv presente pdr 2,
      val c2 = consumptions.filter(_.pdr == "2")

      //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
      checkConsumption(c2.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)

      //IGMGPOST - TGL Prelevata*coeff - Prelevata*coeff
      checkConsumption(c2(1), "IGMGPOST", "TGL", 1 * 1.2, 4 * 1.2)

      //let_correttore presente e let_tot_conv assente pdr 3,
      val c3 = consumptions.filter(_.pdr == "3")

      //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
      checkConsumption(c3.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)

      //IGMGPOST - TGL Prelevata*coeff - Prelevata*coeff
      checkConsumption(c3(1), "IGMGPOST", "TGL", 1 * 1.2, 4 * 1.2)


      //let_correttore assente e let_tot_conv assente pdr 4,
      val c4 = consumptions.filter(_.pdr == "4")

      //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
      checkConsumption(c4.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)

      //IGMGPOST - TGL Prelevata*coeff - Prelevata*coeff
      checkConsumption(c4(1), "IGMGPOST", "TGL", 1 * 1.2, 7 * 1.2)

    }
  }

  /**
   * Se cau_int_mis= 1,2,3,4, null e se cau_int_cor = 3 Installazione virtuale del correttore
   *
   * PRE IGMG                                              PRE MISURA
   * let_misuratore * Coeff_corr IGMG pre                 let_tot_prel * Coeff_corr IGMG pre
   *
   * POST IGMG                                             POST MISURA
   * Let_correttore se presente altrimenti                Let_tot_conv se presente altrimenti
   * let_misuratore * Coeff_corr IGMG pre                 let_tot_prel * Coeff_corr IGMG pre
   *
   */
  def testmis1234Nullcor34(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2017-01-06")
    //Testo tutte le combinazioni di queste casistiche
    val cau_int_mis = List(Some(1), Some(2), Some(3), Some(4), None)
    val cau_int_cor = List(Some(3), Some(4))

    for (
      mis <- cau_int_mis;
      cor <- cau_int_cor
    ) {

      val measuresval = measures(mis, cor)

      val result = new ConsumptionController().execute(measuresval, rcuTech(), rcusCa, rcuGasProfilo)
      val consumptions = result.flatMap(_._2._1).collect.sortBy(_.startSegment.getTime)

      //let_correttore presente e let_tot_conv presente pdr 1,
      val c1 = consumptions.filter(_.pdr == "1")

      //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
      checkConsumption(c1.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)

      //IGMGPOST - TGL Convertita - Convertita
      checkConsumption(c1(1), "IGMGPOST", "TGL", 10, 20)

      //let_correttore assente e let_tot_conv presente pdr 2,
      val c2 = consumptions.filter(_.pdr == "2")

      //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
      checkConsumption(c2.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)

      //let_correttore presente e let_tot_conv assente pdr 3,
      val c3 = consumptions.filter(_.pdr == "3")

      //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
      checkConsumption(c3.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)

      //let_correttore assente e let_tot_conv assente pdr 4,
      val c4 = consumptions.filter(_.pdr == "4")

      //RGL - IGMGPRE Prelevata*coeff - Prelevata*coeff
      checkConsumption(c4.head, "RGL", "IGMGPRE", 5 * 1.1, 9 * 1.1)

      //in questo caso non devo testare pdr2, pdr3, pdr4 sez POST perchè l'assenza del segnale convertito
      // è incongruente con i dati tecnici

      //IGMGPOST - TGL Prelevata*coeff - Prelevata*coeff
      checkConsumption(c2(1), "IGMGPOST", "TGL", 1 * 1.2, 4 * 1.2)
      //IGMGPOST - TGL Prelevata*coeff - Prelevata*coeff
      checkConsumption(c3(1), "IGMGPOST", "TGL", 1 * 1.2, 4 * 1.2)
      //IGMGPOST - TGL Prelevata*coeff - Prelevata*coeff
      checkConsumption(c4(1), "IGMGPOST", "TGL", 1 * 1.2, 7 * 1.2)


    }
  }

}

object ConsumptionControllerTestIntegrazioneIgmg {
  //let_correttore presente e let_tot_conv presente pdr 1,
  //let_correttore assente e let_tot_conv presente pdr 2
  //let_correttore presente e let_tot_conv assente pdr 3
  //let_correttore assente e let_tot_conv assente pdr 4
  def measures(cau_int_mis: Option[Int] = Some(1), cau_int_cor: Option[Int] = Some(1)): RDD[Flow] = {
    Environment.getSparkContext.parallelize(List(
      Rgl(pdr = "1", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      IgmgPre(pdr = "1", service = "IGMGPRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None,
        cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      IgmgPost(pdr = "1", service = "IGMGPOST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "1", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4.648104E7), converted = Some(20),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "2", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = Some(50), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      IgmgPre(pdr = "2", service = "IGMGPRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      IgmgPost(pdr = "2", service = "IGMGPOST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "2", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = Some(30),
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "3", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      IgmgPre(pdr = "3", service = "IGMGPRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = Some(900), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      IgmgPost(pdr = "3", service = "IGMGPOST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = Some(10), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "3", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(4), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false),

      Rgl(pdr = "4", service = "RGL", date = Some(format.parse("2017-01-05")), collected = Some("AC"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(5), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, motivation = Some(0), d_caricamento = None, isNewRoute = false),
      IgmgPre(pdr = "4", service = "IGMGPRE", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(9), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("80066949"), local_file = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        coefCorr = Some(1.1), d_caricamento = None, isNewRoute = false),
      IgmgPost(pdr = "4", service = "IGMGPOST", date = Some(format.parse("2017-01-06")), readType = Some('E'), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = None, cau_int_mis = cau_int_mis, cau_int_cor = cau_int_cor,
        coefCorr = Some(1.2), d_caricamento = None, isNewRoute = false),
      Tgl(service = "TGL", pdr = "4", date = Some(format.parse("2019-10-26")), readType = Some('E'), isValid = Some("SI"), pivaDistr = None, pivaUtente = None, ammissibilita = None,
        measure = Some(7), converted = None,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false)

    )).map(_.asInstanceOf[Flow])
  }

  def rcuTech(gruppo_mis_int: Option[String] = Some(""), pre_conv: Option[String] = Some("")): RDD[RcuGasMassivoTech] = Environment.getSparkContext.parallelize(List(
    RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
      t_misuratore_integrato = gruppo_mis_int, t_pre_conv = pre_conv, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
    RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
      t_misuratore_integrato = gruppo_mis_int, t_pre_conv = pre_conv, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
    RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "3", n_coeff_correzione = Some(2),
      t_misuratore_integrato = gruppo_mis_int, t_pre_conv = pre_conv, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
    RcuGasMassivoTech(startDate = format.parse("2017-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "4", n_coeff_correzione = Some(2),
      t_misuratore_integrato = gruppo_mis_int, t_pre_conv = pre_conv, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
  ))

  val rcuGasProfilo = Environment.getSparkContext.parallelize(List(
    RcuGasProfilo(n_id_pdr = "11", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None"),
    RcuGasProfilo(n_id_pdr = "22", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None"),
    RcuGasProfilo(n_id_pdr = "33", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None"),
    RcuGasProfilo(n_id_pdr = "44", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None"),
    RcuGasProfilo(n_id_pdr = "55", d_data_inizio = Some(Constants.getFormatter("yyyy-MM-dd").parse("1900-01-01")), d_data_fine = Some(Constants.getFormatter("yyyy-MM-dd").parse("2100-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None")
  ))

  val format = new SimpleDateFormat("yyyy-MM-dd")

  val rcusCa = Environment.getSparkContext.parallelize(List(
    RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
      t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
    RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
      t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22"),
    RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "3", cat_uso = null, t_cod_profilo = null,
      t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "33"),
    RcuGasMassivo(startDate = format.parse("2017-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "4", cat_uso = null, t_cod_profilo = null,
      t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "44"),

    RcuGasMassivo(startDate = format.parse("2019-01-16"), endDate = format.parse("2040-01-01"), t_codice_pdr = "00881407814753", cat_uso = "T1", t_cod_profilo = "T1X1", t_processo = "", id_regione_climatica = 11, t_comune_istat_pdr = "005005", t_anno_termico = 2020, n_prelievo_annuo = "2461769", n_id_pdr = "55")
  ))

  def checkConsumption(c: Consumption, startService: String, endService: String, startValue: Double, endValue: Double): Unit = {
    Assert.assertEquals(startService, c.startService)
    Assert.assertEquals(endService, c.endService)
    Assert.assertEquals(startValue, c.startvalue, 0)
    Assert.assertEquals(endValue, c.endvalue, 0)
  }*/
}
