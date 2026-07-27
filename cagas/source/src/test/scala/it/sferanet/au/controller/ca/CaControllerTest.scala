package it.sferanet.au.controller.ca

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.model.CAMethods.{Daily, Monthly}
import it.sferanet.au.model._
import it.sferanet.au.utilities.Environment
import org.junit.Assert

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

//@Ignore
class CaControllerTest extends EnvironmentSparkTest {/*
  val format = new SimpleDateFormat("yyyy-MM-dd")

  def testCodProf(): Unit = {


    val consumptions = IndexedSeq(
      Consumption("09760000066315", "TGL", "TGL", new Timestamp(format.parse("2020-05-30").getTime), new Timestamp(format.parse("2020-05-31").getTime), 120187.0, 135439.0,
        None, None, ConsumptionErrorStates.None, None, Some("NO"), None, Some("SI"), None, Some("C2X1"), "0", null, null),
      Consumption("09760000066315", "TGL", "TGL", new Timestamp(format.parse("2019-05-31").getTime), new Timestamp(format.parse("2020-05-30").getTime), 120187.0, 120187.0,
        None, None, ConsumptionErrorStates.None, None, Some("NO"), None, Some("SI"), None, Some("C2X1"), "0", null, null),
      Consumption("09760000066315", "TGL", "TGL", new Timestamp(format.parse("2019-05-28").getTime), new Timestamp(format.parse("2019-05-31").getTime), 120187.0, 120187.0,
        None, None, ConsumptionErrorStates.None, None, Some("NO"), None, Some("SI"), None, Some("C2X1"), "0", null, null)
    )

    val rcuGasMassivoTech = Iterable(
      RcuGasMassivoTech(format.parse("2020-02-28"), format.parse("2100-05-30"), "09760000066315", None, Some("NO"), Some("SI"), Some(6), Some(7))
    )

    val rcuGasMassivo = Iterable(
      RcuGasMassivo(format.parse("2016-09-02"), format.parse("2017-01-26"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2017, "19209", "150604000000217200"),
      RcuGasMassivo(format.parse("2017-01-27"), format.parse("2100-01-26"), "09760000066315", "C2", "C2X1", "", 21, "042038", 2020, "0", "150604000000217200"),
      RcuGasMassivo(format.parse("1990-01-02"), format.parse("2016-08-31"), "09760000066315", "", "C1D1", "", 21, "042038", 2016, "18288", "150604000000217200"),
      RcuGasMassivo(format.parse("2016-09-01"), format.parse("2016-09-01"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2017, "19209", "150604000000217200")
    )

    val rcusProfilo = Iterable(
      RcuGasProfilo(n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_anno = 2017, t_cod_profilo = "C1D1", t_cod_cat_uso = "C1", t_cod_classe_prelievo = "", d_data_inizio = Some(new Date(0L)), d_data_fine = Some(new Date(Long.MaxValue)))
    )

    val cons = Environment.getSparkContext.parallelize(List(("09760000066315", (consumptions, rcuGasMassivoTech, rcuGasMassivo, 3.541935483870968, rcusProfilo))))

    val weights = Environment.getSparkContext.parallelize(List(
      Weights(format.parse("2018-01-01"), 0.33062123, 0.33062123, Some(21), "C2X1", 0.980173516021203),
      Weights(format.parse("2018-01-01"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203)
    ))

    val lookupZonaClimatica = Environment.getSparkContext.broadcast(scala.collection.Map[String, String](
      "042038" -> "D"
    ))

    val tds = Environment.getSparkContext.broadcast(scala.collection.Map[String, Tds]())

    val result = new CaController().execute(cons, weights, lookupZonaClimatica, tds).cache

    /*print result*/
    val iter = result.map(r => r._2).collect()
    for (i <- iter) {
      i.foreach { case (consumption, d, value, parameter) => {
        println(consumption.pdr, d, value, parameter.caMethods)
      }
      }
    }
    /*print result*/
    Assert.assertEquals("C2X1", result.collect().head._3)
    Assert.assertEquals(ProfStdMode.Calculated, result.collect().head._4)
    result.collect().head._2.foreach(x => Assert.assertEquals("C1D1", x._4.codiceProfilo))
  }

  def testFormula2Daily() = {

    val consumptions = IndexedSeq(
      getCons("09760000066315", "TGL", "TGL", "2020-05-30", "2020-05-31"),
      getCons("09760000066315", "TGL", "TGL", "2020-05-29", "2020-05-30"),
      getCons("09760000066315", "TGL", "TGL", "2020-05-28", "2020-05-29"),
      getCons("09760000066315", "TGL", "TGL", "2010-05-27", "2020-05-28"),
      getCons("09760000066315", "FDD", "FDD", "2020-05-26", "2020-05-27"),
      getCons("09760000066315", "FUI", "FUI", "2020-05-25", "2020-05-26"),
      getCons("09760000066315", "RSL", "RSL", "2019-05-31", "2020-05-25"),
      getCons("09760000066315", "FDD", "FDD", "2019-05-30", "2019-05-31"))

    val rcuGasProfilo = Iterable(
      RcuGasProfilo(d_data_inizio = Some(format.parse("2016-09-02")), d_data_fine = Some(format.parse("2017-01-26")), n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_cod_cat_uso = "C1", t_cod_profilo = "C1D1", t_anno = 2017, t_cod_classe_prelievo = ""),
      RcuGasProfilo(d_data_inizio = Some(format.parse("2017-01-27")), d_data_fine = Some(format.parse("2100-01-26")), n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_cod_cat_uso = "C2", t_cod_profilo = "C2X1", t_anno = 2020, t_cod_classe_prelievo = ""),
      RcuGasProfilo(d_data_inizio = Some(format.parse("1990-01-02")), d_data_fine = Some(format.parse("2016-08-31")), n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_cod_cat_uso = "", t_cod_profilo = "C1D1", t_anno = 2016, t_cod_classe_prelievo = ""),
      RcuGasProfilo(d_data_inizio = Some(format.parse("2016-09-01")), d_data_fine = Some(format.parse("2016-09-01")), n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_cod_cat_uso = "C1", t_cod_profilo = "C1D1", t_anno = 2017, t_cod_classe_prelievo = "")
    )

    val result = CaController.getCaMethods(consumptions, rcuGasProfilo)

    result.map(r => (r._1.pdr, r._2)) foreach (println)
    result.map(_._2).foreach(m => Assert.assertTrue(m == Daily))


  }

  def testFormula2DailyCalcoloCA() = {
    val consumptions = IndexedSeq(
      getCons("09760000066315", "TGL", "TGL", "2020-05-30", "2020-05-31"),
      getCons("09760000066315", "TGL", "TGL", "2020-05-29", "2020-05-30"),
      getCons("09760000066315", "TGL", "TGL", "2020-05-28", "2020-05-29"),
      getCons("09760000066315", "TGL", "TGL", "2010-05-27", "2020-05-28"),
      getCons("09760000066315", "FDD", "FDD", "2020-05-26", "2020-05-27"),
      getCons("09760000066315", "FUI", "FUI", "2020-05-25", "2020-05-26"),
      getCons("09760000066315", "RSL", "RSL", "2019-05-31", "2020-05-25"),
      getCons("09760000066315", "FDD", "FDD", "2019-05-30", "2019-05-31"))

    val rcuGasMassivoTech = Iterable(
      RcuGasMassivoTech(format.parse("2020-02-28"), format.parse("2100-05-30"), "09760000066315", None, Some("NO"), Some("SI"), Some(6), Some(7))
    )

    val rcuGasMassivo = Iterable(
      RcuGasMassivo(format.parse("2016-09-02"), format.parse("2017-01-26"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2017, "19209", "150604000000217200"),
      RcuGasMassivo(format.parse("2017-01-27"), format.parse("2100-01-26"), "09760000066315", "C2", "C2X1", "", 21, "042038", 2020, "0", "150604000000217200"),
      RcuGasMassivo(format.parse("1990-01-02"), format.parse("2016-08-31"), "09760000066315", "", "C1D1", "", 21, "042038", 2016, "18288", "150604000000217200"),
      RcuGasMassivo(format.parse("2016-09-01"), format.parse("2016-09-01"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2017, "19209", "150604000000217200")
    )

    val rcusProfilo = Iterable(
      RcuGasProfilo(n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_anno = 2017, t_cod_profilo = "C1D1", t_cod_cat_uso = "C1", t_cod_classe_prelievo = "", d_data_inizio = None, d_data_fine = Some(format.parse("2018-01-01"))),
      RcuGasProfilo(n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_anno = 2017, t_cod_profilo = "C2X1", t_cod_cat_uso = "C1", t_cod_classe_prelievo = "", d_data_inizio = Some(format.parse("2018-01-02")), d_data_fine = None)
    )

    val cons = Environment.getSparkContext.parallelize(List(("09760000066315", (consumptions, rcuGasMassivoTech, rcuGasMassivo, 3.541935483870968, rcusProfilo))))

    val weights = Environment.getSparkContext.parallelize(List(
      Weights(format.parse("2018-01-01"), 0.33062123, 0.33062123, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2018-01-01"), 0.8194243536687942, 0.83599928, Some(21), "C2X1", 0.980173516021203)
    ))

    val lookupZonaClimatica = Environment.getSparkContext.broadcast(scala.collection.Map[String, String](
      "042038" -> "D"
    ))

    val tds = Environment.getSparkContext.broadcast(scala.collection.Map[String, Tds]())

    val result = new CaController().execute(cons, weights, lookupZonaClimatica, tds).cache

    val iter = result.map(r => r._2).collect()
    for (i <- iter) {
      i.foreach { case (consumption, d, value, parameter) => Assert.assertTrue(value.toString == "None") }
    }
  }

  def testFormula1Month() = {

    val consumptions = IndexedSeq(
      getCons("09760000066315", "AD3", "AD3", "2020-05-30", "2020-05-31"),
      getCons("09760000066315", "AD3", "AD3", "2020-05-29", "2020-05-30"),
      getCons("09760000066315", "AD4", "AD4", "2020-05-28", "2020-05-29"),
      getCons("09760000066315", "TGL", "TGL", "2010-05-27", "2020-05-28"),
      getCons("09760000066315", "FUI", "FUI", "2020-05-26", "2020-05-27"),
      getCons("09760000066315", "A40R", "A40R", "2020-05-25", "2020-05-26"),
      getCons("09760000066315", "SM1", "SM1", "2019-05-31", "2020-05-25"),
      getCons("09760000066315", "FDD", "FDD", "2019-05-30", "2019-05-31"))

    val rcuGasProfilo = Iterable(
      RcuGasProfilo(d_data_inizio = Some(format.parse("2016-09-02")), d_data_fine = Some(format.parse("2017-01-26")), n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_cod_cat_uso = "C1", t_cod_profilo = "C1D1", t_anno = 2017, t_cod_classe_prelievo = ""),
      RcuGasProfilo(d_data_inizio = Some(format.parse("2017-01-27")), d_data_fine = Some(format.parse("2100-01-26")), n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_cod_cat_uso = "C2", t_cod_profilo = "C2X1", t_anno = 2020, t_cod_classe_prelievo = ""),
      RcuGasProfilo(d_data_inizio = Some(format.parse("1990-01-02")), d_data_fine = Some(format.parse("2016-08-31")), n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_cod_cat_uso = "", t_cod_profilo = "C1D1", t_anno = 2016, t_cod_classe_prelievo = ""),
      RcuGasProfilo(d_data_inizio = Some(format.parse("2016-09-01")), d_data_fine = Some(format.parse("2016-09-01")), n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_cod_cat_uso = "C1", t_cod_profilo = "C1D1", t_anno = 2017, t_cod_classe_prelievo = "")
    )

    val result = CaController.getCaMethods(consumptions, rcuGasProfilo)

    result.map(r => (r._1.pdr, r._2)) foreach (println)
    result.map(_._2).foreach(m => Assert.assertTrue(m == Monthly))


  }

  def testFormula1MonthCalcoloCA() = {
    val consumptions = IndexedSeq(
      getCons("09760000066315", "AD3", "AD3", "2020-05-30", "2020-05-31"),
      getCons("09760000066315", "AD3", "AD3", "2020-05-29", "2020-05-30"),
      getCons("09760000066315", "AD4", "AD4", "2020-05-28", "2020-05-29"),
      getCons("09760000066315", "TGL", "TGL", "2010-05-27", "2020-05-28"),
      getCons("09760000066315", "FUI", "FUI", "2020-05-26", "2020-05-27"),
      getCons("09760000066315", "A40R", "A40R", "2020-05-25", "2020-05-26"),
      getCons("09760000066315", "SM1", "SM1", "2019-05-31", "2020-05-25"),
      getCons("09760000066315", "FDD", "FDD", "2019-05-30", "2019-05-31"))

    val rcuGasMassivoTech = Iterable(
      RcuGasMassivoTech(format.parse("2020-02-28"), format.parse("2100-05-30"), "09760000066315", None, Some("NO"), Some("SI"), Some(6), Some(7))
    )

    val rcuGasMassivo = Iterable(
      RcuGasMassivo(format.parse("2016-09-02"), format.parse("2017-01-26"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2017, "19209", "150604000000217200"),
      RcuGasMassivo(format.parse("2017-01-27"), format.parse("2100-01-26"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2020, "0", "150604000000217200"),
      RcuGasMassivo(format.parse("1990-01-02"), format.parse("2016-08-31"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2016, "18288", "150604000000217200"),
      RcuGasMassivo(format.parse("2016-09-01"), format.parse("2016-09-01"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2017, "19209", "150604000000217200")
    )

    val rcusProfilo = Iterable(
      RcuGasProfilo(n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_anno = 2017, t_cod_profilo = "C1D1", t_cod_cat_uso = "C1", t_cod_classe_prelievo = "", d_data_inizio = Some(new Date(0L)), d_data_fine = Some(new Date(Long.MaxValue)))
    )

    val cons = Environment.getSparkContext.parallelize(List(("09760000066315", (consumptions, rcuGasMassivoTech, rcuGasMassivo, 3.541935483870968, rcusProfilo))))

    val weights = Environment.getSparkContext.parallelize(List(
      Weights(format.parse("2020-05-30"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2020-05-29"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2020-05-28"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2020-05-27"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2020-05-26"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2019-12-01"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2020-01-01"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2019-12-01"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2020-01-01"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203)
    ))

    val lookupZonaClimatica = Environment.getSparkContext.broadcast(scala.collection.Map[String, String](
      "042038" -> "D"
    ))

    val tds = Environment.getSparkContext.broadcast(scala.collection.Map[String, Tds]())

    val result = new CaController().execute(cons, weights, lookupZonaClimatica, tds).cache

    val iter = result.map(r => r._2).collect()
    for (i <- iter) {
      i.foreach { case (consumption, d, value, parameter) => {
        //        println(value)
        Assert.assertTrue(value.toString == "None")
      }
      }
    }
  }

  def testFormulaIbrida() = {

    val consumptions = IndexedSeq(
      getCons("09760000066315", "TGL", "TGL", "2020-05-30", "2020-05-31"),
      getCons("09760000066315", "IM1POST", "IM1PRE", "2020-05-29", "2020-05-30"),
      getCons("09760000066315", "AD3", "SM1R", "2020-05-28", "2020-05-29"),
      getCons("09760000066315", "AD2", "A40R", "2010-05-27", "2020-05-28"),
      getCons("09760000066315", "A40", "A40", "2020-05-26", "2020-05-27"),
      getCons("09760000066315", "A01R", "A01R", "2020-05-25", "2020-05-26"),
      getCons("09760000066315", "IM1POST", "IM1PRE", "2019-05-31", "2020-05-25"),
      getCons("09760000066315", "TML", "TML", "2019-05-30", "2019-05-31"))

    val rcuGasProfilo = Iterable(
      RcuGasProfilo(d_data_inizio = Some(format.parse("2016-09-02")), d_data_fine = Some(format.parse("2017-01-26")), n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_cod_cat_uso = "C1", t_cod_profilo = "C1D1", t_anno = 2017, t_cod_classe_prelievo = ""),
      RcuGasProfilo(d_data_inizio = Some(format.parse("2017-01-27")), d_data_fine = Some(format.parse("2100-01-26")), n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_cod_cat_uso = "C2", t_cod_profilo = "C2X1", t_anno = 2020, t_cod_classe_prelievo = ""),
      RcuGasProfilo(d_data_inizio = Some(format.parse("1990-01-02")), d_data_fine = Some(format.parse("2016-08-31")), n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_cod_cat_uso = "", t_cod_profilo = "C1D1", t_anno = 2016, t_cod_classe_prelievo = ""),
      RcuGasProfilo(d_data_inizio = Some(format.parse("2016-09-01")), d_data_fine = Some(format.parse("2016-09-01")), n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_cod_cat_uso = "C1", t_cod_profilo = "C1D1", t_anno = 2017, t_cod_classe_prelievo = "")
    )

    val result = CaController.getCaMethods(consumptions, rcuGasProfilo)

    result.map(r => (r._1.pdr, r._2)) foreach (println)
    result.map(_._2).foreach(m => Assert.assertTrue(m == Monthly || m == Daily))


  }

  def testFormulaIbridaCalcoloCA() = {
    val consumptions = IndexedSeq(
      getCons("09760000066315", "TGL", "TGL", "2020-05-30", "2020-05-31"),
      getCons("09760000066315", "IM1POST", "IM1PRE", "2020-05-29", "2020-05-30"),
      getCons("09760000066315", "AD3", "SM1R", "2020-05-28", "2020-05-29"),
      getCons("09760000066315", "AD2", "A40R", "2010-05-27", "2020-05-28"),
      getCons("09760000066315", "A40", "A40", "2020-05-26", "2020-05-27"),
      getCons("09760000066315", "A01R", "A01R", "2020-05-25", "2020-05-26"),
      getCons("09760000066315", "IM1POST", "IM1PRE", "2019-05-31", "2020-05-25"),
      getCons("09760000066315", "TML", "TML", "2019-05-30", "2019-05-31"))

    val rcuGasMassivoTech = Iterable(
      RcuGasMassivoTech(format.parse("2020-02-28"), format.parse("2100-05-30"), "09760000066315", None, Some("NO"), Some("SI"), Some(6), Some(7))
    )

    val rcuGasMassivo = Iterable(
      RcuGasMassivo(format.parse("2016-09-02"), format.parse("2017-01-26"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2017, "19209", "150604000000217200"),
      RcuGasMassivo(format.parse("2017-01-27"), format.parse("2100-01-26"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2020, "0", "150604000000217200"),
      RcuGasMassivo(format.parse("1990-01-02"), format.parse("2016-08-31"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2016, "18288", "150604000000217200"),
      RcuGasMassivo(format.parse("2016-09-01"), format.parse("2016-09-01"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2017, "19209", "150604000000217200")
    )

    val rcusProfilo = Iterable(
      RcuGasProfilo(n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_anno = 2017, t_cod_profilo = "C1D1", t_cod_cat_uso = "C1", t_cod_classe_prelievo = "", d_data_inizio = Some(new Date(0L)), d_data_fine = Some(new Date(Long.MaxValue)))
    )

    val cons = Environment.getSparkContext.parallelize(List(("09760000066315", (consumptions, rcuGasMassivoTech, rcuGasMassivo, 3.541935483870968, rcusProfilo))))

    val weights = Environment.getSparkContext.parallelize(List(
      Weights(format.parse("2020-05-31"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2020-05-30"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2020-05-29"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2020-05-28"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2020-05-27"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2020-05-26"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2020-01-01"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2019-12-01"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203),
      Weights(format.parse("2020-01-01"), 0.8194243536687942, 0.83599928, Some(21), "C1D1", 0.980173516021203)
    ))

    val lookupZonaClimatica = Environment.getSparkContext.broadcast(scala.collection.Map[String, String](
      "042038" -> "D"
    ))

    val tds = Environment.getSparkContext.broadcast(scala.collection.Map[String, Tds]())

    val result = new CaController().execute(cons, weights, lookupZonaClimatica, tds).cache

    val iter = result.map(r => r._2).collect()
    for (i <- iter) {
      i.foreach { case (consumption, d, value, parameter) => {
        //        println(consumption.pdr, d,value)
        Assert.assertTrue(value.toString == "None")
      }
      }
    }
  }

  def testMissingWeights() = {
    val consumptions = IndexedSeq(
      getCons("09760000066315", "TGL", "TGL", "2020-05-30", "2020-05-31"),
      getCons("09760000066315", "IM1POST", "IM1PRE", "2020-05-29", "2020-05-30"),
      getCons("09760000066315", "AD3", "SM1R", "2020-05-28", "2020-05-29"),
      getCons("09760000066315", "AD2", "A40R", "2010-05-27", "2020-05-28"),
      getCons("09760000066315", "A40", "A40", "2020-05-26", "2020-05-27"),
      getCons("09760000066315", "A01R", "A01R", "2020-05-25", "2020-05-26"),
      getCons("09760000066315", "IM1POST", "IM1PRE", "2019-05-31", "2020-05-25"),
      getCons("09760000066315", "TML", "TML", "2019-05-30", "2019-05-31"))

    val rcuGasMassivoTech = Iterable(
      RcuGasMassivoTech(format.parse("2020-02-28"), format.parse("2100-05-30"), "09760000066315", None, Some("NO"), Some("SI"), Some(6), Some(7))
    )

    val rcuGasMassivo = Iterable(
      RcuGasMassivo(format.parse("2016-09-02"), format.parse("2017-01-26"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2017, "19209", "150604000000217200"),
      RcuGasMassivo(format.parse("2017-01-27"), format.parse("2100-01-26"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2020, "0", "150604000000217200"),
      RcuGasMassivo(format.parse("1990-01-02"), format.parse("2016-08-31"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2016, "18288", "150604000000217200"),
      RcuGasMassivo(format.parse("2016-09-01"), format.parse("2016-09-01"), "09760000066315", "C1", "C1D1", "", 21, "042038", 2017, "19209", "150604000000217200")
    )

    val rcusProfilo = Iterable(
      RcuGasProfilo(n_id_var_profilo = "", n_id_pdr = "150604000000217200", t_anno = 2017, t_cod_profilo = "C1D1", t_cod_cat_uso = "C1", t_cod_classe_prelievo = "", d_data_inizio = Some(new Date(0L)), d_data_fine = Some(new Date(Long.MaxValue)))
    )

    val cons = Environment.getSparkContext.parallelize(List(("09760000066315", (consumptions, rcuGasMassivoTech, rcuGasMassivo, 3.541935483870968, rcusProfilo))))

    val weights = Environment.getSparkContext.parallelize(List(
      Weights(format.parse("2020-05-31"), 0.8194243536687942, 0.83599928, Some(21), "C1D2", 0.980173516021203),
      Weights(format.parse("2020-05-30"), 0.8194243536687942, 0.83599928, Some(21), "C1D2", 0.980173516021203),
      Weights(format.parse("2020-05-29"), 0.8194243536687942, 0.83599928, Some(21), "C1D2", 0.980173516021203),
      Weights(format.parse("2020-05-28"), 0.8194243536687942, 0.83599928, Some(21), "C1D2", 0.980173516021203),
      Weights(format.parse("2020-05-27"), 0.8194243536687942, 0.83599928, Some(21), "C1D2", 0.980173516021203),
      Weights(format.parse("2020-05-26"), 0.8194243536687942, 0.83599928, Some(21), "C1D2", 0.980173516021203),
      Weights(format.parse("2020-01-01"), 0.8194243536687942, 0.83599928, Some(21), "C1D2", 0.980173516021203),
      Weights(format.parse("2019-12-01"), 0.8194243536687942, 0.83599928, Some(21), "C1D2", 0.980173516021203),
      Weights(format.parse("2020-01-01"), 0.8194243536687942, 0.83599928, Some(21), "C1D2", 0.980173516021203)
    ))

    val lookupZonaClimatica = Environment.getSparkContext.broadcast(scala.collection.Map[String, String](
      "042038" -> "D"
    ))

    val tds = Environment.getSparkContext.broadcast(scala.collection.Map[String, Tds]())

    val result = new CaController().execute(cons, weights, lookupZonaClimatica, tds).cache

    val iter = result.map(r => r._2).collect()
    for (i <- iter) {
      i.foreach { case (consumption, d, value, parameter) => {
        //        println(consumption.pdr, d,value)
        Assert.assertTrue(value == CAErrorCode.MissingWeight)
      }
      }
    }
  }

  def getCons(pdr: String, tl: String, tr: String, dl: String, dr: String): Consumption = {
    Consumption(pdr, tl, tr, new Timestamp(format.parse(dl).getTime), new Timestamp(format.parse(dr).getTime), 120187.0, 135439.0,
      None, None, ConsumptionErrorStates.None, None, Some("NO"), None, Some("SI"), None, Some("C1D1"), "0", null, null)
  }*/
}
