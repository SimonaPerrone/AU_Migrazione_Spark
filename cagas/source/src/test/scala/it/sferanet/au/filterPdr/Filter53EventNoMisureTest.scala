package it.sferanet.au.filterPdr

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.model.Flow
import it.sferanet.au.model.periodico.Tgl
import it.sferanet.au.schema.{CaPreFinalSchema, PdrMassivoSchema, RcuGasMassivoPSchema, SettleGasGasTdsSchema}
import it.sferanet.au.utilities.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.time.LocalDate

class Filter53EventNoMisureTest extends EnvironmentSparkTest {
  val filter5 = Filter53EventNoMisureDummy()

  def testGetPdrs(): Unit = {
    val pdrsRdd = filter5.getPdrs.cache()

    Assert.assertEquals(1, pdrsRdd.count())
    Assert.assertEquals(0, pdrsRdd.filter(_.equals("cod_pdr_3")).count())
    Assert.assertEquals(0, pdrsRdd.filter(_.equals("cod_pdr_4")).count())
    Assert.assertEquals(1, pdrsRdd.filter(_.equals("cod_pdr_5")).count())
    Assert.assertEquals(0, pdrsRdd.filter(_.equals("cod_pdr_6")).count())
  }

  def testFilter(): Unit = {
    val measures = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "cod_pdr_1", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "cod_pdr_1", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "cod_pdr_2", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "cod_pdr_3", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "cod_pdr_4", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "cod_pdr_5", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val filteredFlowsOggettoVariazione = filter5.filter(measures).cache()

    Assert.assertEquals(1, filteredFlowsOggettoVariazione.count())
    Assert.assertEquals(0, filteredFlowsOggettoVariazione.filter(_.pdr == "cod_pdr_1").count())
    Assert.assertEquals(0, filteredFlowsOggettoVariazione.filter(_.pdr == "cod_pdr_2").count())
    Assert.assertEquals(0, filteredFlowsOggettoVariazione.filter(_.pdr == "cod_pdr_3").count())
    Assert.assertEquals(0, filteredFlowsOggettoVariazione.filter(_.pdr == "cod_pdr_4").count())
    Assert.assertEquals(1, filteredFlowsOggettoVariazione.filter(_.pdr == "cod_pdr_5").count())
  }

  def testFilterPdrMassivo(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val dummyPdrMassivo: DataFrame = List(
      ("cod_pdr_1", "id1"),
      ("cod_pdr_2", "id26"),
      ("cod_pdr_3", "id13"),
      ("cod_pdr_4", "id64"),
      ("cod_pdr_5", "id50")
    ).toDF(PdrMassivoSchema.codice_pdr, PdrMassivoSchema.n_id_udb)

    val filteredPdrMassivo = filter5.filterPdrMassivo(dummyPdrMassivo).cache()

    Assert.assertEquals(1, filteredPdrMassivo.count())
    Assert.assertEquals(0, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "cod_pdr_1").count())
    Assert.assertEquals(0, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "cod_pdr_2").count())
    Assert.assertEquals(0, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "cod_pdr_3").count())
    Assert.assertEquals(0, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "cod_pdr_4").count())
    Assert.assertEquals(1, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "cod_pdr_5").count())

  }

  case class Filter53EventNoMisureDummy() extends Filter53EventNoMisure {

    override def getSettleGasGasTds: DataFrame = {
      val sqlCtx = Environment.getSqlContext
      import sqlCtx.implicits._

      List(
        ("29/08/20 16:09:1580310593", "cod_pdr_1", true),
        ("29/08/20 16:09:1580310593", "cod_pdr_2", true),
        ("13/08/20 16:28:1578929292", "cod_pdr_3", true),
        ("23/08/20 16:28:1578929292", "cod_pdr_3", true), //duplicate pdr must be considered once
        ("13/07/20 16:28:1578929292", "cod_pdr_4", true), //should not pass since data_creazione< dataGhigliottina
        ("13/07/20 16:28:1578929292", "", false)
      ).toDF(
        SettleGasGasTdsSchema.data_creazione,
        SettleGasGasTdsSchema.cod_pdr,
        SettleGasGasTdsSchema.valid
      )
    }

    override def getRcuGasMassivoP: DataFrame = {
      val sqlCtx = Environment.getSqlContext
      import sqlCtx.implicits._

      val yesterday = LocalDate.now().minusDays(1)

      List(
        ("cod_pdr_11", yesterday.toString, "Y"),
        ("cod_pdr_12", yesterday.toString, "Y"),
        ("cod_pdr_3", yesterday.plusDays(4).toString, "Y"), //not considered since greater than today
        ("cod_pdr_3", yesterday.toString, "Y"),
        ("cod_pdr_4", yesterday.minusYears(2).toString, "Y"), //should not pass since not created between past 1st July and today
        ("cod_pdr_5", yesterday.toString, "Y")
      ).toDF(
        RcuGasMassivoPSchema.t_codice_pdr,
        RcuGasMassivoPSchema.d_data_inizio_for,
        RcuGasMassivoPSchema.t_trattamento
      )
    }

    override def getCaPreFinal: DataFrame = {
      val sqlCtx = Environment.getSqlContext
      import sqlCtx.implicits._
      val today = LocalDate.now()
      val currentThermalYear = getCurrentThermalYear(today)
      val pastThermalYear = currentThermalYear - 1
      List(
        (currentThermalYear, "cod_pdr_1", true, "Y"),
        (currentThermalYear, "cod_pdr_2", false, "Y"),
        (currentThermalYear, "cod_pdr_3", false, "Y"),
        (currentThermalYear, "cod_pdr_3", false, "Y"),
        (currentThermalYear, "cod_pdr_4", true, "Y"),
        (pastThermalYear, "cod_pdr_5", true, "Y"),
        (pastThermalYear, "cod_pdr_6", true, "G")
      ).toDF(
        CaPreFinalSchema.anno_competenza,
        CaPreFinalSchema.codice_pdr,
        CaPreFinalSchema.is_ca_calculated,
        CaPreFinalSchema.trattamento
      )
    }
  }

}
