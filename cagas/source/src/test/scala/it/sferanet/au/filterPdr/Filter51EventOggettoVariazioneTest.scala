package it.sferanet.au.filterPdr

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.model.Flow
import it.sferanet.au.model.periodico.Tgl
import it.sferanet.au.schema.{CaFinalSchema, PdrMassivoSchema, RcuGasMassivoPSchema, SettleGasGasTdsSchema}
import it.sferanet.au.utilities.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.time.LocalDate

class Filter51EventOggettoVariazioneTest extends EnvironmentSparkTest {
  val filter5 = Filter51EventOggettoVariazioneDummy()

  def testGetPdrs(): Unit = {
    val pdrsRdd = filter5.getPdrs.cache()

    Assert.assertEquals(3, pdrsRdd.count())
    Assert.assertEquals(0, pdrsRdd.filter(_.equals("cod_pdr_3")).count())
    Assert.assertEquals(0, pdrsRdd.filter(_.equals("cod_pdr_4")).count())
    Assert.assertEquals(1, pdrsRdd.filter(_.equals("cod_pdr_5")).count())
    Assert.assertEquals(1, pdrsRdd.filter(_.equals("cod_pdr_11")).count())
    Assert.assertEquals(1, pdrsRdd.filter(_.equals("cod_pdr_12")).count())
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


    Environment.setProperty("filterPdr.mode", "oggettoVariazione")
    val filteredFlowsOggettoVariazione = filter5.filter(measures).cache()

    Assert.assertEquals(1, filteredFlowsOggettoVariazione.count())
    Assert.assertEquals(0, filteredFlowsOggettoVariazione.filter(_.pdr == "cod_pdr_1").count())
    Assert.assertEquals(0, filteredFlowsOggettoVariazione.filter(_.pdr == "cod_pdr_2").count())
    Assert.assertEquals(0, filteredFlowsOggettoVariazione.filter(_.pdr == "cod_pdr_3").count())
    Assert.assertEquals(0, filteredFlowsOggettoVariazione.filter(_.pdr == "cod_pdr_4").count())
    Assert.assertEquals(1, filteredFlowsOggettoVariazione.filter(_.pdr == "cod_pdr_5").count())
    Assert.assertEquals(0, filteredFlowsOggettoVariazione.filter(_.pdr == "cod_pdr_11").count())
    Assert.assertEquals(0, filteredFlowsOggettoVariazione.filter(_.pdr == "cod_pdr_12").count())

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
    Assert.assertEquals(0, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "cod_pdr_11").count())
    Assert.assertEquals(0, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "cod_pdr_12").count())
  }

  case class Filter51EventOggettoVariazioneDummy() extends Filter51EventOggettoVariazione {

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

      val today = LocalDate.now()

      List(
        ("cod_pdr_11", today.toString),
        ("cod_pdr_12", today.toString),
        ("cod_pdr_3", today.plusDays(4).toString), //not considered since greater than today
        ("cod_pdr_3", today.toString),
        ("cod_pdr_4", today.minusYears(2).toString), //should not pass since not created between past 1st July and today
        ("cod_pdr_5", today.toString)
      ).toDF(
        RcuGasMassivoPSchema.t_codice_pdr,
        RcuGasMassivoPSchema.d_data_inizio_for
      )
    }

    override def getCaFinal: DataFrame = {
      val sqlCtx = Environment.getSqlContext
      import sqlCtx.implicits._
      val today = LocalDate.now()
      val currentThermalYear = getCurrentThermalYear(today)
      val pastThermalYear = currentThermalYear - 1
      List(
        (currentThermalYear, "cod_pdr_1"),
        (currentThermalYear, "cod_pdr_2"),
        (currentThermalYear, "cod_pdr_3"),
        (currentThermalYear, "cod_pdr_3"),
        (currentThermalYear, "cod_pdr_4"),
        (pastThermalYear, "cod_pdr_5")
      ).toDF(
        CaFinalSchema.anno_competenza,
        CaFinalSchema.codice_pdr
      )
    }
  }

}
