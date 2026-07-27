package it.eng.au.mid.pubblicazione

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.flow.pubblicazione.{PredisposizioneFunzioni, PredisposizioneMid1Flow}
import it.eng.au.mid.model.file.pubblicazione.MidAlphaValoriModel
import it.eng.au.mid.model.flow.DailyConsumptionModel
import it.eng.au.mid.model.hive.mid.{Mid1DettaglioModel, MidContatoriModel}
import it.eng.au.mid.schema.flow.calcolo.DailyConsumptionSchema
import it.eng.au.mid.schema.hive.mid.{Mid1DettaglioSchema, MidContatoriSchema}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.Assert

class PredisposizioneFunzioniTest extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  def testFiltraDailyConsumption(): Unit = {
    val dailyConsumption = Seq(
      DailyConsumptionModel(pdr = "1", annomese = "202301", executionid = 1L),
      DailyConsumptionModel(pdr = "1", annomese = "202301", executionid = 2L),
      DailyConsumptionModel(pdr = "2", annomese = "202302", executionid = 1L),
      DailyConsumptionModel(pdr = "3", annomese = "202303", executionid = 2L)
    ).toDS()

    val midContatori = Seq(
      MidContatoriModel(annomese = "202301", executionid_daily_consumption = 1L),
      MidContatoriModel(annomese = "202303", executionid_daily_consumption = 2L),
      // righe doppie per verificare che la distinct funzioni
      MidContatoriModel(annomese = "202303", executionid_daily_consumption = 2L),
      MidContatoriModel(annomese = "202301", executionid_daily_consumption = 1L)
    ).toDS()

    val result = PredisposizioneFunzioni.filtraDailyConsumption(dailyConsumption, midContatori).cache()

    Assert.assertEquals(2, result.count())
    Assert.assertEquals(0, result.where(col(DailyConsumptionSchema.annomese) === "202302").count())
  }

  def testFiltraMidContatoriDaAnalizzare(): Unit = {
    val annomeseDa = "202301"
    val annomeseA = "202302"
    val sogliaContatore = 1
    val executionId = 100L
    val midContatoriDs = Seq(
      //ok
      MidContatoriModel(pdr = "1", contatore = 2, stato = CostantiMid.STATO_FORZATO, processo_tracciatura = CostantiMid.PROCESSO_AGG, annomese = annomeseDa, executionid_tracciatura = executionId),
      MidContatoriModel(pdr = "2", contatore = 2, stato = CostantiMid.STATO_VALIDO, processo_tracciatura = CostantiMid.PROCESSO_AGG, annomese = annomeseDa, executionid_tracciatura = executionId),
      MidContatoriModel(pdr = "3", contatore = 2, stato = CostantiMid.STATO_VALIDO, processo_tracciatura = CostantiMid.PROCESSO_AGG, annomese = annomeseA, executionid_tracciatura = executionId),
      // scarti
      // condizione non valida su contatore
      MidContatoriModel(pdr = "x", contatore = 1, stato = CostantiMid.STATO_VALIDO, processo_tracciatura = CostantiMid.PROCESSO_AGG, annomese = annomeseDa, executionid_tracciatura = executionId),
      // condizione non valida, executionId precedente: deve prendere il PDR 1 con stato forzato
      MidContatoriModel(pdr = "1", contatore = 1, stato = CostantiMid.STATO_ESCLUSO_FORZATO, processo_tracciatura = CostantiMid.PROCESSO_AGG, annomese = annomeseDa, executionid_tracciatura = 1L),
      // condizione non valida su stato
      MidContatoriModel(pdr = "x", contatore = 1, stato = CostantiMid.STATO_ESCLUSO_FORZATO, processo_tracciatura = CostantiMid.PROCESSO_AGG, annomese = annomeseDa, executionid_tracciatura = executionId),
      // condizione non valida su annomese
      MidContatoriModel(pdr = "x", contatore = 1, stato = CostantiMid.STATO_VALIDO, processo_tracciatura = CostantiMid.PROCESSO_AGG, annomese = "202212", executionid_tracciatura = executionId),
      MidContatoriModel(pdr = "x", contatore = 1, stato = CostantiMid.STATO_VALIDO, processo_tracciatura = CostantiMid.PROCESSO_AGG, annomese = "202303", executionid_tracciatura = executionId),
      // condizione non valida su tipo processo (non dovrebbe mai accadere che ci sia un executionID per processi SBG)
      MidContatoriModel(pdr = "x", contatore = 1, stato = CostantiMid.STATO_VALIDO, processo_tracciatura = CostantiMid.PROCESSO_SBG, annomese = annomeseA, executionid_tracciatura = executionId)
    ).toDS()

    val result = PredisposizioneFunzioni.filtraMidContatoriDaAnalizzare(midContatoriDs, annomeseDa, annomeseA, sogliaContatore).cache()

    Assert.assertEquals(3, result.count())
    Assert.assertEquals(0, result.where(col(MidContatoriSchema.pdr) === "x").count())
    Assert.assertEquals(CostantiMid.STATO_FORZATO, result.where(col(MidContatoriSchema.pdr) === "1").head().stato)
  }

  def testAggiungiInformazioniAnagrafica(): Unit = {
    val annomese = "202301"
    val executionId = 666L
    val dailyConsumption = Seq(
      DailyConsumptionModel(pdr = "1", annomese = annomese, pivadistr = "pivadistr", pivaudd = "pivaudd", codremi = "codremi", classemisuratore = "G4"),
      DailyConsumptionModel(pdr = "2", annomese = annomese, pivadistr = "pivadistr2", pivaudd = "pivaudd2", codremi = "codremi2", classemisuratore = "G4"),
      // condizioni non sufficienti
      DailyConsumptionModel(pdr = "3", annomese = annomese, pivadistr = null, pivaudd = "pivaudd2", codremi = "codremi2", classemisuratore = "G4"),
      DailyConsumptionModel(pdr = "4", annomese = annomese, pivadistr = "pivadistr", pivaudd = null, codremi = "codremi2", classemisuratore = "G4"),
      DailyConsumptionModel(pdr = "5", annomese = annomese, pivadistr = "pivadistr", pivaudd = "", codremi = null, classemisuratore = "G4"),
      DailyConsumptionModel(pdr = "6", annomese = annomese, pivadistr = "pivadistr", pivaudd = "", codremi = "", classemisuratore = null)
    ).toDS()
    val midContatori = Seq(
      MidContatoriModel(pdr = "1", contatore = 2, annomese = annomese, executionid_tracciatura = executionId),
      MidContatoriModel(pdr = "2", contatore = 3, annomese = annomese, executionid_tracciatura = executionId),
      // condizioni non sufficienti
      MidContatoriModel(pdr = "3", contatore = 3, annomese = annomese, executionid_tracciatura = executionId),
      MidContatoriModel(pdr = "4", contatore = 3, annomese = annomese, executionid_tracciatura = executionId),
      MidContatoriModel(pdr = "5", contatore = 3, annomese = annomese, executionid_tracciatura = executionId),
      MidContatoriModel(pdr = "6", contatore = 3, annomese = annomese, executionid_tracciatura = executionId)
    ).toDS()

    val expected_1 = Mid1DettaglioModel(pdr = "1", contatore = 2, piva_id = "pivadistr", piva_udd = "pivaudd",
      cod_remi = "codremi", gdm = "G4", executionid_mid_contatori = executionId, annomese = annomese,
      executionid = Environment.executionId)
    val expected_2 = Mid1DettaglioModel(pdr = "2", contatore = 3, piva_id = "pivadistr2", piva_udd = "pivaudd2",
      cod_remi = "codremi2", gdm = "G4", executionid_mid_contatori = executionId, annomese = annomese,
      executionid = Environment.executionId)

    val result = PredisposizioneFunzioni.aggiungiInformazioniAnagrafica(midContatori, dailyConsumption, Environment.executionId).cache()

    Assert.assertEquals(2, result.count())
    Assert.assertEquals(expected_1, result.where(col(Mid1DettaglioSchema.pdr) === "1").head())
    Assert.assertEquals(expected_2, result.where(col(Mid1DettaglioSchema.pdr) === "2").head())
  }

  def testEscludiElementi_ListVuota(): Unit = {
    val midContatori = Seq(MidContatoriModel(pdr = "1", annomese = "202301")).toDS()
    val valori = List()
    val result = PredisposizioneFunzioni.escludiElementi(midContatori, MidContatoriSchema.annomese, valori)
    Assert.assertEquals(1, result.count())
  }

  def testEscludiElementi_RitornoVuoto(): Unit = {
    val midContatori = Seq(MidContatoriModel(pdr = "1", annomese = "202301")).toDS()
    val valori = List("202301")
    val result = PredisposizioneFunzioni.escludiElementi(midContatori, MidContatoriSchema.annomese, valori)
    Assert.assertEquals(0, result.count())
  }

  def testEscludiElementi(): Unit = {
    val midContatori = Seq(
      MidContatoriModel(pdr = "1", annomese = "202301"),
      MidContatoriModel(pdr = "1", annomese = "202302")
    ).toDS()
    val valori = List("202301")
    val result = PredisposizioneFunzioni.escludiElementi(midContatori, MidContatoriSchema.annomese, valori)
    Assert.assertEquals(1, result.count())
  }

  def testCalcolaAlpha(): Unit = {
    val midContatoriArricchiti = Seq(
      Mid1DettaglioModel(pdr = "1", gdm = "G1"),
      Mid1DettaglioModel(pdr = "2", gdm = "G2"),
      Mid1DettaglioModel(pdr = "3", gdm = "G3"),
      // da escludere nel risultato
      Mid1DettaglioModel(pdr = "4", gdm = "XX")
    ).toDS()

    val alphaDs = Seq(
      MidAlphaValoriModel(gdm = "G1", alpha = 1),
      MidAlphaValoriModel(gdm = "G2", alpha = 2),
      MidAlphaValoriModel(gdm = "G3", alpha = 3)
    ).toDS()

    val result = PredisposizioneFunzioni.calcolaAlpha(midContatoriArricchiti, alphaDs).cache()

    Assert.assertEquals(1, result.where(col(Mid1DettaglioSchema.pdr) === "1").head().alpha)
    Assert.assertEquals(2, result.where(col(Mid1DettaglioSchema.pdr) === "2").head().alpha)
    Assert.assertEquals(3, result.where(col(Mid1DettaglioSchema.pdr) === "3").head().alpha)
    Assert.assertEquals(0, result.where(col(Mid1DettaglioSchema.pdr) === "4").count())
  }

}
