package it.eng.au.mid.calcolo.common

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.flow.calcolo.CalcoloMidFunzioni
import it.eng.au.mid.flow.calcolo.CalcoloMidFunzioni.maxExecutionIdPerAnnomese
import it.eng.au.mid.model.file.calcolo.{EsclusioniModel, InclusioniModel}
import it.eng.au.mid.model.flow
import it.eng.au.mid.model.flow.DailyConsumptionModel
import it.eng.au.mid.model.flow.calcolo.{AnnoMeseExecusionIdModel, PdrAnomaloModel}
import it.eng.au.mid.model.hive.mid.MidContatoriModel
import it.eng.au.mid.schema.flow.calcolo.{AnnoMeseExecutionIdSchema, PdrAnomaloSchema}
import it.eng.au.mid.schema.hive.mid.MidContatoriSchema
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Date

case class Df(annomese: String, executionId: Long)

case class DfOptional(annomese: String, executionId: Option[Long])

class CalcoloMidFunzioniTest extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

  def testMaxExecutionIdPerAnnomese(): Unit = {
    val df = Seq(
      Df(annomese = "202301", executionId = 1),
      Df(annomese = "202301", executionId = 2),
      Df(annomese = "202302", executionId = 9)
    ).toDF()

    val result = maxExecutionIdPerAnnomese(df, "202301")
    Assert.assertEquals(2, result.get)
  }

  def testMaxExecutionIdPerAnnomeseNoRes(): Unit = {
    val df = Seq(
      Df(annomese = "202302", executionId = 9)
    ).toDF()

    val result = maxExecutionIdPerAnnomese(df, "202301")
    Assert.assertEquals(false, result.isDefined)
  }


  def testRimuoviPdrDaEscludereDalCalcolo(): Unit = {
    val annomese = "202301"
    val anomali = Seq(
      PdrAnomaloModel(pdr = "1", annomese = annomese, executionid = 123),
      PdrAnomaloModel(pdr = "2", annomese = annomese, executionid = 123), //rimosso da inclusioni
      PdrAnomaloModel(pdr = "3", annomese = annomese, executionid = 123) //rimosso da esclusioni
    ).toDS()
    val inclusioni = Seq(
      InclusioniModel(pdr = "1", annomese = "202212", n = 1), //annomese diverso, non deve rimuovere
      InclusioniModel(pdr = "2", annomese = annomese, n = 1)
    ).toDS()
    val esclusioni = Seq(
      EsclusioniModel(pdr = "3", annomese = annomese)
    ).toDS()
    val result = CalcoloMidFunzioni.rimuoviPdrDaEscludereDalCalcolo(anomali, esclusioni, inclusioni).cache()
    Assert.assertEquals(1, result.count())
    Assert.assertEquals("1", result.head().pdr)
  }

  def testRaggruppaPdrAnomali(): Unit = {
    val annomese = "202301"
    val executionId = 132
    val anomali = Seq(
      PdrAnomaloModel(pdr = "1", annomese = annomese, executionid = executionId, tipoAnomalia = CostantiMid.CAUSALE_ESCLUSI),
      PdrAnomaloModel(pdr = "1", annomese = annomese, executionid = executionId, tipoAnomalia = CostantiMid.CAUSALE_INCOERENTI),
      PdrAnomaloModel(pdr = "2", annomese = annomese, executionid = executionId, tipoAnomalia = CostantiMid.CAUSALE_INCOERENTI),
      PdrAnomaloModel(pdr = "2", annomese = annomese, executionid = executionId, tipoAnomalia = CostantiMid.CAUSALE_INCOERENTI),
      PdrAnomaloModel(pdr = "3", annomese = annomese, executionid = executionId, tipoAnomalia = CostantiMid.CAUSALE_ESCLUSI)
    ).toDS()
    val expected = Seq(
      PdrAnomaloModel(pdr = "1", annomese = annomese, executionid = executionId, tipoAnomalia = CostantiMid.CAUSALE_ESCLUSI_INCOERENTI),
      PdrAnomaloModel(pdr = "2", annomese = annomese, executionid = executionId, tipoAnomalia = CostantiMid.CAUSALE_INCOERENTI),
      PdrAnomaloModel(pdr = "3", annomese = annomese, executionid = executionId, tipoAnomalia = CostantiMid.CAUSALE_ESCLUSI)
    ).toDS().cache()
    val result = CalcoloMidFunzioni.raggruppaPdrAnomali(anomali).cache()
    Assert.assertEquals(expected.count(), result.count())
    Assert.assertEquals(expected.where(col(PdrAnomaloSchema.pdr) === "1").head().tipoAnomalia, result.where(col(PdrAnomaloSchema.pdr) === "1").head().tipoAnomalia)
    Assert.assertEquals(expected.where(col(PdrAnomaloSchema.pdr) === "2").head().tipoAnomalia, result.where(col(PdrAnomaloSchema.pdr) === "2").head().tipoAnomalia)
    Assert.assertEquals(expected.where(col(PdrAnomaloSchema.pdr) === "3").head().tipoAnomalia, result.where(col(PdrAnomaloSchema.pdr) === "3").head().tipoAnomalia)
  }


  def testRimuoviInclusioniEdEsclusioni(): Unit = {
    val annomese = "202301"
    val mid = Seq(
      MidContatoriModel(pdr = "1", annomese = annomese),
      MidContatoriModel(pdr = "2", annomese = annomese),
      MidContatoriModel(pdr = "3", annomese = annomese)
    ).toDS()
    val inclusioni = Seq(
      InclusioniModel(pdr = "1", annomese = annomese, n = 1)
    ).toDS()
    val esclusioni = Seq(
      EsclusioniModel(pdr = "2", annomese = annomese)
    ).toDS()
    val result = CalcoloMidFunzioni.rimuoviInclusioniEdEsclusioni(mid, inclusioni, esclusioni).cache()
    Assert.assertEquals(1, result.count())
    Assert.assertEquals(1, result.where(col(MidContatoriSchema.pdr) === "3").count())
  }

  def testCalcoloMaxExexIdTracciaturaPerAnnomese(): Unit = {
    val annomese = "202301"
    val annomese2 = "202302"
    val annomese3 = "202303"
    val annomese4 = "202304"

    val executionid = 123L
    val executionid1 = 222L
    val executionid3 = 333L
    val executionid4 = 444L
    val executionid5 = 555L
    val executionid6 = null

    val mid = Seq(
      MidContatoriModel(pdr = "1", annomese = annomese,executionid_tracciatura = executionid),
      MidContatoriModel(pdr = "2", annomese = annomese,executionid_tracciatura = executionid6),
      MidContatoriModel(pdr = "3", annomese = annomese,executionid_tracciatura = executionid1),
      MidContatoriModel(pdr = "4", annomese = annomese,executionid_tracciatura = executionid1),
      MidContatoriModel(pdr = "1", annomese = annomese2,executionid_tracciatura = executionid3),
      MidContatoriModel(pdr = "1", annomese = annomese2,executionid_tracciatura = executionid3),
      MidContatoriModel(pdr = "1", annomese = annomese2,executionid_tracciatura = executionid3),
      MidContatoriModel(pdr = "2", annomese = annomese3,executionid_tracciatura = executionid4),
      MidContatoriModel(pdr = "3", annomese = annomese3,executionid_tracciatura = executionid4),
      MidContatoriModel(pdr = "3", annomese = annomese3,executionid_tracciatura = executionid5),
      MidContatoriModel(pdr = "4", annomese = annomese3,executionid_tracciatura = executionid5),
      MidContatoriModel(pdr = "5", annomese = annomese4,executionid_tracciatura = executionid6)
    ).toDS()

    val expecteValue_1 = Seq(
      AnnoMeseExecusionIdModel(annomese_riferimento = annomese,executionid_max = executionid1)).head
    val expecteValue_2 = Seq(
      AnnoMeseExecusionIdModel(annomese_riferimento = annomese2,executionid_max = executionid3)).head
    val expecteValue_3 = Seq(
      AnnoMeseExecusionIdModel(annomese_riferimento = annomese3,executionid_max = executionid5)).head
    val expecteValue_4 = Seq(
      AnnoMeseExecusionIdModel(annomese_riferimento = annomese4,executionid_max = executionid6)).head

    val result = CalcoloMidFunzioni.calcoloMaxExexIdTracciaturaPerAnnomese(mid)

    Assert.assertEquals("the number of different annomese is not equal." +
      "remember result need to contain only one row per annomese value",4L, result.count())
    Assert.assertEquals(expecteValue_1, result.where(col(AnnoMeseExecutionIdSchema.annomese_riferimento)==="202301").head())
    Assert.assertEquals(expecteValue_2, result.where(col(AnnoMeseExecutionIdSchema.annomese_riferimento)==="202302").head())
    Assert.assertEquals(expecteValue_3, result.where(col(AnnoMeseExecutionIdSchema.annomese_riferimento)==="202303").head())
    Assert.assertEquals(expecteValue_4, result.where(col(AnnoMeseExecutionIdSchema.annomese_riferimento)==="202304").head())

    result.show()
  }

  def testCalcolaContatoriMid(): Unit = {
    val annomese = "202301"
    val annomese2 = "202302"
    val annomese3 = "202303"

    val executionid = 123L
    val executionid_prev = 999L
    /*
    pdr 1 su anomali e mid
    pdr 2 solo su anomali
    pdr 3 solo su mid
     */

    val executionid_prevDs = Seq(
      AnnoMeseExecusionIdModel(annomese_riferimento = "202302",executionid_max = 200L),
      AnnoMeseExecusionIdModel(annomese_riferimento = "202303",executionid_max = null)
    ).toDS()

    val anomali = Seq(
      PdrAnomaloModel(pdr = "1", annomese = annomese, executionid = executionid, tipoAnomalia = CostantiMid.CAUSALE_INCOERENTI),
      PdrAnomaloModel(pdr = "2", annomese = annomese, executionid = executionid, tipoAnomalia = CostantiMid.CAUSALE_INCOERENTI)
    ).toDS()
    val mid = Seq(
      MidContatoriModel(pdr = "1", annomese = annomese, contatore = 1, executionid_tracciatura = executionid_prev),
      MidContatoriModel(pdr = "3", annomese = annomese, contatore = 2, executionid_tracciatura = executionid_prev)
    ).toDS()
    val expected_1 = MidContatoriModel(
      pdr = "1", contatore = 2, stato = CostantiMid.STATO_VALIDO, causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      annomese = annomese, executionid_tracciatura_prev = executionid_prev)
    val expected_2 = MidContatoriModel(
      pdr = "2", contatore = 1, stato = CostantiMid.STATO_VALIDO, causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      annomese = annomese, executionid_tracciatura_prev = executionid_prev)
    val expected_3 = MidContatoriModel(
      pdr = "3", contatore = 2, stato = CostantiMid.STATO_INVALIDO, annomese = annomese, executionid_tracciatura_prev = executionid_prev)

    val result = CalcoloMidFunzioni.calcolaContatoriMid(anomali, mid, executionid_prev).cache()
    Assert.assertEquals(expected_1, result.where(col(MidContatoriSchema.pdr) === "1").head())
    Assert.assertEquals(expected_2, result.where(col(MidContatoriSchema.pdr) === "2").head())
    Assert.assertEquals(expected_3, result.where(col(MidContatoriSchema.pdr) === "3").head())

  }

  def testCalcolaContatoriMidAgg(): Unit = {
    val annomese = "202301"
    val annomese2 = "202302"
    val annomese3 = "202303"


    val executionid = 123L
    val executionid_prev = 123L
    val executionid_prev2 = 333L


    val executionid_prevDs = Seq(
      AnnoMeseExecusionIdModel(annomese_riferimento = annomese2,executionid_max = executionid_prev2),
      AnnoMeseExecusionIdModel(annomese_riferimento = annomese3,executionid_max = null)
    ).toDS()


    val anomali = Seq(
      PdrAnomaloModel(pdr = "1", annomese = annomese, executionid = executionid, tipoAnomalia = CostantiMid.CAUSALE_INCOERENTI),
      PdrAnomaloModel(pdr = "2", annomese = annomese, executionid = executionid, tipoAnomalia = CostantiMid.CAUSALE_INCOERENTI),
      PdrAnomaloModel(pdr = "1", annomese = annomese2, executionid = executionid, tipoAnomalia = CostantiMid.CAUSALE_INCOERENTI),
      PdrAnomaloModel(pdr = "2", annomese = annomese2, executionid = executionid, tipoAnomalia = CostantiMid.CAUSALE_INCOERENTI),
      PdrAnomaloModel(pdr = "1", annomese = annomese3, executionid = executionid, tipoAnomalia = CostantiMid.CAUSALE_INCOERENTI),
      PdrAnomaloModel(pdr = "2", annomese = annomese3, executionid = executionid, tipoAnomalia = CostantiMid.CAUSALE_INCOERENTI)

    ).toDS()
    val mid = Seq(
      MidContatoriModel(pdr = "1", annomese = annomese, contatore = 1, executionid_tracciatura = executionid_prev),
      MidContatoriModel(pdr = "3", annomese = annomese, contatore = 2, executionid_tracciatura = executionid_prev),
      MidContatoriModel(pdr = "1", annomese = annomese2, contatore = 1, executionid_tracciatura = executionid_prev),
      MidContatoriModel(pdr = "3", annomese = annomese2, contatore = 2, executionid_tracciatura = executionid_prev),
      MidContatoriModel(pdr = "1", annomese = annomese3, contatore = 1, executionid_tracciatura = executionid_prev),
      MidContatoriModel(pdr = "3", annomese = annomese3, contatore = 2, executionid_tracciatura = executionid_prev)

    ).toDS()


    val expected_1 = MidContatoriModel(
      pdr = "1", contatore = 2, stato = CostantiMid.STATO_VALIDO, causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      annomese = annomese, executionid_tracciatura_prev = null)
    val expected_2 = MidContatoriModel(
      pdr = "2", contatore = 1, stato = CostantiMid.STATO_VALIDO, causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      annomese = annomese, executionid_tracciatura_prev = null)
    val expected_3 = MidContatoriModel(
      pdr = "3", contatore = 2, stato = CostantiMid.STATO_INVALIDO, annomese = annomese, executionid_tracciatura_prev = null)

    val expected2_1 = MidContatoriModel(
      pdr = "1", contatore = 2, stato = CostantiMid.STATO_VALIDO, causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      annomese = annomese2, executionid_tracciatura_prev = executionid_prev2)
    val expected2_2 = MidContatoriModel(
      pdr = "2", contatore = 1, stato = CostantiMid.STATO_VALIDO, causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      annomese = annomese2, executionid_tracciatura_prev = executionid_prev2)
    val expected2_3 = MidContatoriModel(
      pdr = "3", contatore = 2, stato = CostantiMid.STATO_INVALIDO, annomese = annomese2, executionid_tracciatura_prev = executionid_prev2)

    val expected3_1 = MidContatoriModel(
      pdr = "1", contatore = 2, stato = CostantiMid.STATO_VALIDO, causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      annomese = annomese3, executionid_tracciatura_prev = null)
    val expected3_2 = MidContatoriModel(
      pdr = "2", contatore = 1, stato = CostantiMid.STATO_VALIDO, causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      annomese = annomese3, executionid_tracciatura_prev = null)
    val expected3_3 = MidContatoriModel(
      pdr = "3", contatore = 2, stato = CostantiMid.STATO_INVALIDO, annomese = annomese3, executionid_tracciatura_prev = null)


    val result = CalcoloMidFunzioni.calcolaContatoriMid(anomali, mid, executionid_prevDs).cache()
//    result.where(col(MidContatoriSchema.annomese)==="202301").show()
//    result.where(col(MidContatoriSchema.annomese)==="202302").show()
//    result.where(col(MidContatoriSchema.annomese)==="202303").show()

    Assert.assertEquals(expected2_1, result.where(col(MidContatoriSchema.pdr) === "1" and col(MidContatoriSchema.annomese)===annomese2).head())
    Assert.assertEquals(expected2_2, result.where(col(MidContatoriSchema.pdr) === "2"and col(MidContatoriSchema.annomese)===annomese2).head())
    Assert.assertEquals(expected2_3, result.where(col(MidContatoriSchema.pdr) === "3"and col(MidContatoriSchema.annomese)===annomese2).head())

    Assert.assertEquals(expected2_1, result.where(col(MidContatoriSchema.pdr) === "1" and col(MidContatoriSchema.annomese)===annomese2).head())
    Assert.assertEquals(expected2_2, result.where(col(MidContatoriSchema.pdr) === "2"and col(MidContatoriSchema.annomese)===annomese2).head())
    Assert.assertEquals(expected2_3, result.where(col(MidContatoriSchema.pdr) === "3"and col(MidContatoriSchema.annomese)===annomese2).head())

    Assert.assertEquals(expected3_1, result.where(col(MidContatoriSchema.pdr) === "1" and col(MidContatoriSchema.annomese)===annomese3).head())
    Assert.assertEquals(expected3_2, result.where(col(MidContatoriSchema.pdr) === "2"and col(MidContatoriSchema.annomese)===annomese3).head())
    Assert.assertEquals(expected3_3, result.where(col(MidContatoriSchema.pdr) === "3"and col(MidContatoriSchema.annomese)===annomese3).head())

  }

  def testCalcolaContatoriMid_midVuoto(): Unit = {
    val annomese = "202301"
    val executionid = 123L
    val executionid_prev = 999L
    val anomali = Seq(
      PdrAnomaloModel(pdr = "1", annomese = annomese, executionid = executionid, tipoAnomalia = CostantiMid.CAUSALE_INCOERENTI),
      PdrAnomaloModel(pdr = "2", annomese = annomese, executionid = executionid, tipoAnomalia = CostantiMid.CAUSALE_INCOERENTI)
    ).toDS()
    val mid = List.empty[MidContatoriModel].toDS()
    val result = CalcoloMidFunzioni.calcolaContatoriMid(anomali, mid, executionid_prev).cache()
    Assert.assertEquals(2, result.count())
  }

  def testAggiungiInclusioni(): Unit = {
    val annomese = "2022301"
    val executionid_prev = 999L
    val mid = Seq(MidContatoriModel(pdr = "2")).toDS()
    val inclusioni = Seq(
      InclusioniModel(pdr = "1", annomese = annomese, n = 1)
    ).toDS()
    val result = CalcoloMidFunzioni.aggiungiInclusioni(mid, inclusioni, executionid_prev).cache()
    Assert.assertEquals(2, result.count())
  }

  def testAggiungiInclusioniAgg(): Unit = {
    val annomese = "2022301"
    val executionid_prev = 999L

    val executionid_prevDs = Seq(
      AnnoMeseExecusionIdModel(annomese_riferimento = annomese,executionid_max = executionid_prev)
    ).toDS()

    val mid = Seq(MidContatoriModel(pdr = "2")).toDS()
    val inclusioni = Seq(
      InclusioniModel(pdr = "1", annomese = annomese, n = 1)
    ).toDS()
    val result = CalcoloMidFunzioni.aggiungiInclusioni(mid, inclusioni, executionid_prevDs).cache()
    Assert.assertEquals(2, result.count())

    result.show()
  }

  def testFinalizzaMidContatori(): Unit = {
    val dataCalcolo = Date.valueOf("2023-01-01")
    val processoTracciatura = CostantiMid.PROCESSO_SBG
    val sessioneTracciatura = "SBG_FIN"
    val tipoCalcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO
    val executionIdDaily = 1L
    val executionId = 2L
    val treatment = "Y"

    val mid = Seq(
      MidContatoriModel(pdr = "1", contatore = 1, stato = CostantiMid.STATO_VALIDO, causale_tracciatura = CostantiMid.CAUSALE_ESCLUSI,
        executionid_tracciatura_prev = 123L, annomese = "202301")
    ).toDS()

    val dailyConsumption = Seq(
      DailyConsumptionModel(pdr = "1", annomese = "202301", treatment = treatment, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "1", annomese = "202302", treatment = "Q", executionid = executionIdDaily)
    ).toDS()
    val expected = MidContatoriModel(pdr = "1", contatore = 1, stato = CostantiMid.STATO_VALIDO, treatment = treatment,
      data_tracciatura = dataCalcolo, processo_tracciatura = processoTracciatura, sessione_tracciatura = sessioneTracciatura, causale_tracciatura = CostantiMid.CAUSALE_ESCLUSI, tipo_calcolo = tipoCalcolo,
      executionid_daily_consumption = executionIdDaily, executionid_tracciatura_prev = 123L, annomese = "202301", executionid_tracciatura = executionId)

    val result = CalcoloMidFunzioni.finalizzaMidContatori(
      mid = mid,
      dailyConsumption = dailyConsumption,
      dataCalcolo = dataCalcolo,
      processoTracciatura = processoTracciatura,
      sessioneTracciatura = sessioneTracciatura,
      tipoCalcolo = tipoCalcolo,
      executionIdDaily = executionIdDaily,
      executionId = executionId
    ).cache()

    Assert.assertEquals(expected, result.head())
  }


}
