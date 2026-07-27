package it.eng.au.mid.collaudo

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.mock.dao._
import it.eng.au.mid.mock.sbg.SbgStandardFlowMock
import it.eng.au.mid.model.file.calcolo.{EsclusioniModel, InclusioniModel}
import it.eng.au.mid.model.flow
import it.eng.au.mid.model.flow.calcolo.{DailyConsumptionEsclusiModel, DailyConsumptionIncoerentiModel}
import it.eng.au.mid.model.hive.mid.MidContatoriModel
import it.eng.au.mid.schema.hive.mid.MidContatoriSchema
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter


class SbgStandardFlowCollaudoTest extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

  /** *
   * Collaudo flusso SBG Standard
   */
  def testCollaudoSBG(): Unit = {
    val session = "SBG"
    val dailyExecutionId = 1704106800000L
    val executionId = 1704106800000L
    val trattamento = "Y"
    val dataCalcolo = LocalDate.parse("2024-01-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val dataTracciatura = Date.valueOf(dataCalcolo)
    val meseOffset = 0
    val annomese = "202401"

    // mock
    val midContatoriDao = new MidContatoriDaoMock(List.empty[MidContatoriModel].toDS)

    val esclusiDao = new DailyConsumptionSbgEsclusiDaoMock(Seq(
      DailyConsumptionEsclusiModel(pdr = "sbg1", annomese = annomese, session = session, executionid = dailyExecutionId),
      DailyConsumptionEsclusiModel(pdr = "sbg3", annomese = annomese, session = session, executionid = dailyExecutionId)
    ).toDS())

    val incoerentiDao = new DailyConsumptionSbgIncoerentiDaoMock(Seq(
      DailyConsumptionIncoerentiModel(pdr = "sbg2", annomese = annomese, session = session, ispdranomalousgdm = true, executionid = dailyExecutionId)
    ).toDS())

    val dailyConsumptionDao = new DailyConsumptionSbgDaoMock(Seq(
      flow.DailyConsumptionModel(pdr = "sbg1", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "sbg2", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "sbg3", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "sbg4", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId)
    ).toDS())

    val fileEsclusioni = new EsclusioniSbgDaoMock(Seq(
      EsclusioniModel(pdr = "sbg3", annomese = annomese)
    ).toDS)

    val fileInclusioni = new InclusioniSbgDaoMock(Seq(
      InclusioniModel(pdr = "sbg4", annomese = annomese, n = 2)
    ).toDS)

    // definizione job
    val sbgStandardFlow = new SbgStandardFlowMock(
      incoerentiDao = incoerentiDao,
      esclusiDao = esclusiDao,
      fileEsclusioni = fileEsclusioni,
      fileInclusioni = fileInclusioni,
      dailyConsumptionDao = dailyConsumptionDao,
      midContatoriDao = midContatoriDao,
      meseOffset = meseOffset,
      executionId = executionId,
      dataCalcolo = dataCalcolo,
      fileEsclusioniAbilitato = true,
      fileInclusioniAbilitato = true
    )

    sbgStandardFlow.run()

    val expected_1 = MidContatoriModel(
      pdr = "sbg1",
      contatore = 1,
      stato = CostantiMid.STATO_VALIDO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = CostantiMid.CAUSALE_ESCLUSI,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = null,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_2 = MidContatoriModel(
      pdr = "sbg2",
      contatore = 1,
      stato = CostantiMid.STATO_VALIDO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = null,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_3 = MidContatoriModel(
      pdr = "sbg3",
      contatore = 0,
      stato = CostantiMid.STATO_ESCLUSO_FORZATO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = null,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = null,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_4 = MidContatoriModel(
      pdr = "sbg4",
      contatore = 2,
      stato = CostantiMid.STATO_FORZATO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = null,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = null,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val result = midContatoriDao.ds.cache()
    result.show()

    Assert.assertEquals(4, result.count())
    Assert.assertEquals(expected_1, result.where(col(MidContatoriSchema.pdr) === "sbg1").head())
    Assert.assertEquals(expected_2, result.where(col(MidContatoriSchema.pdr) === "sbg2").head())
    Assert.assertEquals(expected_3, result.where(col(MidContatoriSchema.pdr) === "sbg3").head())
    Assert.assertEquals(expected_4, result.where(col(MidContatoriSchema.pdr) === "sbg4").head())
  }

}
