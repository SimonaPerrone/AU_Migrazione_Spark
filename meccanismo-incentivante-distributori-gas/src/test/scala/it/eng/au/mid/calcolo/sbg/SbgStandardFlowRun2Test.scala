package it.eng.au.mid.calcolo.sbg

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


class SbgStandardFlowRun2Test extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

  /** *
   * Test run con tabella MID con aggiornamento PDR validi e invalidi. File esclusioni ed inclusioni vuoti
   *
   * pdr 1: deve essere incrementato in quanto valido in MID precedente
   * pdr 2: deve essere inserito a 1 in quanto invalido in MID precedente
   * pdr 3: deve essere escluso in quanto non anomalo e invalido in MID precedente
   * pdr 4: deve essere incluso come nuovo anomalo (non presente in MID precedente)
   * pdr 5: deve essere incluso come invalido in quanto presente nel calcolo precedente ma non piu' anomalo
   */
  def testRun2(): Unit = {
    val session = "SBG"
    val dailyExecutionId = 111L
    val executionId = 999L
    val executionIdPrev = 888L
    val trattamento = "Y"
    val dataCalcolo = LocalDate.parse("2023-02-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val dataTracciatura = Date.valueOf(dataCalcolo)
    val dataTracciaturaPassata = Date.valueOf("2022-12-29")
    val meseOffset = 1
    val annomese = "202301"

    // mock
    val midContatoriDao = new MidContatoriDaoMock(Seq(
      MidContatoriModel(pdr = "1", contatore = 1, stato = CostantiMid.STATO_VALIDO, treatment = trattamento,
        data_tracciatura = dataTracciaturaPassata, processo_tracciatura = session, sessione_tracciatura = session,
        causale_tracciatura = CostantiMid.CAUSALE_ESCLUSI_INCOERENTI, tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
        executionid_daily_consumption = 9L, executionid_tracciatura_prev = 8L, annomese = annomese, executionid_tracciatura = executionIdPrev),
      MidContatoriModel(pdr = "2", contatore = 2, stato = CostantiMid.STATO_INVALIDO, treatment = trattamento,
        data_tracciatura = dataTracciaturaPassata, processo_tracciatura = session, sessione_tracciatura = session,
        causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI, tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
        executionid_daily_consumption = 9L, executionid_tracciatura_prev = 8L, annomese = annomese, executionid_tracciatura = executionIdPrev),
      MidContatoriModel(pdr = "3", contatore = 5, stato = CostantiMid.STATO_INVALIDO, treatment = trattamento,
        data_tracciatura = dataTracciaturaPassata, processo_tracciatura = session, sessione_tracciatura = session,
        causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI, tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
        executionid_daily_consumption = 9L, executionid_tracciatura_prev = 8L, annomese = annomese, executionid_tracciatura = executionIdPrev),
      MidContatoriModel(pdr = "5", contatore = 3, stato = CostantiMid.STATO_VALIDO, treatment = trattamento,
        data_tracciatura = dataTracciaturaPassata, processo_tracciatura = session, sessione_tracciatura = session,
        causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI, tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
        executionid_daily_consumption = 9L, executionid_tracciatura_prev = 8L, annomese = annomese, executionid_tracciatura = executionIdPrev)
    ).toDS)

    val esclusiDao = new DailyConsumptionSbgEsclusiDaoMock(Seq(
      DailyConsumptionEsclusiModel(pdr = "1", annomese = annomese, session = session, executionid = dailyExecutionId)
    ).toDS())

    val incoerentiDao = new DailyConsumptionSbgIncoerentiDaoMock(Seq(
      DailyConsumptionIncoerentiModel(pdr = "2", annomese = annomese, session = session, ispdranomalousgdm = true, executionid = dailyExecutionId),
      DailyConsumptionIncoerentiModel(pdr = "4", annomese = annomese, session = session, ispdranomalousgdm = true, executionid = dailyExecutionId)
    ).toDS())

    val dailyConsumptionDao = new DailyConsumptionSbgDaoMock(Seq(
      flow.DailyConsumptionModel(pdr = "1", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "2", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "3", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "4", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId)
    ).toDS())

    val fileEsclusioni = new EsclusioniSbgDaoMock(List.empty[EsclusioniModel].toDS)

    val fileInclusioni = new InclusioniSbgDaoMock(List.empty[InclusioniModel].toDS)

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
      fileEsclusioniAbilitato = false,
      fileInclusioniAbilitato = false
    )

    sbgStandardFlow.run()

    val expected_1 = MidContatoriModel(
      pdr = "1",
      contatore = 2,
      stato = CostantiMid.STATO_VALIDO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = CostantiMid.CAUSALE_ESCLUSI,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = executionIdPrev,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_2 = MidContatoriModel(
      pdr = "2",
      contatore = 1,
      stato = CostantiMid.STATO_VALIDO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = executionIdPrev,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_4 = MidContatoriModel(
      pdr = "4",
      contatore = 1,
      stato = CostantiMid.STATO_VALIDO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = executionIdPrev,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_5 = MidContatoriModel(
      pdr = "5",
      contatore = 3,
      stato = CostantiMid.STATO_INVALIDO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = executionIdPrev,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val result = midContatoriDao.ds.cache()

    Assert.assertEquals(4, result.count())
    Assert.assertEquals(expected_1, result.where(col(MidContatoriSchema.pdr) === "1").head())
    Assert.assertEquals(expected_2, result.where(col(MidContatoriSchema.pdr) === "2").head())
    Assert.assertEquals(expected_4, result.where(col(MidContatoriSchema.pdr) === "4").head())
    Assert.assertEquals(expected_5, result.where(col(MidContatoriSchema.pdr) === "5").head())
  }

}
