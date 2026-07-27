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


class SbgStandardFlowRun3Test extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

  /** *
   * Test run con tabella MID con file esclusioni ed inclusioni valorizzati
   *
   * pdr 1 (presente in MID precedente e risulta anche anomalo) è escluso e quindi deve risultare invalido
   * pdr 2 è anomalo ma escluso e quindi non deve essere presente in MID
   * pdr 3 è anomalo ma deve essere forzato al valore presente nel file inclusioni
   * pdr 4 non è anomalo ma deve essere forzato
   */
  def testRun3(): Unit = {
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
      MidContatoriModel(pdr = "1", contatore = 1, stato = CostantiMid.STATO_VALIDO, treatment = "Y",
        data_tracciatura = dataTracciaturaPassata, processo_tracciatura = session, sessione_tracciatura = session,
        causale_tracciatura = CostantiMid.CAUSALE_ESCLUSI, tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
        executionid_daily_consumption = 9L, executionid_tracciatura_prev = 8L, annomese = annomese, executionid_tracciatura = executionIdPrev)
    ).toDS)

    val esclusiDao = new DailyConsumptionSbgEsclusiDaoMock(List.empty[DailyConsumptionEsclusiModel].toDS())

    val incoerentiDao = new DailyConsumptionSbgIncoerentiDaoMock(Seq(
      DailyConsumptionIncoerentiModel(pdr = "1", annomese = annomese, session = session, ispdranomalousgdm = true, executionid = dailyExecutionId),
      DailyConsumptionIncoerentiModel(pdr = "2", annomese = annomese, session = session, ispdranomalousgdm = true, executionid = dailyExecutionId),
      DailyConsumptionIncoerentiModel(pdr = "3", annomese = annomese, session = session, ispdranomalousgdm = true, executionid = dailyExecutionId)
    ).toDS())

    val dailyConsumptionDao = new DailyConsumptionSbgDaoMock(Seq(
      flow.DailyConsumptionModel(pdr = "1", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "2", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "3", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "4", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId)
    ).toDS())

    val fileEsclusioni = new EsclusioniSbgDaoMock(Seq(
      EsclusioniModel(pdr = "1", annomese = annomese),
      EsclusioniModel(pdr = "2", annomese = annomese)
    ).toDS())

    val fileInclusioni = new InclusioniSbgDaoMock(Seq(
      InclusioniModel(pdr = "3", annomese = annomese, n = 9),
      InclusioniModel(pdr = "4", annomese = annomese, n = 5)
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
      pdr = "1",
      contatore = 0,
      stato = CostantiMid.STATO_ESCLUSO_FORZATO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = null,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = executionIdPrev,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_2 = MidContatoriModel(
      pdr = "2",
      contatore = 0,
      stato = CostantiMid.STATO_ESCLUSO_FORZATO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = null,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = executionIdPrev,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_3 = MidContatoriModel(
      pdr = "3",
      contatore = 9,
      stato = CostantiMid.STATO_FORZATO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = null,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = executionIdPrev,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_4 = MidContatoriModel(
      pdr = "4",
      contatore = 5,
      stato = CostantiMid.STATO_FORZATO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = null,
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
    Assert.assertEquals(expected_3, result.where(col(MidContatoriSchema.pdr) === "3").head())
    Assert.assertEquals(expected_4, result.where(col(MidContatoriSchema.pdr) === "4").head())
  }

}
