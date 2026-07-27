package it.eng.au.mid.calcolo.agg.standard

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.mock.agg.AggStandardFlowMock
import it.eng.au.mid.mock.dao._
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


class AggStandardFlowRun1Test extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

  /** *
   * Test run con tabella MID vuota e senza file di inclusioni ed esclusioni
   *
   * pdr 1 e 2 sono anomali (pdr 1 sia in esclusi che in incoerenti)
   * pdr 3 e 4 sono da filtrare (vedi commento su definizione pdr)
   */
  def testRun1(): Unit = {
    val session = "AGG"
    val dailyExecutionId = 111L
    val executionId = 999L
    val trattamento = "Y"
    val dataCalcolo = LocalDate.parse("2023-02-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val dataTracciatura = Date.valueOf(dataCalcolo)
    val annomese = "202301"

    // mock
    val midContatoriDao = new MidContatoriDaoMock(List.empty[MidContatoriModel].toDS)

    val esclusiDao = new DailyConsumptionAggEsclusiDaoMock(Seq(
      DailyConsumptionEsclusiModel(pdr = "1", annomese = annomese, session = session, executionid = dailyExecutionId),
      DailyConsumptionEsclusiModel(pdr = "3", annomese = annomese, session = "XXX", executionid = 222L) // sessione non corretta
    ).toDS())

    val incoerentiDao = new DailyConsumptionAggIncoerentiDaoMock(Seq(
      DailyConsumptionIncoerentiModel(pdr = "1", annomese = annomese, session = session, ispdranomalousgdm = true, executionid = dailyExecutionId),
      DailyConsumptionIncoerentiModel(pdr = "2", annomese = annomese, session = session, ispdranomalousgdm = true, executionid = dailyExecutionId),
      DailyConsumptionIncoerentiModel(pdr = "4", annomese = annomese, session = session, ispdranomalousgdm = false, executionid = dailyExecutionId) // ispdranomalousgdm false: non anomalo
    ).toDS())

    val dailyConsumptionDao = new DailyConsumptionAggDaoMock(Seq(
      flow.DailyConsumptionModel(pdr = "1", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "2", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "1", annomese = annomese, treatment = "XX", session = session, executionid = 1L) // execution id da escludere
    ).toDS())

    val fileEsclusioni = new EsclusioniAggDaoMock(List.empty[EsclusioniModel].toDS)

    val fileInclusioni = new InclusioniAggDaoMock(List.empty[InclusioniModel].toDS)

    // definizione job
    val aggStandardFlow = new AggStandardFlowMock(
      incoerentiDao = incoerentiDao,
      esclusiDao = esclusiDao,
      fileEsclusioni = fileEsclusioni,
      fileInclusioni = fileInclusioni,
      dailyConsumptionDao = dailyConsumptionDao,
      midContatoriDao = midContatoriDao,
      aggExecutionId = dailyExecutionId,
      executionId = executionId,
      dataCalcolo = dataCalcolo,
      fileEsclusioniAbilitato = false,
      fileInclusioniAbilitato = false
    )

    aggStandardFlow.run()

    val expected_1 = MidContatoriModel(
      pdr = "1",
      contatore = 1,
      stato = CostantiMid.STATO_VALIDO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = CostantiMid.CAUSALE_ESCLUSI_INCOERENTI,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = null,
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
      executionid_tracciatura_prev = null,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val result = midContatoriDao.ds.cache()

    Assert.assertEquals(2, result.count())
    Assert.assertEquals(expected_1, result.where(col(MidContatoriSchema.pdr) === "1").head())
    Assert.assertEquals(expected_2, result.where(col(MidContatoriSchema.pdr) === "2").head())
  }

}
