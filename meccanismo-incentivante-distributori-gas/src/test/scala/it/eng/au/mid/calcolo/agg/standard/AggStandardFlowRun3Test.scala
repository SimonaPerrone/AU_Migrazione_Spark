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


class AggStandardFlowRun3Test extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

  /** *
   * Test run con tabella MID con file esclusioni ed inclusioni valorizzati
   * pdr 1 viene escluso
   * pdr 2 viene escluso solo un annomese mentre un successivo e' atteso
   * pdr 3 e 4 sono inclusi su annomesi differenti
   */
  def testRun3(): Unit = {
    val session = "AGG_S2_PRE"
    val dailyExecutionId = 111L
    val executionId = 999L
    val trattamento = "Y"
    val dataCalcolo = LocalDate.parse("2023-02-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val dataTracciatura = Date.valueOf(dataCalcolo)
    val annomese = "202301"
    val annomese2 = "202302"

    // mock
    val midContatoriDao = new MidContatoriDaoMock(List.empty[MidContatoriModel].toDS)

    val esclusiDao = new DailyConsumptionAggEsclusiDaoMock(Seq(
      DailyConsumptionEsclusiModel(pdr = "1", annomese = annomese, session = session, executionid = dailyExecutionId)
    ).toDS())

    val incoerentiDao = new DailyConsumptionAggIncoerentiDaoMock(Seq(
      DailyConsumptionIncoerentiModel(pdr = "1", annomese = annomese, session = session, ispdranomalousgdm = true, executionid = dailyExecutionId),
      DailyConsumptionIncoerentiModel(pdr = "2", annomese = annomese, session = session, ispdranomalousgdm = true, executionid = dailyExecutionId),
      DailyConsumptionIncoerentiModel(pdr = "2", annomese = annomese2, session = session, ispdranomalousgdm = true, executionid = dailyExecutionId)
    ).toDS())

    val dailyConsumptionDao = new DailyConsumptionAggDaoMock(Seq(
      flow.DailyConsumptionModel(pdr = "1", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "2", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "2", annomese = annomese2, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "3", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "4", annomese = annomese2, treatment = trattamento, session = session, executionid = dailyExecutionId)
    ).toDS())

    val fileEsclusioni = new EsclusioniAggDaoMock(Seq(
      EsclusioniModel(pdr = "1", annomese = annomese),
      EsclusioniModel(pdr = "2", annomese = annomese)
    ).toDS())

    val fileInclusioni = new InclusioniAggDaoMock(Seq(
      InclusioniModel(pdr = "3", annomese = annomese, n = 9),
      InclusioniModel(pdr = "4", annomese = annomese2, n = 5)
    ).toDS)

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
      fileEsclusioniAbilitato = true,
      fileInclusioniAbilitato = true
    )

    aggStandardFlow.run()

    val expected_1_EF = MidContatoriModel(
      pdr = "1",
      contatore = 0,
      stato = CostantiMid.STATO_ESCLUSO_FORZATO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = "AGG",
      sessione_tracciatura = session,
      causale_tracciatura = null,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = null,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_2_EF = MidContatoriModel(
      pdr = "2",
      contatore = 0,
      stato = CostantiMid.STATO_ESCLUSO_FORZATO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = "AGG",
      sessione_tracciatura = session,
      causale_tracciatura = null,
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
      processo_tracciatura = "AGG",
      sessione_tracciatura = session,
      causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = null,
      annomese = annomese2,
      executionid_tracciatura = executionId
    )
    val expected_3 = MidContatoriModel(
      pdr = "3",
      contatore = 9,
      stato = CostantiMid.STATO_FORZATO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = "AGG",
      sessione_tracciatura = session,
      causale_tracciatura = null,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = null,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_4 = MidContatoriModel(
      pdr = "4",
      contatore = 5,
      stato = CostantiMid.STATO_FORZATO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = "AGG",
      sessione_tracciatura = session,
      causale_tracciatura = null,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = null,
      annomese = annomese2,
      executionid_tracciatura = executionId
    )


    val result = midContatoriDao.ds.cache()
    result.show

    Assert.assertEquals(5, result.count())
    Assert.assertEquals(expected_1_EF, result
      .where(col(MidContatoriSchema.pdr) === "1" and col(MidContatoriSchema.annomese) === annomese).head())
    Assert.assertEquals(expected_2_EF, result
      .where(col(MidContatoriSchema.pdr) === "2" and col(MidContatoriSchema.annomese) === annomese).head())
    Assert.assertEquals(expected_2, result
      .where(col(MidContatoriSchema.pdr) === "2" and col(MidContatoriSchema.annomese) === annomese2).head())
    Assert.assertEquals(expected_3, result
      .where(col(MidContatoriSchema.pdr) === "3" and col(MidContatoriSchema.annomese) === annomese).head())
    Assert.assertEquals(expected_4, result
      .where(col(MidContatoriSchema.pdr) === "4" and col(MidContatoriSchema.annomese) === annomese2).head())
  }

}
