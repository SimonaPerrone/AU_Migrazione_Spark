package it.eng.au.mid.collaudo

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


class AggStandardFlowCollaudoTest extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

  /** *
   * Collaudo flusso AGG Standard
   */
  def testCollaudo(): Unit = {
    val processoTracciatura = "AGG"
    val session = "AGG_S1_FIN"
    val dailyExecutionId = 1704193200000L
    val executionId = 1704279600000L
    val executionIdPrecedente = 1704106800000L
    val trattamento = "Y"
    val dataCalcolo = LocalDate.parse("2024-01-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val dataTracciatura = Date.valueOf(dataCalcolo)
    val annomese = "202401"
    val classeMisuratore = "G4"

    // mock
    val midContatoriDao = new MidContatoriDaoMock(Seq(
      MidContatoriModel(pdr = "agg5", contatore = 1, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, data_tracciatura = dataTracciatura, processo_tracciatura = processoTracciatura, sessione_tracciatura = session, causale_tracciatura = CostantiMid.CAUSALE_ESCLUSI, tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO, executionid_daily_consumption = executionIdPrecedente, executionid_tracciatura_prev = null, annomese = annomese, executionid_tracciatura = executionIdPrecedente),
      MidContatoriModel(pdr = "agg6", contatore = 1, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, data_tracciatura = dataTracciatura, processo_tracciatura = processoTracciatura, sessione_tracciatura = session, causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI, tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO, executionid_daily_consumption = executionIdPrecedente, executionid_tracciatura_prev = null, annomese = annomese, executionid_tracciatura = executionIdPrecedente),
      MidContatoriModel(pdr = "agg7", contatore = 1, stato = CostantiMid.STATO_INVALIDO, treatment = trattamento, data_tracciatura = dataTracciatura, processo_tracciatura = processoTracciatura, sessione_tracciatura = session, causale_tracciatura = null, tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO, executionid_daily_consumption = executionIdPrecedente, executionid_tracciatura_prev = null, annomese = annomese, executionid_tracciatura = executionIdPrecedente)
    ).toDS)

    val esclusiDao = new DailyConsumptionAggEsclusiDaoMock(Seq(
      DailyConsumptionEsclusiModel(pdr = "agg1", annomese = annomese, session = session, executionid = dailyExecutionId),
      DailyConsumptionEsclusiModel(pdr = "agg3", annomese = annomese, session = session, executionid = dailyExecutionId),
      DailyConsumptionEsclusiModel(pdr = "agg5", annomese = annomese, session = session, executionid = dailyExecutionId)
    ).toDS())

    val incoerentiDao = new DailyConsumptionAggIncoerentiDaoMock(Seq(
      DailyConsumptionIncoerentiModel(pdr = "agg2", annomese = annomese, session = session, ispdranomalousgdm = true, executionid = dailyExecutionId)
    ).toDS())

    val dailyConsumptionDao = new DailyConsumptionAggDaoMock(Seq(
      flow.DailyConsumptionModel(pdr = "agg1", annomese = annomese, treatment = trattamento, session = session, classemisuratore = classeMisuratore, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "agg2", annomese = annomese, treatment = trattamento, session = session, classemisuratore = classeMisuratore, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "agg3", annomese = annomese, treatment = trattamento, session = session, classemisuratore = classeMisuratore, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "agg4", annomese = annomese, treatment = trattamento, session = session, classemisuratore = classeMisuratore, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "agg5", annomese = annomese, treatment = trattamento, session = session, classemisuratore = classeMisuratore, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "agg6", annomese = annomese, treatment = trattamento, session = session, classemisuratore = classeMisuratore, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "agg7", annomese = annomese, treatment = trattamento, session = session, classemisuratore = classeMisuratore, executionid = dailyExecutionId)
    ).toDS())

    val fileEsclusioni = new EsclusioniAggDaoMock(Seq(
      EsclusioniModel(pdr = "agg3", annomese = annomese)
    ).toDS)

    val fileInclusioni = new InclusioniAggDaoMock(Seq(
      InclusioniModel(pdr = "agg4", annomese = annomese, n = 2)
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

    val expected_1 = MidContatoriModel(
      pdr = "agg1",
      contatore = 1,
      stato = CostantiMid.STATO_VALIDO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = processoTracciatura,
      sessione_tracciatura = session,
      causale_tracciatura = CostantiMid.CAUSALE_ESCLUSI,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = executionIdPrecedente,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_2 = MidContatoriModel(
      pdr = "agg2",
      contatore = 1,
      stato = CostantiMid.STATO_VALIDO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = processoTracciatura,
      sessione_tracciatura = session,
      causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = executionIdPrecedente,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_3 = MidContatoriModel(
      pdr = "agg3",
      contatore = 0,
      stato = CostantiMid.STATO_ESCLUSO_FORZATO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = processoTracciatura,
      sessione_tracciatura = session,
      causale_tracciatura = null,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = executionIdPrecedente,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_4 = MidContatoriModel(
      pdr = "agg4",
      contatore = 2,
      stato = CostantiMid.STATO_FORZATO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = processoTracciatura,
      sessione_tracciatura = session,
      causale_tracciatura = null,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = executionIdPrecedente,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_5 = MidContatoriModel(
      pdr = "agg5",
      contatore = 2,
      stato = CostantiMid.STATO_VALIDO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = processoTracciatura,
      sessione_tracciatura = session,
      causale_tracciatura = CostantiMid.CAUSALE_ESCLUSI,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = executionIdPrecedente,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_6 = MidContatoriModel(
      pdr = "agg6",
      contatore = 1,
      stato = CostantiMid.STATO_INVALIDO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = processoTracciatura,
      sessione_tracciatura = session,
      causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = executionIdPrecedente,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val result = midContatoriDao.ds.cache()
    result.show()

    Assert.assertEquals(6, result.count())
    Assert.assertEquals(expected_1, result.where(col(MidContatoriSchema.pdr) === "agg1").head())
    Assert.assertEquals(expected_2, result.where(col(MidContatoriSchema.pdr) === "agg2").head())
    Assert.assertEquals(expected_3, result.where(col(MidContatoriSchema.pdr) === "agg3").head())
    Assert.assertEquals(expected_4, result.where(col(MidContatoriSchema.pdr) === "agg4").head())
    Assert.assertEquals(expected_5, result.where(col(MidContatoriSchema.pdr) === "agg5").head())
    Assert.assertEquals(expected_6, result.where(col(MidContatoriSchema.pdr) === "agg6").head())
  }

}
