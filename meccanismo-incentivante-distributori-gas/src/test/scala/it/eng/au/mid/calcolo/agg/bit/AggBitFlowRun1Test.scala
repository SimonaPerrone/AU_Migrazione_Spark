package it.eng.au.mid.calcolo.agg.bit

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.mock.agg.AggBitFlowMock
import it.eng.au.mid.mock.dao._
import it.eng.au.mid.model.file.calcolo.{EsclusioniModel, InclusioniModel}
import it.eng.au.mid.model.flow
import it.eng.au.mid.model.flow.calcolo.{DailyConsumptionEsclusiModel, DailyConsumptionIncoerentiModel}
import it.eng.au.mid.model.hive.mid.MidContatoriModel
import it.eng.au.mid.schema.hive.mid.MidContatoriSchema
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Date
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneId}


class AggBitFlowRun1Test extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

  /** *
   * Test run con tabella MID back in time con intervallo annomese Da - A definito e dataMid nel passato
   *
   * mid ha due calcolo di cui l'ultimo fuori limite imposta da dataMid
   * Atteso ricalcolo per 'exec1' su annomese 'annomese' escludendo calcoli con valori diversi
   */
  def testRun1(): Unit = {
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    val session = "AGG"
    val dailyExecutionId = 100L
    val executionId = 999L
    val trattamento = "Y"
    val dataCalcolo = LocalDate.parse("2023-02-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val dataTracciatura = Date.valueOf(dataCalcolo)
    val dataTracciaturaPassata = Date.valueOf(LocalDate.parse("2022-02-01", DateTimeFormatter.ofPattern("yyyy-MM-dd")))
    val annomese = "202301"
    // primo calcolo Mid ed atteso nei risultati
    val midExec1 = LocalDateTime.parse("2023-01-01 01:12:00", dateTimeFormatter).atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
    // calcolo successivo Mid da escludere
    val midExec2 = LocalDateTime.parse("2023-01-02 00:00:00", dateTimeFormatter).atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
    // filtro esclusione calcolo Mid
    val dataSogliaMid = LocalDate.parse("2023-01-01")

    // mock
    val midContatoriDao = new MidContatoriDaoMock(Seq(
      //exe1: ricalcolo da qui
      MidContatoriModel(pdr = "1", contatore = 1, stato = CostantiMid.STATO_VALIDO, treatment = trattamento,
        data_tracciatura = dataTracciaturaPassata, processo_tracciatura = session, sessione_tracciatura = session,
        causale_tracciatura = CostantiMid.CAUSALE_ESCLUSI_INCOERENTI, tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
        executionid_daily_consumption = 9L, executionid_tracciatura_prev = 8L, annomese = annomese, executionid_tracciatura = midExec1),
      MidContatoriModel(pdr = "2", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento,
        data_tracciatura = dataTracciaturaPassata, processo_tracciatura = session, sessione_tracciatura = session,
        causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI, tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
        executionid_daily_consumption = 9L, executionid_tracciatura_prev = 8L, annomese = annomese, executionid_tracciatura = midExec1),
      // fuori dal range annomeseDa-A
      MidContatoriModel(pdr = "3", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento,
        data_tracciatura = dataTracciaturaPassata, processo_tracciatura = session, sessione_tracciatura = session,
        causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI, tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
        executionid_daily_consumption = 9L, executionid_tracciatura_prev = 8L, annomese = "202302", executionid_tracciatura = midExec1),
      //exec2: calcolo da ignorare
      MidContatoriModel(pdr = "1", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento,
        data_tracciatura = dataTracciaturaPassata, processo_tracciatura = session, sessione_tracciatura = session,
        causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI, tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
        executionid_daily_consumption = 9L, executionid_tracciatura_prev = 8L, annomese = annomese, executionid_tracciatura = midExec2),
      MidContatoriModel(pdr = "3", contatore = 5, stato = CostantiMid.STATO_FORZATO, treatment = trattamento,
        data_tracciatura = dataTracciaturaPassata, processo_tracciatura = session, sessione_tracciatura = session,
        causale_tracciatura = null, tipo_calcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
        executionid_daily_consumption = 9L, executionid_tracciatura_prev = 8L, annomese = annomese, executionid_tracciatura = midExec2)
    ).toDS)

    val esclusiDao = new DailyConsumptionAggEsclusiDaoMock(Seq(
      DailyConsumptionEsclusiModel(pdr = "1", annomese = annomese, session = session, executionid = dailyExecutionId)
    ).toDS())

    val incoerentiDao = new DailyConsumptionAggIncoerentiDaoMock(Seq(
      DailyConsumptionIncoerentiModel(pdr = "3", annomese = annomese, session = session, ispdranomalousgdm = true, executionid = dailyExecutionId)
    ).toDS())

    val dailyConsumptionDao = new DailyConsumptionAggDaoMock(Seq(
      flow.DailyConsumptionModel(pdr = "1", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "2", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "3", annomese = annomese, treatment = trattamento, session = session, executionid = dailyExecutionId),
      flow.DailyConsumptionModel(pdr = "4", annomese = "202302", treatment = trattamento, session = session, executionid = dailyExecutionId) //filtrato da annomeseDa-A
    ).toDS())

    val fileEsclusioni = new EsclusioniAggDaoMock(List.empty[EsclusioniModel].toDS)

    val fileInclusioni = new InclusioniAggDaoMock(List.empty[InclusioniModel].toDS)

    // definizione job
    val aggStandardFlow = new AggBitFlowMock(
      annomeseDa = annomese,
      annomeseA = annomese,
      dataSogliaMid = dataSogliaMid,
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
      contatore = 2,
      stato = CostantiMid.STATO_VALIDO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = CostantiMid.CAUSALE_ESCLUSI,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_STRAORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = midExec1,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_2 = MidContatoriModel(
      pdr = "2",
      contatore = 2,
      stato = CostantiMid.STATO_INVALIDO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_STRAORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = midExec1,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val expected_3 = MidContatoriModel(
      pdr = "3",
      contatore = 1,
      stato = CostantiMid.STATO_VALIDO,
      treatment = trattamento,
      data_tracciatura = dataTracciatura,
      processo_tracciatura = session,
      sessione_tracciatura = session,
      causale_tracciatura = CostantiMid.CAUSALE_INCOERENTI,
      tipo_calcolo = CostantiMid.TIPO_CALCOLO_STRAORDINARIO,
      executionid_daily_consumption = dailyExecutionId,
      executionid_tracciatura_prev = midExec1,
      annomese = annomese,
      executionid_tracciatura = executionId
    )
    val result = midContatoriDao.ds.cache()

    Assert.assertEquals(3, result.count())
    Assert.assertEquals(expected_1, result.where(col(MidContatoriSchema.pdr) === "1").head())
    Assert.assertEquals(expected_2, result.where(col(MidContatoriSchema.pdr) === "2").head())
    Assert.assertEquals(expected_3, result.where(col(MidContatoriSchema.pdr) === "3").head())
  }

}
