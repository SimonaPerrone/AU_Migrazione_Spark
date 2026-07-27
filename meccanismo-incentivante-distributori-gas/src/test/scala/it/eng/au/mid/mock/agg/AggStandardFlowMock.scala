package it.eng.au.mid.mock.agg

import it.eng.au.mid.flow.calcolo.AggStandardFlow
import it.eng.au.mid.mock.dao._

import java.time.LocalDate

class AggStandardFlowMock(
                           override val incoerentiDao: DailyConsumptionAggIncoerentiDaoMock,
                           override val esclusiDao: DailyConsumptionAggEsclusiDaoMock,
                           override val fileEsclusioni: EsclusioniAggDaoMock,
                           override val fileInclusioni: InclusioniAggDaoMock,
                           override val dailyConsumptionDao: DailyConsumptionAggDaoMock,
                           override val midContatoriDao: MidContatoriDaoMock,
                           override val aggExecutionId: Long,
                           override val executionId: Long,
                           override val dataCalcolo: LocalDate,
                           override val fileEsclusioniAbilitato: Boolean,
                           override val fileInclusioniAbilitato: Boolean
                         ) extends AggStandardFlow
