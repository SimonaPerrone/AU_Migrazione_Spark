package it.eng.au.mid.mock.sbg

import it.eng.au.mid.flow.calcolo.SbgStandardFlow
import it.eng.au.mid.mock.dao._

import java.time.LocalDate

class SbgStandardFlowMock(
                           override val incoerentiDao: DailyConsumptionSbgIncoerentiDaoMock,
                           override val esclusiDao: DailyConsumptionSbgEsclusiDaoMock,
                           override val fileEsclusioni: EsclusioniSbgDaoMock,
                           override val fileInclusioni: InclusioniSbgDaoMock,
                           override val dailyConsumptionDao: DailyConsumptionSbgDaoMock,
                           override val midContatoriDao: MidContatoriDaoMock,
                           override val meseOffset: Int,
                           override val executionId: Long,
                           override val dataCalcolo: LocalDate,
                           override val fileEsclusioniAbilitato: Boolean,
                           override val fileInclusioniAbilitato: Boolean
                         ) extends SbgStandardFlow
