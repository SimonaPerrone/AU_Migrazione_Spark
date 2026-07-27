package it.eng.au.mid.mock.pubblicazione

import it.eng.au.mid.flow.pubblicazione.PredisposizioneMid1Flow
import it.eng.au.mid.mock.dao._

class PredisposizioneMid1FlowMock(
                                   override val midContatoriDao: MidContatoriDaoMock,
                                   override val dailyConsumptionAggDao: DailyConsumptionAggDaoMock,
                                   override val mid1DettaglioDao: Mid1DettaglioDaoMock,
                                   override val fileEsclusioniPdrDao: Mid1EsclusioniPdrDaoMock,
                                   override val fileEsclusioniTrattamentoDao: Mid1EsclusioniTrattamentoDaoMock,
                                   override val fileEsclusioniAnnomeseDao: Mid1EsclusioniAnnomeseDaoMock,
                                   override val fileEsclusioniDistributoreDao: Mid1EsclusioniDistributoreDaoMock,
                                   override val fileAlphaValori: MidAlphaValoriDaoMock,
                                   override val midAnnomeseDa: String,
                                   override val midAnnomeseA: String,
                                   override val sogliaContatore: Int
                                 )
  extends PredisposizioneMid1Flow
