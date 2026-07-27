package it.eng.au.mid.mock.pubblicazione

import it.eng.au.mid.dao.hive.atg.AtgVariazioniSocDao
import it.eng.au.mid.dao.hive.rcu.RcuAziendaPDao
import it.eng.au.mid.dao.hive.rcugas.RcugasConnessioniDistr2RemiPDao
import it.eng.au.mid.flow.pubblicazione.PredisposizioneMid2Flow
import it.eng.au.mid.mock.dao._

class PredisposizioneMid2FlowMock(
                                   override val midContatoriDao: MidContatoriDaoMock,
                                   override val dailyConsumptionAggDao: DailyConsumptionAggDaoMock,
                                   override val mid2DettaglioDao: Mid2DettaglioDaoMock,
                                   override val rcuAziendaDao: RcuAziendaPDao,
                                   override val rcugasConnessioniDao: RcugasConnessioniDistr2RemiPDao,
                                   override val atgVariazioniSocDao: AtgVariazioniSocDao,
                                   override val fileEsclusioniPdrDao: Mid2EsclusioniPdrDaoMock,
                                   override val fileEsclusioniTrattamentoDao: Mid2EsclusioniTrattamentoDaoMock,
                                   override val fileEsclusioniAnnomeseDao: Mid2EsclusioniAnnomeseDaoMock,
                                   override val fileEsclusioniDistributoreDao: Mid2EsclusioniDistributoreDaoMock,
                                   override val fileAlphaValori: MidAlphaValoriDaoMock,
                                   override val midAnnomeseDa: String,
                                   override val midAnnomeseA: String,
                                   override val sogliaContatore: Int
                                 )
  extends PredisposizioneMid2Flow
