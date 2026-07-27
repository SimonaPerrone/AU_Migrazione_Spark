package it.eng.au.mid.mock.pubblicazione

import it.eng.au.mid.dao.file.zip.ZipWriterDao
import it.eng.au.mid.dao.hive.mid.{Mid2DettaglioDao, MidAggregatoreInfoDao}
import it.eng.au.mid.flow.pubblicazione.PubblicazioneMid2Flow

import java.time.LocalDate

class PubblicazioneMid2FlowMock(
                                 override val mid2DettaglioDao: Mid2DettaglioDao = null,
                                 override val zipWriterDao: ZipWriterDao = null,
                                 override val midAggregatoreInfoDao: MidAggregatoreInfoDao = null,
                                 override val executionIdMid2Dettaglio: Long = 0L,
                                 override val sessioneForzata: String,
                                 override val percorsoSalvataggio: String,
                                 override val maxRighePerCsv: Int = 20,
                                 override val cseaPartitaIva: String = null,
                                 override val executionId: Long = 0L,
                                 override val dataCalcolo: LocalDate = LocalDate.now(),
                                 override val fileTimestamp: String = "20230101000000"
                               )
  extends PubblicazioneMid2Flow

