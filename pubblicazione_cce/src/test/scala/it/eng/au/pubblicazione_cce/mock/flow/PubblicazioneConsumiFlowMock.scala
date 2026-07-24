package it.eng.au.pubblicazione_cce.mock.flow

import it.eng.au.pubblicazione_cce.dao.cce._
import it.eng.au.pubblicazione_cce.file.csv.DataFrameCsvBuilder
import it.eng.au.pubblicazione_cce.file.writer.{FileWriter, ZipWriter}
import it.eng.au.pubblicazione_cce.flow.consumi.PubblicazioneConsumiFlow
import it.eng.au.pubblicazione_cce.mock.writer.{FileWriterMock, ZipWriterMock}

import java.sql.Timestamp
import java.time.LocalDate

class PubblicazioneConsumiFlowMock(
                                    val dataRichieste: LocalDate,
                                    val processo: String,
                                    override val richiestePodDao: CceRichiestaPodDao,
                                    override val richiesteFiltroDao: CceRichiestaFiltroDao,
                                    override val anagraficaPodDao: CceCalcoloAnagraficaDao,
                                    override val misureDao: CceCalcoloDao,
                                    override val trattamentoDao: CceCalcoloTrattamentoDao,
                                    override val trackDao: CceCalcTrackDao,
                                    override val outputFileCsvWriter: FileWriter = new FileWriterMock,
                                    override val consumiCsvBuilder: DataFrameCsvBuilder,
                                    override val elencoFileCsvBuilder: DataFrameCsvBuilder,
                                    override val processTimestamp: Timestamp,
                                    override val fileTimestamp: String
                                  ) extends PubblicazioneConsumiFlow(dataRichieste = dataRichieste, processo = processo, misureDao = misureDao) {

  override val outputFileZipWriter: ZipWriter = new ZipWriterMock(processDate = dataRichieste, fileTimestamp = fileTimestamp)
}
