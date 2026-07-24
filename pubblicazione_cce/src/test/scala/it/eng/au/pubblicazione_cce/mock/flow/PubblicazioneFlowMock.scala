package it.eng.au.pubblicazione_cce.mock.flow

import it.eng.au.pubblicazione_cce.dao.cce._
import it.eng.au.pubblicazione_cce.flow.PubblicazioneFlow
import it.eng.au.pubblicazione_cce.mock.file.{AmmissibilitaFileCsvBuilderMock, AmmissibilitaPodCsvBuilderMock, ConsumiCsvBuilderMock, ElencoFlussiCsvBuilderMock}
import it.eng.au.pubblicazione_cce.mock.writer.FileWriterMock

import java.sql.Timestamp
import java.time.LocalDate

class PubblicazioneFlowMock(
                             override val dataRichieste: LocalDate,
                             override val processo: String,
                             override val richiestaFiltroDao: CceRichiestaFiltroDao,
                             override val esitoDao: CceEsitoDao,
                             override val dataCalcolo: LocalDate,
                             override val processTimestamp: Timestamp,
                             override val executionId: String,
                             override val outputFilePath: String,
                             override val misureDao: CceCalcoloDao,
                             val richiestaPodDao: CceRichiestaPodDao,
                             val anagraficaPodDao: CceCalcoloAnagraficaDao,
                             val trattamentoDao: CceCalcoloTrattamentoDao,
                             val trackDao: CceCalcTrackDao,
                             val fileTimestamp: String,
                             val maxLineCsv: Int
                           )
  extends PubblicazioneFlow(dataRichieste = dataRichieste, processo = processo, misureDao = misureDao) {

  override val pubblicazioneAmmissibilitaFileFlow =
    new PubblicazioneAmmissibilitaFileFlowMock(
      dataRichieste = dataRichieste,
      processo = processo,
      richiestePodDao = richiestaPodDao,
      outputFileDao = new FileWriterMock(),
      outputFilePath = outputFilePath,
      fileTimestamp = fileTimestamp,
      csvBuilder = new AmmissibilitaFileCsvBuilderMock(fileTimestamp = fileTimestamp, dataCalcolo = dataCalcolo, outputFilePath = outputFilePath)
    )
  override val pubblicazioneAmmissibilitaPodFlow = new PubblicazioneAmmissibilitaPodFlowMock(
    dataRichieste = dataRichieste,
    processo = processo,
    richiestePodDao = richiestaPodDao,
    outputFileDao = new FileWriterMock(),
    outputFilePath = outputFilePath,
    fileTimestamp = fileTimestamp,
    csvBuilder = new AmmissibilitaPodCsvBuilderMock(fileTimestamp = fileTimestamp, dataCalcolo = dataCalcolo, outputFilePath = outputFilePath)
  )

  override val pubblicazioneConsumiFlow = new PubblicazioneConsumiFlowMock(
    dataRichieste = dataRichieste,
    processo = processo,
    richiestePodDao = richiestaPodDao,
    richiesteFiltroDao = richiestaFiltroDao,
    anagraficaPodDao = anagraficaPodDao,
    misureDao = misureDao,
    trattamentoDao = trattamentoDao,
    trackDao = trackDao,
    consumiCsvBuilder = new ConsumiCsvBuilderMock(fileTimestamp, dataCalcolo, outputFilePath, maxLineCsv),
    elencoFileCsvBuilder = new ElencoFlussiCsvBuilderMock(fileTimestamp, dataCalcolo, outputFilePath, maxLineCsv),
    processTimestamp = processTimestamp,
    fileTimestamp = fileTimestamp
  )
}