package it.eng.au.pubblicazione_cce.mock.flow

import it.eng.au.pubblicazione_cce.dao.cce.CceRichiestaPodDao
import it.eng.au.pubblicazione_cce.file.csv.DataFrameCsvBuilder
import it.eng.au.pubblicazione_cce.file.writer.FileWriter
import it.eng.au.pubblicazione_cce.flow.ammissibilita.PubblicazioneAmmissibilitaPodFlow

import java.time.LocalDate

class PubblicazioneAmmissibilitaPodFlowMock(
                                             override val dataRichieste: LocalDate,
                                             override val processo: String,
                                             override val richiestePodDao: CceRichiestaPodDao,
                                             override val outputFileDao: FileWriter = new FileWriter,
                                             override val outputFilePath: String,
                                             override val fileTimestamp: String,
                                             override val csvBuilder: DataFrameCsvBuilder
                                           )
  extends PubblicazioneAmmissibilitaPodFlow(dataRichieste, processo) {

}
