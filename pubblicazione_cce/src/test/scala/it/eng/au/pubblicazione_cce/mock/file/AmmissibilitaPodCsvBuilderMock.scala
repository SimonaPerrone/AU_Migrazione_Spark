package it.eng.au.pubblicazione_cce.mock.file

import it.eng.au.pubblicazione_cce.file.csv.AmmissibilitaPodCsvBuilder

import java.time.LocalDate

class AmmissibilitaPodCsvBuilderMock(
                                      override val fileTimestamp: String,
                                      override val dataCalcolo: LocalDate,
                                      override val outputFilePath: String
                                    ) extends AmmissibilitaPodCsvBuilder