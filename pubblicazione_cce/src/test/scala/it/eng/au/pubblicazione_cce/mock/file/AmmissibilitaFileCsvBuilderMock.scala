package it.eng.au.pubblicazione_cce.mock.file

import it.eng.au.pubblicazione_cce.file.csv.AmmissibilitaFileCsvBuilder

import java.time.LocalDate

class AmmissibilitaFileCsvBuilderMock(
                                       override val fileTimestamp: String,
                                       override val dataCalcolo: LocalDate,
                                       override val outputFilePath: String
                                     ) extends AmmissibilitaFileCsvBuilder