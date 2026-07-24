package it.eng.au.pubblicazione_cce.mock.file

import it.eng.au.pubblicazione_cce.file.csv.ConsumiCsvBuilder

import java.time.LocalDate

class ConsumiCsvBuilderMock(
                             override val fileTimestamp: String,
                             override val dataCalcolo: LocalDate,
                             override val outputFilePath: String,
                             override val maxLineCsv: Int = 0
                           ) extends ConsumiCsvBuilder