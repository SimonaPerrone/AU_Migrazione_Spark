package it.eng.au.pubblicazione_cce.mock.file

import it.eng.au.pubblicazione_cce.file.csv.ElencoFlussiCsvBuilder

import java.time.LocalDate

class ElencoFlussiCsvBuilderMock(
                                  override val fileTimestamp: String,
                                  override val dataCalcolo: LocalDate,
                                  override val outputFilePath: String,
                                  override val maxLineCsv: Int
                                ) extends ElencoFlussiCsvBuilder
