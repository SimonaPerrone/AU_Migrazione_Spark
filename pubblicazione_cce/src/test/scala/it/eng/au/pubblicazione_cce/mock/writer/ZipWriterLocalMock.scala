package it.eng.au.pubblicazione_cce.mock.writer

import it.eng.au.pubblicazione_cce.file.writer.ZipWriter

import java.time.LocalDate

class ZipWriterLocalMock(
                          override val processDate: LocalDate = LocalDate.parse("2024-01-01"),
                          override val fileTimestamp: String = "20240101000000",
                          override val MAX_BYTES_SIZE_ZIP: Long,
                          override val outputFilePath: String,
                          override val numberOfSubDirectories: Int
                        ) extends ZipWriter
