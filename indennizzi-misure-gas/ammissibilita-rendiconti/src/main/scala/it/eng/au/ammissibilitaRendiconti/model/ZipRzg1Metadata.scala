package it.eng.au.ammissibilitaRendiconti.model

import java.io.File

case class ZipRzg1Metadata(
                            file: File,
                            lastModified: Long,
                            csv: Option[CsvRzg1Metadata] = None,
                            pivaUtente: String,
                            pivaId: String,
                            pivaUdd: String,
                            yearDir: String,
                            monthDir: String,
                            annoMeseCompetenza: String,
                            timestamp: String,
                            progressivo: String,
                            isAmmissibile: Boolean,
                            statusCode: String = "",
                            statusMessage: String = ""
                        ) extends Serializable
