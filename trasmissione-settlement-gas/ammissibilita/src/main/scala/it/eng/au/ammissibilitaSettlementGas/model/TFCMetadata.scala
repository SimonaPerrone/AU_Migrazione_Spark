package it.eng.au.ammissibilitaSettlementGas.model

import java.io.File

case class TFCMetadata(
                        file: File,
                        tipoFile: String,
                        lastModified: Long,
                        yearDir: String,
                        monthDir: String,
                        pivaRdb: Option[String],
                        annoMese: Option[String],
                        progressivo: Option[String],
                        csv: Option[List[TFC]],
                        isAlreadyTransmitted: Boolean = false,
                        isAmmissibile: Boolean,
                        statusCode: String = "",
                        statusMessage: String = ""
                      )
