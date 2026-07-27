package it.eng.au.ammissibilitaSettlementGas.model

import java.io.File

case class QKRIUDMetadata(
                           file: File,
                           tipoFile: String,
                           lastModified: Long,
                           yearDir: String,
                           monthDir: String,
                           pivaRdb: Option[String],
                           annoTermico: Option[String],
                           progressivo: Option[String],
                           csv: Option[List[QKRIUD]],
                           isAlreadyTransmitted: Boolean = false,
                           isAmmissibile: Boolean,
                           statusCode: String = "",
                           statusMessage: String = ""
                         )
