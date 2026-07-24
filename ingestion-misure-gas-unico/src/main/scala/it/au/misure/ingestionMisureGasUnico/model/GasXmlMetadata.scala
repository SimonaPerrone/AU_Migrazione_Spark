package it.au.misure.ingestionMisureGasUnico.model

import java.io.File
import java.time.LocalDateTime

import scala.xml.Node

case class GasXmlMetadata(
                           xmlNode: Node,
                           file: File,
                           pivaDistributore: String,
                           pivaUtente: String,
                           anno: String,
                           annoRiferimento: String,
                           mese: String,
                           meseRiferimento: String,
                           giorno: String,
                           flusso: String,
                           timestamp: String,
                           progressivo: String,
                           tS: String,
                           ammissibilita: Map[String, String] = null,
                           pdrRcuExist: Boolean = true,
                           pdrValidFrom: LocalDateTime = null,
                           pdrValidTo: LocalDateTime = null,
                           fileError: String = "",
                           igmgMatch: String = "",
                           igmrMatch: String = ""
                         ) extends GasMetadata {
  override def loadXml: Node = xmlNode

  override def getAmmissibilita(codPdr: String): String = {
    ammissibilita(codPdr)
  }
}
