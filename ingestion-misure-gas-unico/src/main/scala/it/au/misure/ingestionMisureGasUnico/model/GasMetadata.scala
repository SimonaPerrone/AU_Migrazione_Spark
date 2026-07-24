package it.au.misure.ingestionMisureGasUnico.model

import java.io.File

import scala.xml.Node

abstract class GasMetadata extends Serializable {
  def xmlNode: Node
  def file: File
  def pivaDistributore: String
  def pivaUtente: String
  def anno: String
  def annoRiferimento: String
  def mese: String
  def meseRiferimento: String
  def giorno: String
  def flusso: String
  def timestamp: String
  def progressivo: String
  def tS: String
  def fileError: String

  def loadXml: Node

  def getAmmissibilita(codPdr: String = ""): String = "OK"

  def normalizeFlusso: String = {
    if (flusso.length <= 4) {
      flusso
    } else if (flusso.contains(".")) {
      flusso.substring(0, flusso.indexOf("."))
    } else {
      flusso.substring(0, flusso.length - 4)
    }
  }

  def originalFolder: String = {
    s"TMG_$pivaDistributore/DISTRIBUTORE/TMG_${pivaDistributore}_$pivaUtente/$anno/$mese$giorno"
  }

  def originalRelativePath: String = {
    val ending = if (tS == "") "" else s"_$tS"
    s"$originalFolder/" +
      s"${pivaDistributore}_${pivaUtente}_$annoRiferimento${meseRiferimento}_${flusso}_${timestamp}_$progressivo$ending.zip"
  }
}
