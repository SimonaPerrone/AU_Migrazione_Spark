package it.au.misure.ingestionMisureGasUnico.model

import java.io.File
import java.util.zip.ZipFile

import it.au.misure.ingestionMisureGasUnico.utility.PropertyUtility

import scala.collection.JavaConverters._
import scala.xml.{Node, XML}

case class GasUnzipMetadata(
                             file: File
                             , xmlNode: Node = null
                             , outputFilePath: String = ""
                             , statusCode: String = ""
                             , statusMessage: String = ""
                             , statusType: String = ""
                             , pivaDistributore: String
                             , pivaUtente: String
                             , anno: String
                             , annoRiferimento: String
                             , mese: String
                             , meseRiferimento: String
                             , giorno: String
                             , flusso: String
                             , timestamp: String
                             , progressivo: String
                             , tS: String
                             , ammissibile: Boolean = true
                             , alreadyTransmitted: Boolean = false
                             , externalInfo: ExternalInfo = ExternalInfo()
                             , trackType: String = ""
                             , fileError: String = ""
                           ) extends GasMetadata {
  override def loadXml: Node = {
    val zipFile = new ZipFile(file)
    XML.load(zipFile.getInputStream(zipFile.entries().nextElement()))
  }

  def numFiles: Int = {
    if (file.getName.takeRight(3).toLowerCase == "zip") {
      new ZipFile(file).entries().asScala.length
    } else {
      1
    }
  }

  override def originalFolder: String = {
    file.getParent.replaceAll("\\\\", "/").replace(PropertyUtility.getUnzipInputPath, "")
  }
}
