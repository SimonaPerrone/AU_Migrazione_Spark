package it.au.misure.ee_switching.utility

import it.au.misure.ee_switching.model.mapping.DatiPodFieldsMapping
import it.au.misure.ee_switching.model.schema.hive.{FunzionaliCompressedSchema, PodMetadata, ReportEntry, StoriciCompressedSchema}
import it.au.misure.ee_switching.model.schema.xml.{CommonInfoXMLSchema, TagXml}
import it.au.misure.ee_switching.utility.Constants.XML_HEADER_CONSTANT
import it.au.misure.ee_switching.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}

object FlowUtility {

  // from Row to (nomeXmlIntero, (ID Pod, DatiPodXmlTag))
  def buildFunzionaliDatiPodXmlNode(row: Row): (String, String) = {
    val podMeta = PodMetadata(nomeFlusso = row.getAs[String](FunzionaliCompressedSchema.nome_flusso))
    var xmlNode: String = ""

    for ((n_row,tagXml) <- DatiPodFieldsMapping.mappingDatiPodFunzionali)
      xmlNode += buildTag(row, tagXml, podMeta)

    (row.getAs[String](FunzionaliCompressedSchema.pod14), xmlNode)
  }

  // from Row to (nomeXmlIntero, (ID Pod, DatiPodXmlTag))
  def buildStoriciDatiPodXmlNode(row: Row): (String, String) = {
    val podMeta = PodMetadata(nomeFlusso = row.getAs[String](StoriciCompressedSchema.nome_flusso), trattamento = row.getAs[String](StoriciCompressedSchema.trattamento),
      tipoMisuratore = row.getAs[String](StoriciCompressedSchema.tipo_misuratore), messaRegime = row.getAs[String](StoriciCompressedSchema.messa_regime),
      podRiconfigurato = (row.getAs[String](StoriciCompressedSchema.t_tipo_configurazione) == "SI"))
    var xmlNode: String = ""

    for ((n_row,tagXml) <- DatiPodFieldsMapping.mappingDatiPodStorici)
      xmlNode += buildTag(row, tagXml, podMeta)

    (row.getAs[String](StoriciCompressedSchema.pod14), xmlNode)
  }

  // costruzione singolo tag xml
  def buildTag(row: Row, tagXml: TagXml, podMeta: PodMetadata): String = {
    var tag: String = ""
    if (tagXml.presenceCondition(podMeta)) { // verifico presenza del tag secondo la funzione definita per questo tag
      if (tagXml.onlyOpening)
        tag += s"<${tagXml.tagName}>"
      else if (tagXml.onlyClosure)
        tag += s"</${tagXml.tagName}>"
      else {
        val tagValue = if (row.getAs(tagXml.infoFrom) != null) row.getAs(tagXml.infoFrom).toString else null
        if (tagValue != null && !tagValue.toLowerCase.equals("null") && tagValue.nonEmpty) {
          if (tagXml.readyTag) // tag intero già preparato in caso di tag "Ea" per flussi storici
            tag += tagValue
          else if (tagXml.stringTransformation != null)
            tag += s"<${tagXml.tagName}>${tagXml.stringTransformation(tagValue)}</${tagXml.tagName}>"
          else if (tagXml.toItalianDate) {
            if (tagXml.getItalianDateFormat(tagValue).nonEmpty)
              tag += s"<${tagXml.tagName}>${tagXml.getItalianDateFormat(tagValue)}</${tagXml.tagName}>"
          } else if (tagXml.doubleToInt) {
            if (tagXml.getIntFromDouble(tagValue).nonEmpty)
              tag += s"<${tagXml.tagName}>${tagXml.getIntFromDouble(tagValue)}</${tagXml.tagName}>"
          } else if (tagXml.formatDouble) {
            if (tagXml.getFormattedDouble(tagValue).nonEmpty || tagXml.emptyTagIfNegativeValue)  // Tags EaFi flussi storici introdotti vuoti in caso di valore negativo
              tag += s"<${tagXml.tagName}>${tagXml.getFormattedDouble(tagValue)}</${tagXml.tagName}>"
          } else
           tag += s"<${tagXml.tagName}>${tagValue}</${tagXml.tagName}>"
        } else if (tagValue == null && tagXml.nvlDefaultValue != null) { // simulazione funzione nvl (sql)
          tag += s"<${tagXml.tagName}>${tagXml.nvlDefaultValue}</${tagXml.tagName}>"
        }
      }
    }
    tag
  }

  def addCommonInfo(chunkData: String, PIvaDistributore: String, PIvaUtente: String, codFlusso: String, CodContrDisp: String): String = {
    val header =
      s"""
         |<${CommonInfoXMLSchema.FlussoMisure} ${XML_HEADER_CONSTANT} ${CommonInfoXMLSchema.CodFlusso}="${codFlusso}">
         |<${CommonInfoXMLSchema.IdentificativiFlusso}>
         |<${CommonInfoXMLSchema.PIvaUtente}>${PIvaUtente}</${CommonInfoXMLSchema.PIvaUtente}>
         |<${CommonInfoXMLSchema.PIvaDistributore}>${PIvaDistributore}</${CommonInfoXMLSchema.PIvaDistributore}>
         |<${CommonInfoXMLSchema.CodContrDisp}>${CodContrDisp}</${CommonInfoXMLSchema.CodContrDisp}>
         |</${CommonInfoXMLSchema.IdentificativiFlusso}>
         |""".stripMargin

    val footer = s"</${CommonInfoXMLSchema.FlussoMisure}>"

    header + chunkData + footer
  }

  def writeReport(reportRdd: RDD[ReportEntry]): Unit = {
    val sqlContext = Environment.getSpark.sqlContext
    sqlContext.createDataFrame(reportRdd)
      .write
      .mode(SaveMode.Append)
      //.partitionBy(Constants.REPORT_TABLE_PARTITIONING_COLUMN) --> Commented in order to fix the error "Insertinto() can't be used together with partitionBy.
      .insertInto(Constants.REPORT_TABLE)
  }

}
