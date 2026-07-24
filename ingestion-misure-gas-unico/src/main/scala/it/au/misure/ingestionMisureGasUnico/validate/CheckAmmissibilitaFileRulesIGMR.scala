package it.au.misure.ingestionMisureGasUnico.validate

import it.au.misure.ingestionMisureGasUnico.model.schema.IGMRXMLSchema
import it.au.misure.ingestionMisureGasUnico.model.schema.IGMRXMLSchema._
import it.au.misure.ingestionMisureGasUnico.model.{GasMetadata, GasUnzipMetadata}
import it.au.misure.ingestionMisureGasUnico.model.validate.{ReportEsitoFILEMessage, ReportMessage, Rule, RuleParameters}
import it.au.misure.ingestionMisureGasUnico.utility.Constants._
import it.au.misure.ingestionMisureGasUnico.utility.DateTimeUtility

import java.io.{ByteArrayInputStream, InputStream}
import javax.xml.transform.stream.StreamSource
import scala.util.Try
import scala.xml.NodeSeq

class CheckAmmissibilitaFileRulesIGMR extends CheckAmmissibilitaRules[ReportEsitoFILEMessage]{
  override def ammissibilitaType: String = "file"

  override def okMessage: ReportMessage = ReportEsitoFILEMessage()

  override def addFileInfoToMessage(message: ReportMessage, xml: NodeSeq, gasXmlMetadata: GasMetadata): ReportEsitoFILEMessage = {
    message.asInstanceOf[ReportEsitoFILEMessage].copy(cartellaCloud = gasXmlMetadata.file.getParent, nomeFile = gasXmlMetadata.file.getName)
  }

  override def rules: List[Rule] = List(
    ruleNamingUnmatch // 1
    , ruleNamingUnmatchAndStdFlowMatch // 2
    , ruleNamingXmlUnmatch // 3
    , ruleXmlNotPresent // 4
    , ruleZipXmlNamingUnmatch // 5
    , ruleZipError // 6
    , ruleXmlError // 7
    , ruleFlussoCodFlusso // 8
    , rulePivaDistributore // 9
    , rulePivaUDDCloud // 10
    , rulePivaUdDFile // 11
    , rulePIVAUDDRCU // 12
    , ruleAnnoMeseRiferimentoNomeFile // 13
    , ruleTimestamp // 14
    , ruleFileAlreadyTransmittedIGMR // 15
    , ruleXMLValidate // 16
    , ruleGenericError // 17
  )

  val ruleNamingUnmatch: Rule = Rule( //ID: 1
    ruleName = "ruleNamingUnmatch",
    condition = (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      fileXmlWIthMeta.fileError.equals(IGMR_NAMING_UNMATCH)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- Formato .zip non rispettato",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  val ruleNamingUnmatchAndStdFlowMatch: Rule = Rule( //ID: 2
    ruleName = "ruleNamingUnmatchAndStdFlowMatch",
    condition = (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      fileXmlWIthMeta.fileError.equals(IGMR_NAMING_UNMATCH_BUT_STD_MATCH)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- Nomenclatura file .zip non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  val ruleNamingXmlUnmatch: Rule = Rule( //ID: 3
    ruleName = "ruleNamingUnmatchAndStdFlowMatch",
    condition = (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      fileXmlWIthMeta.fileError.equals(FILE_NOT_XML)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- file .xml non presente",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  val ruleXmlNotPresent: Rule = Rule( //ID: 4
    ruleName = "ruleXmlNotPresent",
    condition = (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      fileXmlWIthMeta.fileError.equals(FILE_XML_NOT_PRESENT)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- Non è presente un unico file xml dentro allo zip",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  val ruleZipXmlNamingUnmatch: Rule = Rule( //ID: 5
    ruleName = "ruleZipXmlNamingUnmatch",
    condition = (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      fileXmlWIthMeta.fileError.equals(ZIP_XML_NAMING_UNMATCH)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- Nomenclatura file xml non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  val ruleZipError: Rule = Rule( //ID: 6
    ruleName = "ruleZipError",
    condition = (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      fileXmlWIthMeta.fileError.equals(CORRUPTED_ZIP)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- File zip corrotto",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  val ruleXmlError: Rule = Rule(//ID: 7
    ruleName = "ruleXmlError",
    condition = (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      fileXmlWIthMeta.fileError.equals(CORRUPTED_XML)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- File xml corrotto",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  val ruleFlussoCodFlusso: Rule = Rule( //ID: 8
    condition = (fileNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {
      !fileXmlWithMeta.flusso.equalsIgnoreCase((fileNode \ cod_flusso).text)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + " - Codice Flusso errato",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    ),
    ruleName = "ruleFlussoCodFlusso"
  )

  val rulePivaDistributore: Rule = Rule( //ID: 9
    condition = (fileNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      !fileXmlWithMeta.pivaDistributore.equalsIgnoreCase((fileNode \ IdentificativiFlusso \ piva_distr).text)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + " - PIVA Distributore errata",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    ),
    ruleName = "rulePivaDistributore"
  )

  val rulePivaUDDCloud: Rule = Rule( //ID: 10
    condition = (fileNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {
      val pivaUddFromFolder: String = fileXmlWithMeta.file.getParentFile.getParentFile.getParentFile.getName.split("_")(2)
      !fileXmlWithMeta.pivaUtente.equalsIgnoreCase(pivaUddFromFolder)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + " - PIVA UdD non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    ),
    ruleName = "rulePivaUDDCloud"
  )

  val rulePivaUdDFile: Rule = Rule( //ID: 11
    condition = (fileNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {
      !fileXmlWithMeta.pivaUtente.equalsIgnoreCase((fileNode \ IdentificativiFlusso \ piva_utente).text)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + " - PIVA UdD non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    ),
    ruleName = "rulePivaUdDFile"
  )

  val rulePIVAUDDRCU: Rule = Rule( //ID: 12
    condition = (fileNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {
      //get param
      val xGiorni = params.get.parameters("XGiorni").toInt
      //get file YearMonth
      val annoMeseFile = DateTimeUtility.getYearMonthOrNull(s"${fileXmlWithMeta.meseRiferimento}/${fileXmlWithMeta.annoRiferimento}", MONTH_YEAR_PATTERN)
      //get piva UdD
      val pIvaUdDFile = fileXmlWithMeta.pivaUtente
      //get piva UdD -> valid period Map
      val uDDActivePeriodsMap = fileXmlWithMeta.asInstanceOf[GasUnzipMetadata].externalInfo.uDDActivePeriodsMap.value
      //get valid period for this piva UdD
      val pIvaUdDActivePeriod = uDDActivePeriodsMap.get(pIvaUdDFile)

      if (pIvaUdDActivePeriod.isDefined) { //apply the rule if a period exist for this pIva

        val startDate = pIvaUdDActivePeriod.get._1.toLocalDate
        val endDate = pIvaUdDActivePeriod.get._2.toLocalDate

        val submissionStartDate = annoMeseFile.atDay(1)
        val submissionEndDate = submissionStartDate.plusMonths(1).plusDays(xGiorni - 1)

        submissionEndDate.isBefore(startDate) || submissionStartDate.isAfter(endDate)

      } else false
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + " - PIVA UdD non coerente",
      bloccante = NON_BLOCCANTE,
      ammissibilita = FILE
    ),
    ruleName = "rulePIVAUDDRCUIGMR"
  )

  val ruleAnnoMeseRiferimentoNomeFile: Rule = Rule( //ID: 13
    ruleName = "ruleAnnoMeseRiferimentoNomeFile",
    condition = (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val annoMeseFile = DateTimeUtility.getYearMonthOrNull(s"${fileXmlWIthMeta.meseRiferimento}/${fileXmlWIthMeta.annoRiferimento}", MONTH_YEAR_PATTERN)
      if (annoMeseFile != null && (annoMeseFile.getYear.toString.startsWith("19") || annoMeseFile.getYear.toString.startsWith("20"))) false else true
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- AnnoMese di riferimento nel nome del file non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  val ruleTimestamp: Rule = Rule( //ID: 14
    condition = (fileNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {
      DateTimeUtility.getDateTimeOrNull(fileXmlWithMeta.timestamp, FILE_TIMESTAMP_PATTERN) == null
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + " - Timestamp formalmente errato",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    ),
    ruleName = "ruleTimestamp"
  )

  val ruleFileAlreadyTransmittedIGMR: Rule = Rule( //ID: 15
    condition = (fileNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {
      fileXmlWithMeta.asInstanceOf[GasUnzipMetadata].alreadyTransmitted
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD919,
      descrizione = ERROR_FILE_ALREADY_TRANSMITTED,
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    ),
    ruleName = "ruleFileAlreadyTransmittedIGMR"
  )

  val ruleXMLValidate: Rule = Rule( //ID: 16
    condition = (fileNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {
      val metadata: GasUnzipMetadata = fileXmlWithMeta.asInstanceOf[GasUnzipMetadata]

      val v = Try(metadata.externalInfo.flussoIGMRXSD.validate(new StreamSource(nodeSeqToInputBuffer(fileNode))))
      if (v.isFailure) {
        logger.warn(s"Flusso IGMR XSD val error: ${v.failed.get.getMessage}")
        true
      } else false
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + " - struttura xsd non conforme",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    ),
    ruleName = "ruleXMLValidate"
  )

  val ruleGenericError: Rule = Rule( //ID: 17
    ruleName = "ruleGenericError",
    condition = (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      fileXmlWIthMeta.fileError.equals(GENERIC_ERROR)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- Errore generico",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )


  def nodeSeqToInputBuffer(nodeSeq: NodeSeq): InputStream = new ByteArrayInputStream(nodeSeq.toString.getBytes)

  printRules()
}
