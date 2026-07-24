package it.au.misure.ingestionMisureGasUnico.validate

import java.io.{ByteArrayInputStream, InputStream}

import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.MisuraXMLSchema._
import it.au.misure.ingestionMisureGasUnico.model.validate.{ReportEsitoFILEMessage, ReportMessage, Rule, RuleParameters}
import it.au.misure.ingestionMisureGasUnico.model.{GasMetadata, GasUnzipMetadata}
import it.au.misure.ingestionMisureGasUnico.utility.Constants._
import it.au.misure.ingestionMisureGasUnico.utility.DateTimeUtility
import javax.xml.transform.stream.StreamSource

import scala.util.Try
import scala.xml.NodeSeq

class CheckAmmissibilitaFileRules extends CheckAmmissibilitaRules[ReportEsitoFILEMessage] {
  override def ammissibilitaType = "file"

  override def okMessage: ReportMessage = ReportEsitoFILEMessage()

  override def addFileInfoToMessage(message: ReportMessage, xml: NodeSeq, fileXmlWIthMeta: GasMetadata): ReportEsitoFILEMessage = {
    message.asInstanceOf[ReportEsitoFILEMessage].copy(cartellaCloud = fileXmlWIthMeta.file.getParent, nomeFile = fileXmlWIthMeta.file.getName)
  }

  override def rules: List[Rule] = List(
    ruleNamingUnmatch, // 43
    ruleAnnoMeseRiferimentoNomeFile, // 42
    ruleXmlNotPresent, // 44
    ruleZipXmlNamingUnmatch, // 45
    ruleZipError, // 46
    ruleXmlError, // 47
    ruleGenericError, // 48
    ruleFlusso1ValidateXML, // 9
    ruleFlusso2ValidateXML, // 10
    rulePIVADistributore, // 1
    rulePIVAUDD, // 2
    rulePIVAUDDPIVAUtente, // 3
    rulePIVAUDDRCU, // 4
    ruleTimestamp, // 5
    ruleCodFlusso, // 6
    ruleTraccaitoStandard, // 7
    ruleAlreadyTransmitted // 8
  )

  val ruleNamingUnmatch:Rule = Rule(
    ruleName = "ruleNamingUnmatch",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      fileXmlWIthMeta.fileError.equals(STD_NAMING_UNMATCH)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- Nomenclatura tracciato non rispettata" ,
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  val ruleAnnoMeseRiferimentoNomeFile:Rule = Rule(
    ruleName = "ruleAnnoMeseRiferimentoNomeFile",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val annoMeseFile = DateTimeUtility.getYearMonthOrNull(s"${fileXmlWIthMeta.meseRiferimento}/${fileXmlWIthMeta.annoRiferimento}",MONTH_YEAR_PATTERN)
      if (annoMeseFile != null && (annoMeseFile.getYear.toString.startsWith("19") || annoMeseFile.getYear.toString.startsWith("20"))) false else true
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE+"- AnnoMese di riferimento nel nome del file non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= FILE
    )
  )

  val ruleXmlNotPresent:Rule = Rule(
    ruleName = "ruleXmlNotPresent",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      fileXmlWIthMeta.fileError.equals(FILE_XML_NOT_PRESENT)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- Non è presente un unico file xml dentro allo zip" ,
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  val ruleZipXmlNamingUnmatch:Rule = Rule(
    ruleName = "ruleZipXmlNamingUnmatch",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      fileXmlWIthMeta.fileError.equals(ZIP_XML_NAMING_UNMATCH)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- Nomenclatura file xml non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  val ruleZipError:Rule = Rule(
    ruleName = "ruleZipError",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      fileXmlWIthMeta.fileError.equals(CORRUPTED_ZIP)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- File zip corrotto" ,
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  val ruleXmlError:Rule = Rule(
    ruleName = "ruleXmlError",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      fileXmlWIthMeta.fileError.equals(CORRUPTED_XML)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- File xml corrotto" ,
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  val ruleGenericError:Rule = Rule(
    ruleName = "ruleGenericError",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      fileXmlWIthMeta.fileError.equals(GENERIC_ERROR)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- Errore generico" ,
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  val rulePIVADistributore:Rule = Rule(
    ruleName = "rulePIVADistributore",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val pivaDistrFile = (xmlFile \ IdentificativiFlusso \ piva_distr).text
      !fileXmlWIthMeta.pivaDistributore.equalsIgnoreCase(pivaDistrFile)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE+"- PIVA Distributore errata" ,
      bloccante = BLOCCANTE,
      ammissibilita= FILE
    )
  )

  val rulePIVAUDD:Rule = Rule(
    ruleName = "rulePIVAUDD",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val pivaUddFromFolder :String = fileXmlWIthMeta.file.getParentFile.getParentFile.getParentFile.getName.split("_")(2)
      !fileXmlWIthMeta.pivaUtente.equalsIgnoreCase(pivaUddFromFolder)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- PIVA UdD non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= FILE
    )
  )
  val rulePIVAUDDPIVAUtente:Rule = Rule(
    ruleName = "rulePIVAUDDPIVAUtente",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      !fileXmlWIthMeta.pivaUtente.equalsIgnoreCase((xmlFile \ IdentificativiFlusso \ piva_utente).text)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- PIVA UdD non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= FILE
    )
  )
  val rulePIVAUDDRCU:Rule = Rule(
    ruleName = "rulePIVAUDDRCU",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, ruleParams: Option[RuleParameters]) => {
      //get param
      val xGiorni = ruleParams.get.parameters("XGiorni").toInt
      //get file YearMonth
      val annoMeseFile = DateTimeUtility.getYearMonthOrNull(s"${fileXmlWIthMeta.meseRiferimento}/${fileXmlWIthMeta.annoRiferimento}",MONTH_YEAR_PATTERN)
      //get piva UdD
      val pIvaUdDFile = fileXmlWIthMeta.pivaUtente
      //get piva UdD -> valid period Map
      val uDDActivePeriodsMap = fileXmlWIthMeta.asInstanceOf[GasUnzipMetadata].externalInfo.uDDActivePeriodsMap.value
      //get valid period for this piva UdD
      val pIvaUdDActivePeriod = uDDActivePeriodsMap.get(pIvaUdDFile)

      if(pIvaUdDActivePeriod.isDefined){ //apply the rule if a period exist for this pIva

        val startDate =  pIvaUdDActivePeriod.get._1.toLocalDate
        val endDate =  pIvaUdDActivePeriod.get._2.toLocalDate

        val submissionStartDate = annoMeseFile.atDay(1)
        val submissionEndDate = submissionStartDate.plusMonths(1).plusDays(xGiorni-1)

       /*
       ->------------------>---------------------------TIME---------------------------->--------------------------->
        - CASE 1:
                                              startDate     endDate
                                                |--------------|

      submissionStartDate     submissionEndDate
         |-----------------------------|

        - CASE 2:
                                                startDate     endDate
                                                |--------------|

                                                                          submissionStartDate     submissionEndDate
                                                                             |-----------------------------|


        - CASE _ :
             In all the other case there is an intersection

       * */

        submissionEndDate.isBefore(startDate) || submissionStartDate.isAfter(endDate)

      } else false

    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- PIVA UdD non coerente",
      bloccante = NON_BLOCCANTE,
      ammissibilita= FILE
    )
  )
  val ruleTimestamp:Rule = Rule(
    ruleName = "ruleTimestamp",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      DateTimeUtility.getDateTimeOrNull(fileXmlWIthMeta.timestamp,FILE_TIMESTAMP_PATTERN)==null
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "Timestamp formalmente errato" ,
      bloccante = BLOCCANTE,
      ammissibilita= FILE
    )
  )
  val ruleCodFlusso:Rule = Rule(
    ruleName = "ruleCodFlusso",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {

      !fileXmlWIthMeta.flusso.equalsIgnoreCase( (xmlFile \ cod_flusso).text )
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "Codice Flusso errato",
      bloccante = BLOCCANTE,
      ammissibilita= FILE
    )
  )
  val ruleTraccaitoStandard:Rule = Rule(
    ruleName = "ruleTraccaitoStandard",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      ! (fileXmlWIthMeta.tS.equalsIgnoreCase("M") || fileXmlWIthMeta.tS.equalsIgnoreCase("R") )
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE ,
      bloccante = BLOCCANTE,
      ammissibilita= FILE
    )
  )
  val ruleAlreadyTransmitted:Rule = Rule(
    ruleName = "ruleAlreadyTransmitted",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      fileXmlWIthMeta.asInstanceOf[GasUnzipMetadata].alreadyTransmitted
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD919,
      descrizione = ERROR_FILE_ALREADY_TRANSMITTED ,
      bloccante = BLOCCANTE,
      ammissibilita= FILE
    )
  )
  val ruleFlusso1ValidateXML:Rule = Rule(
    ruleName = "ruleFlusso1ValidateXML",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val metadata:GasUnzipMetadata = fileXmlWIthMeta.asInstanceOf[GasUnzipMetadata]

      // checking if flusso is not in flusso2list: this ensure that whenever it is a flusso1 it is validated correctly but
      // if it is a flusso that should be blocked it will be blocked since never seen before
      if (fileXmlWIthMeta.tS.equals("M")) {

        val v = Try(metadata.externalInfo.flusso1XSD.validate(new StreamSource(nodeSeqToInputBuffer(xmlFile))))
        if (v.isFailure) {
          logger.warn(s"Flusso 1 XSD val error for  ${fileXmlWIthMeta.file.getAbsolutePath}")
          true
        } else false

      } else {
        false
      }
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- Struttura xsd non conforme" ,
      bloccante = BLOCCANTE,
      ammissibilita= FILE
    )
  )
  val ruleFlusso2ValidateXML:Rule = Rule(
    ruleName = "ruleFlusso2ValidateXML",
    condition =  (xmlFile: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val metadata:GasUnzipMetadata = fileXmlWIthMeta.asInstanceOf[GasUnzipMetadata]

      if (fileXmlWIthMeta.tS.equals("R")) {

        val v = Try(metadata.externalInfo.flusso2XSD.validate(new StreamSource(nodeSeqToInputBuffer(xmlFile))))
        if (v.isFailure) {
          logger.warn(s"Flusso 2 XSD val error for ${fileXmlWIthMeta.file.getAbsolutePath}")
          true
        } else false

      } else {
        false
      }
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- Struttura xsd non conforme" ,
      bloccante = BLOCCANTE,
      ammissibilita= FILE
    )
  )

  def nodeSeqToInputBuffer(nodeSeq: NodeSeq): InputStream =  new ByteArrayInputStream(nodeSeq.toString.getBytes)

  printRules()
}
