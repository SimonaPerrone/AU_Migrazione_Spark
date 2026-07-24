package it.au.misure.eng.ammissibilita.file

import it.au.misure.eng.ammissibilita.CheckAmmissibilitaRules
import it.au.misure.eng.model.{ReportEsitoFILEMessage, ReportMessage, Rule, XMLMetadata}
import it.au.misure.eng.schema.GenericXmlSchema._
import it.au.misure.eng.utility.Constants._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import java.io.{ByteArrayInputStream, InputStream}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, YearMonth}
import javax.xml.transform.stream.StreamSource
import scala.util.{Failure, Success, Try}
import scala.xml.{Node, NodeSeq}

object CheckAmmissibilitaFileRules extends CheckAmmissibilitaRules[ReportEsitoFILEMessage] {
  override def ammissibilitaType = "file"

  override def okMessage: ReportMessage = ReportEsitoFILEMessage(bloccante = OK)

  override def addFileInfoToMessage(message: ReportMessage, xml: NodeSeq, fileXmlWIthMeta: XMLMetadata): ReportEsitoFILEMessage = {
    message.asInstanceOf[ReportEsitoFILEMessage].copy(cartellaCloud = fileXmlWIthMeta.file.getParent, nomeFile = fileXmlWIthMeta.file.getName)
  }

  override def rules: List[Rule] = List(
    ruleFlusso1XsdVal, //12
    ruleFlusso2XsdVal, //13
    rulePIVADistributore, //1
    rulePIVADistributorePratica, //2
    rulePIVAUdd, //3
    rulePIVAUddRcu, //4
    ruleCodDp, //5
    ruleCodDpPIVA, //6
    ruleTimestamp, //7
    ruleCodFlusso, //8
    ruleStatoMisuratore, //9
    ruleStatoMisuratoreFlusso, //10
    ruleAlreadyTransmitted, //11
    rulePastFutureMonth, //14 (rule 15 in CheckAmmissibilitaPodRules)
    ruleDatiFile //15
  )

  val rulePIVADistributore: Rule = Rule( // 1
    ruleName = "rulePIVADistributore",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      !fileXmlWIthMeta.pivaDistributore.toUpperCase().equals((fileContent \\ IdentificativiFlusso \\ PIvaDistributore).text.toUpperCase())
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE,
      ammissibilita = FILE
    )
  )

  val rulePIVADistributorePratica: Rule = Rule( //2
    ruleName = "rulePIVADistributorePratica",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      val pivasPath = fileXmlWIthMeta.file.getParentFile.getParentFile.getParentFile.getName.split("_")
      val pivaDistrPath = pivasPath(1)
      val pivaUddPath = pivasPath(2)

      !pivaDistrPath.equalsIgnoreCase((fileContent \\ IdentificativiFlusso \\ PIvaDistributore).text) ||
        !pivaUddPath.equalsIgnoreCase((fileContent \\ IdentificativiFlusso \\ PIvaUtente).text)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE,
      ammissibilita = FILE
    )
  )

  val rulePIVAUdd: Rule = Rule( //3
    ruleName = "rulePIVAUdd",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      !fileXmlWIthMeta.pivaUDD.equalsIgnoreCase((fileContent \\ IdentificativiFlusso \\ PIvaUtente).text)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE,
      ammissibilita = FILE
    )
  )

  val rulePIVAUddRcu: Rule = Rule( //4
    ruleName = "rulePIVAUddRcu",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      val annoMeseFile = Try(YearMonth.parse(fileXmlWIthMeta.annoMese, DateTimeFormatter.ofPattern("yyyyMM")))
      val dpSkippingRule = Set("DP7001", "DP7002", "DP7003", "DP7004", "DP0426", "DP1706")
      if (dpSkippingRule.contains(fileXmlWIthMeta.codDp.toUpperCase)) {
        false
      } else {

        val pivaUddRCUMap = fileXmlWIthMeta.mapPivaRcu.value
        val tpl = pivaUddRCUMap.get(fileXmlWIthMeta.pivaUDD)
        if (tpl.isEmpty) true //if the map has no entry for this pivaudd then the piva is not active
        else {
          val (pivaStartDate, pivaEndDate) = tpl.get

          annoMeseFile.isFailure ||
            annoMeseFile.get.isBefore(YearMonth.from(pivaStartDate)) ||
            annoMeseFile.get.isAfter(YearMonth.from(pivaEndDate))
        }
      }

    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE,
      ammissibilita = FILE
    )
  )

  val ruleCodDp: Rule = Rule( //5
    ruleName = "ruleCodDp",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      !fileXmlWIthMeta.codDp.equalsIgnoreCase((fileContent \\ IdentificativiFlusso \\ CodContrDisp).text)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE,
      ammissibilita = FILE
    )
  )

  val ruleCodDpPIVA: Rule = Rule( //6
    ruleName = "ruleCodDpPIVA",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      val codDpPivaUddMap = fileXmlWIthMeta.mapCodDPRcuPivaUdd.value
      val dpSkippingRule = Set("DP7001", "DP7002", "DP7003", "DP7004", "DP0426", "DP1706")
      if (dpSkippingRule.contains(fileXmlWIthMeta.codDp.toUpperCase)) {
        false //skip the rule
      } else {
        val pivasUddSet = codDpPivaUddMap.get(fileXmlWIthMeta.codDp)
        pivasUddSet.isEmpty || (!pivasUddSet.get.contains(fileXmlWIthMeta.pivaUDD))
      }
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE,
      ammissibilita = FILE
    )
  )

  val ruleTimestamp: Rule = Rule( //7
    ruleName = "ruleTimestamp",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      Try(LocalDateTime.parse(fileXmlWIthMeta.timestamp, DateTimeFormatter.ofPattern(FILE_TIMESTAMP_PATTERN))).isFailure
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE,
      ammissibilita = FILE
    )
  )

  val ruleCodFlusso: Rule = Rule( //8
    ruleName = "ruleCodFlusso",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      !fileXmlWIthMeta.flusso.equalsIgnoreCase((fileContent \\ FlussoMisure \ CodFlusso).text)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE,
      ammissibilita = FILE
    )
  )

  val ruleStatoMisuratore: Rule = Rule( //9
    ruleName = "ruleStatoMisuratore",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      !(fileXmlWIthMeta.sm.equalsIgnoreCase("R") || fileXmlWIthMeta.sm.equalsIgnoreCase("NR"))
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE,
      ammissibilita = FILE
    )
  )

  val ruleStatoMisuratoreFlusso: Rule = Rule( //10
    ruleName = "ruleStatoMisuratoreFlusso",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      fileXmlWIthMeta.sm.equalsIgnoreCase("NR") && fileXmlWIthMeta.flusso.equalsIgnoreCase("PDO2G")
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE,
      ammissibilita = FILE
    )
  )

  val ruleAlreadyTransmitted: Rule = Rule( //11
    ruleName = "ruleAlreadyTransmitted",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      fileXmlWIthMeta.alreadyTransmitted
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD919,
      descrizione = ERROR_FILE_ALREADY_TRANSMITTED,
      ammissibilita = FILE
    )
  )

  val ruleFlusso1XsdVal: Rule = Rule( //12
    ruleName = "ruleFlusso1XsdVal",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      if (!flusso2List.contains(fileXmlWIthMeta.flusso.toLowerCase())) {
        Try(fileXmlWIthMeta.flusso1XSDBroad.validate(new StreamSource(nodeSeqToInputStream(fileContent)))).isFailure
      } else {
        false
      }

    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE,
      ammissibilita = FILE
    )
  )

  val ruleFlusso2XsdVal: Rule = Rule( //13
    ruleName = "ruleFlusso2XsdVal",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      if (flusso2List.contains(fileXmlWIthMeta.flusso.toLowerCase())) {
        Try(fileXmlWIthMeta.flusso2XSDBroad.validate(new StreamSource(nodeSeqToInputStream(fileContent)))).isFailure
      } else {
        false
      }
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE,
      ammissibilita = FILE
    )
  )

  val rulePastFutureMonth: Rule = Rule( //14
    ruleName = "rulePastFutureMonth",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      val paramsYearMonth = fileXmlWIthMeta.params.year + fileXmlWIthMeta.params.month
      //If fileXmlWIthMeta.params are not correctly parsed the process explodes here, due to YearMonth formatting error
      //As precondition we assume fileXmlWIthMeta.params correctly set when created
      val processNow = YearMonth.parse(paramsYearMonth, DateTimeFormatter.ofPattern("yyyyMM"))

      Try(YearMonth.parse(fileXmlWIthMeta.annoMese, DateTimeFormatter.ofPattern("yyyyMM"))) match {
        case Success(annoMese) => annoMese.isAfter(processNow) || annoMese.isBefore(processNow.minusMonths(68))
        case Failure(exception) => true
      }
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD914,
      descrizione = ERROR_FILE_FUTURE,
      ammissibilita = FILE
    )
  )

  val ruleDatiFile: Rule = Rule( // 15
    ruleName = "ruleDatiFile",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {

      val meseAnnoDateNodes = fileContent \\ DatiPod \\ MeseAnno  //<MeseAnno>07/2017</MeseAnno>
      val dataMisuraDateNodes = fileContent \\ DatiPod \\ DataMisura  //<DataMisura>30/06/2012</DataMisura>
      val error =
        if (fileXmlWIthMeta.flusso.toUpperCase.equals("RFO")) {
          /**
           * For the RFO1G we must check that exist the node with data equals to annoMese from file.xml for each pod,
           * otherwise error.
           * NOTE: ROOM [AU] - Rilascio ammissibilità EE
           *
           * [18:26] Doriana L'Erario
           * Ciao Nicola
           * [18:28] Doriana L'Erario
           * i flussi RFO non hanno la Data Misura, quindi per questi flussi il controllo riguarda solo il tag MeseAnno
           *
           * */
          val datiPodNodes = fileContent \\ DatiPod
          val allPodsNodes = (fileContent \\ DatiPod \ Pod).map(_.text.trim)

          val podErrors = allPodsNodes.map(pod => {
            val podMeseAnnoDateNodes = datiPodNodes.filter(node => (node \ Pod).text.equals(pod)) \ MeseAnno
            val podDataMisuraDateNodes = datiPodNodes.filter(node => (node \ Pod).text.equals(pod)) \ DataMisura
            val meseAnnoDateNodesErrors = !podMeseAnnoDateNodes.exists(dateNodeWithSameYearMonthAsXMLFileName(_, MONTH_YEAR_PATTERN, fileXmlWIthMeta))
            val dataMisuraDateNodesErrors = !podDataMisuraDateNodes.exists(dateNodeWithSameYearMonthAsXMLFileName(_, ITALIAN_DATE_PATTERN, fileXmlWIthMeta))

            meseAnnoDateNodesErrors && dataMisuraDateNodesErrors

          /*
          val podErrors = allPodsNodes.map(pod => {
            val podMeseAnnoDateNodes = datiPodNodes.filter(node => (node \ Pod).text.equals(pod)) \ MeseAnno
            val podDataMisuraDateNodes = datiPodNodes.filter(node => (node \ Pod).text.equals(pod)) \ DataMisura

            val areMeseAnnoValued = podMeseAnnoDateNodes.exists(rule15DateNodeCheckerRFO(_, MONTH_YEAR_PATTERN, fileXmlWIthMeta)._1)
            val areMeseAnnoCorrect = podMeseAnnoDateNodes.exists(rule15DateNodeCheckerRFO(_, MONTH_YEAR_PATTERN, fileXmlWIthMeta)._2)
            val areDataMisuraValued = podDataMisuraDateNodes.exists(rule15DateNodeCheckerRFO(_, ITALIAN_DATE_PATTERN, fileXmlWIthMeta)._1)
            val areDataMisuraCorrect = podDataMisuraDateNodes.exists(rule15DateNodeCheckerRFO(_, ITALIAN_DATE_PATTERN, fileXmlWIthMeta)._2)

            ((areMeseAnnoValued && !areMeseAnnoCorrect) && (areDataMisuraValued && !areDataMisuraCorrect)) ||
              (!areMeseAnnoValued && !areDataMisuraValued)
           */

          })

          podErrors.reduce(_ || _)
        } else {
          val meseAnnoDateNodesErrors = meseAnnoDateNodes.exists(rule15DateNodeChecker(_, MONTH_YEAR_PATTERN, fileXmlWIthMeta))
          val dataMisuraDateNodesErrors = dataMisuraDateNodes.exists(rule15DateNodeChecker(_, ITALIAN_DATE_PATTERN, fileXmlWIthMeta))

          meseAnnoDateNodesErrors || dataMisuraDateNodesErrors
        }
      error
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD906,
      descrizione = "I dati del file non sono riferiti al mese di riferimento della misura",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  def nodeSeqToInputStream(nodeSeq: NodeSeq): InputStream = new ByteArrayInputStream(nodeSeq.toString.getBytes)

  protected val rule15DateNodeChecker: (Node, String, XMLMetadata) => Boolean =
    (dateNode: Node, datePattern: String, fileXmlWithMeta: XMLMetadata) => {

      val fileDate = DateTime.parse(fileXmlWithMeta.annoMese, DateTimeFormat.forPattern("yyyyMM"))
      val fileDateMinusMonth = fileDate.minusMonths(1)
      val meseAnnoDateNode = Try(DateTime.parse(dateNode.text, DateTimeFormat.forPattern(datePattern)))

      if (Set("SNM", "SNM2G", "RSN", "RSN2G").contains(fileXmlWithMeta.flusso.toUpperCase)) {
        isValued(dateNode) &&
          (meseAnnoDateNode.isFailure || fileDate.year() != meseAnnoDateNode.get.year() || fileDate.monthOfYear() != meseAnnoDateNode.get.monthOfYear()) &&
          (meseAnnoDateNode.isFailure || fileDateMinusMonth.year() != meseAnnoDateNode.get.year() || fileDateMinusMonth.monthOfYear() != meseAnnoDateNode.get.monthOfYear())
      } else {
        isValued(dateNode) &&
          (meseAnnoDateNode.isFailure || fileDate.year() != meseAnnoDateNode.get.year() || fileDate.monthOfYear() != meseAnnoDateNode.get.monthOfYear())
      }
    }

/*
  protected val rule15DateNodeCheckerRFO: (Node, String, XMLMetadata) => (Boolean, Boolean) =
    (dateNode: Node, datePattern: String, fileXmlWithMeta: XMLMetadata) => {

      val fileDate = DateTime.parse(fileXmlWithMeta.annoMese, DateTimeFormat.forPattern("yyyyMM"))
      val meseAnnoDateNode = Try(DateTime.parse(dateNode.text, DateTimeFormat.forPattern(datePattern)).withDayOfMonth(1))

      (isValued(dateNode),
        meseAnnoDateNode.isSuccess &&
          (meseAnnoDateNode.get.isAfter(fileDate.minusMonths(60)) || meseAnnoDateNode.get.isEqual(fileDate.minusMonths(60))) &&
          (meseAnnoDateNode.get.isBefore(fileDate) || meseAnnoDateNode.get.isEqual(fileDate)))
    }
*/

  protected val dateNodeWithSameYearMonthAsXMLFileName: (Node, String, XMLMetadata) => Boolean =
    (dateNode: Node, datePattern: String, fileXmlWithMeta: XMLMetadata) => {

      val fileDate = DateTime.parse(fileXmlWithMeta.annoMese, DateTimeFormat.forPattern("yyyyMM"))
      val meseAnnoDateNode = Try(DateTime.parse(dateNode.text, DateTimeFormat.forPattern(datePattern)))

      isValued(dateNode) &&
        meseAnnoDateNode.isSuccess &&
        fileDate.year().equals(meseAnnoDateNode.get.year()) &&
        fileDate.monthOfYear().equals(meseAnnoDateNode.get.monthOfYear())
    }
}
