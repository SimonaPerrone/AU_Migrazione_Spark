package it.eng.au.ammissibilita.file

import it.eng.au.ammissibilita.CheckAmmissibilitaRules
import it.eng.au.model.{ReportEsitoFILEMessage, ReportMessage, Rule, RuleParameters, XMLMetadata}
import it.eng.au.schema.GenericXmlSchema._
import it.eng.au.utility.Constants._

import java.io.{ByteArrayInputStream, InputStream}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, YearMonth}
import javax.xml.transform.stream.StreamSource
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

object CheckAmmissibilitaFileRulesSMIS extends CheckAmmissibilitaRules[ReportEsitoFILEMessage] {
  override def ammissibilitaType = "smis.file"

  override def okMessage: ReportMessage = ReportEsitoFILEMessage(bloccante = OK)

  override def addFileInfoToMessage(message: ReportMessage, xml: NodeSeq, fileXmlWIthMeta: XMLMetadata): ReportEsitoFILEMessage = {
    message.asInstanceOf[ReportEsitoFILEMessage].copy(cartellaCloud = fileXmlWIthMeta.file.getParent, nomeFile = fileXmlWIthMeta.file.getName)
  }

  override def rules: List[Rule] = List(
    ruleAlreadyTransmitted, //1
    rulePIVARcuDistr, //2
    rulePIVADistributorePratica, //3
    rulePIVAUddRcu, //4
    ruleCodDpPIVA, //5
    ruleTimestamp, // 6
    rulePastFutureMonth, // 7
    ruleFlussoXsdVal, //8
    rulePIVADistributore, //9
    rulePIVAUdd, //10
    ruleCodDp, //11
    ruleCodFlusso, //12
    rulePIVARcuEmtCodContrDisp, //13
    ruleCodContrDispStruct //14
  )

  //sarebbe l'11
  val ruleAlreadyTransmitted: Rule = Rule( //1
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

  val rulePIVARcuDistr: Rule = Rule( //2
    ruleName = "rulePIVARcuDistr",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {

      val pivaUddRCUMap = fileXmlWIthMeta.listPivaRcuDistr.value

      !pivaUddRCUMap.contains(fileXmlWIthMeta.pivaDistributore)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- PIVA Distributore non attivo in RCU",
      ammissibilita = FILE
    )
  )

  //corrisponde alla 2
  val rulePIVADistributorePratica: Rule = Rule( //3
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
      descrizione = ERROR_FILE_STRUCTURE + "- PIVA Distributore non coerente",
      ammissibilita = FILE
    )
  )

  //corrisponde alla 4
  val rulePIVAUddRcu: Rule = Rule( //4
    ruleName = "rulePIVAUddRcu",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {

      val annoMeseFile = Try(YearMonth.parse(fileXmlWIthMeta.annoMese, DateTimeFormatter.ofPattern("yyyyMM")))
      val pivaUddRCUMap = fileXmlWIthMeta.mapPivaRcu.value
      val dpSkippingRule = Set("DP7001", "DP7002", "DP7003", "DP7004", "DP0426", "DP1706")
      if (dpSkippingRule.contains(fileXmlWIthMeta.codDp.toUpperCase))
        false
      else {
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
      descrizione = ERROR_FILE_STRUCTURE + "- PIVA UDD RCU non coerente",
      ammissibilita = FILE
    )
  )

  //corrisponde alla 6
  val ruleCodDpPIVA: Rule = Rule( //5
    ruleName = "ruleCodDpPIVA",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      val codDpPivaUddMap = fileXmlWIthMeta.mapCodDPRcuPivaUdd.value
      val dpSkippingRule = Set("DP7001", "DP7002", "DP7003", "DP7004", "DP0426", "DP1706")
      val pivasUddSet = codDpPivaUddMap.get(fileXmlWIthMeta.codDp)
      if (dpSkippingRule.contains(fileXmlWIthMeta.codDp.toUpperCase)) {
        false //skip the rule
      } else {
        pivasUddSet.isEmpty || (!pivasUddSet.get.contains(fileXmlWIthMeta.pivaUDD))
      }
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- COD_DP non coerente con PIVA file",
      ammissibilita = FILE
    )
  )

  //corrisponde alla 7
  val ruleTimestamp: Rule = Rule( //6
    ruleName = "ruleTimestamp",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      Try(LocalDateTime.parse(fileXmlWIthMeta.timestamp, DateTimeFormatter.ofPattern(FILE_TIMESTAMP_PATTERN))).isFailure
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- timestamp",
      ammissibilita = FILE
    )
  )

  //corrisponde alla 14
  val rulePastFutureMonth: Rule = Rule( //7
    ruleName = "rulePastFutureMonth",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, ruleParam: Option[RuleParameters]) => {
      var x = 68
      if (ruleParam.isDefined) {
        x = Try(ruleParam.get.parameters("x")) match {
          case Success(ruleParameter) => Try(ruleParameter.trim.toInt) match {
            case Success(value) => value
            case Failure(_) => 68
          }
          case Failure(_) => 68
        }
      }
      val paramsYearMonth = fileXmlWIthMeta.params.year + fileXmlWIthMeta.params.month
      //If fileXmlWIthMeta.params are not correctly parsed the process explodes here, due to YearMonth formatting error
      //As precondition we assume fileXmlWIthMeta.params correctly set when created
      val processNow = YearMonth.parse(paramsYearMonth, DateTimeFormatter.ofPattern("yyyyMM"))

      Try(YearMonth.parse(fileXmlWIthMeta.annoMese, DateTimeFormatter.ofPattern("yyyyMM"))) match {
        case Success(annoMese) => annoMese.isAfter(processNow) || annoMese.isBefore(processNow.minusMonths(x))
        case Failure(exception) => true
      }
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD914,
      descrizione = ERROR_FILE_FUTURE,
      ammissibilita = FILE
    )
  )

  //corrisponde al 12
  val ruleFlussoXsdVal: Rule = Rule( //8
    ruleName = "ruleFlussoXsdVal",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      Try(fileXmlWIthMeta.flussoSMISXSDBroad.validate(new StreamSource(nodeSeqToInputStream(fileContent)))).isFailure
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- Flusso SMIS",
      ammissibilita = FILE
    )
  )

  //corrisiponde alla 1
  val rulePIVADistributore: Rule = Rule( //9
    ruleName = "rulePIVADistributore",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      !fileXmlWIthMeta.pivaDistributore.toUpperCase().equals((fileContent \\ IdentificativiFlusso \\ PIvaDistributore).text.toUpperCase())
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- PIVA Distributore flusso non coerente",
      ammissibilita = FILE
    )
  )
  //corrisponde alla 3
  val rulePIVAUdd: Rule = Rule( //10
    ruleName = "rulePIVAUdd",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      !fileXmlWIthMeta.pivaUDD.equalsIgnoreCase((fileContent \\ IdentificativiFlusso \\ PIvaUtente).text)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- PIVA UDD flusso non coerente",
      ammissibilita = FILE
    )
  )


  //corrisponde alla 5
  val ruleCodDp: Rule = Rule( //11
    ruleName = "ruleCodDp",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      !fileXmlWIthMeta.codDp.equalsIgnoreCase((fileContent \\ IdentificativiFlusso \\ CodContrDisp).text)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- COD_DP flusso non coerente",
      ammissibilita = FILE
    )
  )
  //corrisponde alla 8
  val ruleCodFlusso: Rule = Rule( //12
    ruleName = "ruleCodFlusso",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      !fileXmlWIthMeta.flusso.equalsIgnoreCase((fileContent \\ FlussoMisure \ CodFlusso).text)
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- CodFlusso",
      ammissibilita = FILE
    )
  )

  val rulePIVARcuEmtCodContrDisp: Rule = Rule( //13
    ruleName = "rulePIVARcuEmtCodContrDisp",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {

      val pivaUddRCUMap = fileXmlWIthMeta.listPivaRcuEmt.value

      if ((fileContent \\ IdentificativiFlusso \\ CodContrDisp).text == "DP0426") {
        !pivaUddRCUMap.contains(fileXmlWIthMeta.pivaUDD)
      }
      else false
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + "- PIVA_UTENTE non identifica un esercente la maggior tutela in RCU",
      ammissibilita = FILE
    )
  )

  val ruleCodContrDispStruct: Rule = Rule( //14
    ruleName = "ruleCodContrDispStruct",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      !(fileContent \\ IdentificativiFlusso \\ CodContrDisp).text.matches("(DP|dp|Dp|dP)(000[1-9]|00[1-9][0-9]|0[1-9][0-9][0-9]|[1-9][0-9][0-9][0-9])")
    },
    message = ReportEsitoFILEMessage(
      codiceInamissibilita = COD967,
      descrizione = "Codice Contratto Dispacciamento strutturalmente non corretto",
      ammissibilita = FILE
    )
  )

  def nodeSeqToInputStream(nodeSeq: NodeSeq): InputStream = new ByteArrayInputStream(nodeSeq.toString.getBytes)

}
