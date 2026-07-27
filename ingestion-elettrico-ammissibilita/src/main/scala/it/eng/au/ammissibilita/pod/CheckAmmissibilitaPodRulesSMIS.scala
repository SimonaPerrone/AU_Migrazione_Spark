package it.eng.au.ammissibilita.pod

import it.eng.au.ammissibilita.CheckAmmissibilitaRules
import it.eng.au.model.{ReportEsitoPODMessage, ReportMessage, Rule, XMLMetadata}
import it.eng.au.schema.GenericXmlSchema._
import it.eng.au.utility.Constants._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Try
import scala.xml.NodeSeq

object CheckAmmissibilitaPodRulesSMIS extends CheckAmmissibilitaRules[ReportEsitoPODMessage] {
  override def ammissibilitaType = "smis.pod"


  override def addFileInfoToMessage(message: ReportMessage, xml: NodeSeq, fileXmlWIthMeta: XMLMetadata): ReportEsitoPODMessage = {
    message.asInstanceOf[ReportEsitoPODMessage].copy(cartellaCloud = fileXmlWIthMeta.file.getParent, nomeFile = fileXmlWIthMeta.file.getName, pod = (xml \\ Pod).text, flusso = fileXmlWIthMeta.flusso)
  }

  override def okMessage: ReportMessage = ReportEsitoPODMessage(bloccante = OK)

  def rules: List[Rule] = List(
    ruleIsPodActive,              //14bis
    ruleMotivazione,              //15
    ruleDataMisura,               //16
    ruleTipoMisuratoreMontaggio,  //17
    rulePodCompetenceDistributor, //18
    rulePodCompetenceUdd, //19
    ruleMisuratoreSmontaggioG,    //20
    ruleMisuratoreMontaggioG,     //21
    ruleDataMontaggioValorizzata, //22
    ruleMisuratoreSmontaggioT,    //23
    ruleMisuratoreMontaggioT      //24
  )

  val ruleIsPodActive: Rule = Rule( //14bis
    ruleName = "ruleIsPodActive",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      !fileXmlWIthMeta.isPodActiveAtDataMontaggio
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- Il POD non è attivo alla data dell’evento tecnico",
      bloccante = BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleMotivazione: Rule = Rule( //15
    ruleName = "ruleMotivazione",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      val codificheAmmesseST = Set("01", "02", "03")
      !codificheAmmesseST.contains((datiPod \\ Motivazione).text)
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- Motivazione della sezione DatiPod non corrispondente",
      bloccante = BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleDataMisura: Rule = Rule( // 16
    ruleName = "ruleDataMisura",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {

      val dataMisuraDate = Try(DateTime.parse((datiPod \\ Montaggio \\ DataMisura).text, DateTimeFormat.forPattern(ITALIAN_DATE_PATTERN)))
      val fileDate = DateTime.parse(fileXmlWIthMeta.annoMese, DateTimeFormat.forPattern("yyyyMM"))

      !isValued(datiPod \\ Montaggio \\ DataMisura) ||
        dataMisuraDate.isFailure ||
        dataMisuraDate.get.year() != fileDate.year() ||
        dataMisuraDate.get.monthOfYear() != fileDate.monthOfYear()
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- Data misura",
      bloccante = BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleTipoMisuratoreMontaggio: Rule = Rule( //17
    ruleName = "ruleTipoMisuratoreMontaggio",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      val misuratoriAmmessi = Set("G", "E", "T")
      !misuratoriAmmessi.contains((datiPod \\ Montaggio \\ TipoMisuratore).text)
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- TipoMisuratore non corrispondente",
      bloccante = BLOCCANTE,
      ammissibilita = POD
    )
  )

  val rulePodCompetenceDistributor: Rule = Rule( //18
    ruleName = "rulePodCompetenceDistributor",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      !fileXmlWIthMeta.isPodCompetenceDistr
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_POD_COMPETENCE + "- Distributore",
      bloccante = BLOCCANTE,
      ammissibilita = POD
    )
  )

  val rulePodCompetenceUdd: Rule = Rule( //19
    ruleName = "rulePodCompetenceUdd",
    condition = (fileContent: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      !fileXmlWIthMeta.isPodCompetenceUdd
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_POD_COMPETENCE + "- UDD",
      bloccante = BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleMisuratoreSmontaggioG: Rule = Rule( //20
    ruleName = "ruleMisuratoreSmontaggioG",
    condition = (datiPod: NodeSeq, fileXmlWithMeta: XMLMetadata, _) => {
      (datiPod \\ Smontaggio \\ TipoMisuratore).text == "G" &&
        (!isValued(datiPod \\ Smontaggio \\ EaF1) || !isValued(datiPod \\ Smontaggio \\ EaF2) ||
        !isValued(datiPod \\ Smontaggio \\ EaF3) || !isValued(datiPod \\ Smontaggio \\ PotF1) ||
        !isValued(datiPod \\ Smontaggio \\ PotF2) || !isValued(datiPod \\ Smontaggio \\ PotF3))
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- valori dei totalizzatori per Fasce Smontaggio",
      bloccante = BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleMisuratoreMontaggioG: Rule = Rule( //21
    ruleName = "ruleMisuratoreMontaggioG",
    condition = (datiPod: NodeSeq, fileXmlWithMeta: XMLMetadata, _) => {
      (datiPod \\ Montaggio \\ TipoMisuratore).text == "G" &&
        (!isValued(datiPod \\ Montaggio \\ EaF1) || !isValued(datiPod \\ Montaggio \\ EaF2) ||
          !isValued(datiPod \\ Montaggio \\ EaF3) || !isValued(datiPod \\ Montaggio \\ PotF1) ||
          !isValued(datiPod \\ Montaggio \\ PotF2) || !isValued(datiPod \\ Montaggio \\ PotF3))
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- valori dei totalizzatori per Fasce Montaggio",
      bloccante = BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleDataMontaggioValorizzata: Rule = Rule( //22
    ruleName = "ruleDataMontaggioValorizzata",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      !(
        ((datiPod \\ Motivazione).text == "01" && isValued(datiPod \\ Montaggio \\ DataMessaRegime2G))
          || (datiPod \\ Motivazione).text != "01"
        )
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- data messa a regime 2G",
      bloccante = BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleMisuratoreSmontaggioT: Rule = Rule( //23
    ruleName = "ruleMisuratoreSmontaggioT",
    condition = (datiPod: NodeSeq, fileXmlWithMeta: XMLMetadata, _) => {
      (datiPod \\ Smontaggio \\ TipoMisuratore).text == "T" &&
        (!isValued(datiPod \\ Smontaggio \\ EaM) || !isValued(datiPod \\ Smontaggio \\ PotM))
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- valori del totalizzatore monorario Smontaggio",
      bloccante = BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleMisuratoreMontaggioT: Rule = Rule( //24
    ruleName = "ruleMisuratoreMontaggioT",
    condition = (datiPod: NodeSeq, fileXmlWithMeta: XMLMetadata, _) => {
      (datiPod \\ Montaggio \\ TipoMisuratore).text == "T" &&
        (!isValued(datiPod \\ Montaggio \\ EaM) || !isValued(datiPod \\ Montaggio \\ PotM))
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- valori del totalizzatore monorario Montaggio",
      bloccante = BLOCCANTE,
      ammissibilita = POD
    )
  )

}
