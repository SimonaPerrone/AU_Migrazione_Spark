package it.eng.au.ammissibilita.pod

import it.eng.au.ammissibilita.CheckAmmissibilitaRules
import it.eng.au.model.{ReportEsitoPODMessage, ReportMessage, Rule, XMLMetadata}
import it.eng.au.schema.GenericXmlSchema._
import it.eng.au.utility.Constants._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Try
import scala.xml.NodeSeq

object CheckAmmissibilitaPodRules extends CheckAmmissibilitaRules[ReportEsitoPODMessage] {
  override def ammissibilitaType = "pod"


  override def addFileInfoToMessage(message: ReportMessage, xml: NodeSeq, fileXmlWIthMeta: XMLMetadata): ReportEsitoPODMessage = {
    message.asInstanceOf[ReportEsitoPODMessage].copy(cartellaCloud = fileXmlWIthMeta.file.getParent, nomeFile = fileXmlWIthMeta.file.getName, pod = (xml \\ Pod).text, flusso = fileXmlWIthMeta.flusso)
  }

  override def okMessage: ReportMessage = ReportEsitoPODMessage(bloccante = OK)

  def rules: List[Rule] = List(
    ruleDataMisura, //16
    ruleTrattamento, //23
    ruleMeseAnno, //17
    ruleCodPrat, //18
    ruleDataPrest1, //19
    ruleDataPrest2, //20
    ruleKaKrKp, //21
    ruleMotivazioneStima, //22
    ruleCodFlussoRaccolta, //24
    ruleCodFlussoTipoRettifica, //25
    ruleTrattamentoConsumo, //29
    ruleEaEr, //27
    //(28 is removed from specs)
    ruleCodFlussoPotMax, //26
    ruleTrattamentoConsumoCurva, //30
    ruleForfaitConsumo, //31
    ruleMotivazioneMisuraConsumo, //32
    ruleTipoRettificaMotivazione //33
  )

  @deprecated("Use CheckAmmissibilitaFileRules.ruleDatiFile instead", "11/01/2021")
  val ruleDatiFile: Rule = Rule( // 15
    ruleName = "ruleDatiFile",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      val fileDate = DateTime.parse(fileXmlWIthMeta.annoMese, DateTimeFormat.forPattern("yyyyMM"))
      val fileDateMinusMonth = fileDate.minusMonths(1)
      val meseAnnoDate = Try(DateTime.parse((datiPod \\ MeseAnno).text, DateTimeFormat.forPattern(MONTH_YEAR_PATTERN)))
      val dataMisuraDate = Try(DateTime.parse((datiPod \\ DataMisura).text, DateTimeFormat.forPattern(ITALIAN_DATE_PATTERN)))

      (isValued(datiPod \\ MeseAnno) && (isFlusso(fileXmlWIthMeta.codFlusso, flusso1List) || isFlusso(fileXmlWIthMeta.codFlusso, flusso2List)) &&
        (fileDate.year() != meseAnnoDate.get.year() || fileDate.monthOfYear() != meseAnnoDate.get.monthOfYear()) &&
        (fileDateMinusMonth.year() != meseAnnoDate.get.year() || fileDateMinusMonth.monthOfYear() != meseAnnoDate.get.monthOfYear())
        ) ||
        (isValued(datiPod \\ DataMisura) && (
          (dataMisuraDate.get.year() != fileDate.year() || dataMisuraDate.get.monthOfYear() != fileDate.monthOfYear()) &&
            (dataMisuraDate.get.year() != fileDateMinusMonth.year() || dataMisuraDate.get.monthOfYear() != fileDateMinusMonth.monthOfYear())
          ))
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD906,
      descrizione = "I dati del file non sono riferiti al mese di riferimento della misura",
      bloccante = BLOCCANTE,
      ammissibilita = FILE
    )
  )

  val ruleDataMisura: Rule = Rule( // 16
    ruleName = "ruleDataMisura",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      (isFlusso(fileXmlWIthMeta.codFlusso, flusso1List) || isFlusso(fileXmlWIthMeta.codFlusso, flusso2List)) &&
        Set("M", "F", "C").contains((datiPod \\ Trattamento).text) &&
        (!isValued(datiPod \\ DataMisura) || Try(DateTime.parse((datiPod \\ DataMisura).text, DateTimeFormat.forPattern(ITALIAN_DATE_PATTERN))).isFailure)
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- Datamisura non valorizzato",
      bloccante = NON_BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleMeseAnno: Rule = Rule( //17
    ruleName = "ruleMeseAnno",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      (isFlusso(fileXmlWIthMeta.codFlusso, flusso1List) || isFlusso(fileXmlWIthMeta.codFlusso, flusso2List)) &&
        (datiPod \\ Trattamento).text == "O" &&
        (!isValued(datiPod \\ MeseAnno) || Try(DateTime.parse((datiPod \\ MeseAnno).text, DateTimeFormat.forPattern(MONTH_YEAR_PATTERN))).isFailure)
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- meseanno non valorizzato",
      bloccante = BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleCodPrat: Rule = Rule( //18
    ruleName = "ruleCodPrat",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      isFlusso(fileXmlWIthMeta.codFlusso, flusso1List) &&
        Set("S", "V").contains((datiPod \\ Raccolta).text) &&
        !isValued(datiPod \\ CodPrat_SII)
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- CodPrat_SII non valorizzato",
      bloccante = NON_BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleDataPrest1: Rule = Rule( //19
    ruleName = "ruleDataPrest1",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      isFlusso(fileXmlWIthMeta.codFlusso, flusso1List) &&
        Set("S", "V", "T").contains((datiPod \\ Raccolta).text) &&
        (!isValued(datiPod \\ DataPrest) || Try(DateTime.parse((datiPod \\ DataPrest).text, DateTimeFormat.forPattern(ITALIAN_DATE_PATTERN))).isFailure)
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- data prestazione Flusso 1 non valorizzato",
      bloccante = NON_BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleDataPrest2: Rule = Rule( //20
    ruleName = "ruleDataPrest2",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      isFlusso(fileXmlWIthMeta.codFlusso, flusso2List) &&
        Set("S", "V", "T").contains((datiPod \\ TipoRettifica).text) &&
        (!isValued(datiPod \\ DataPrest) || Try(DateTime.parse((datiPod \\ DataPrest).text, DateTimeFormat.forPattern(ITALIAN_DATE_PATTERN))).isFailure)
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- data prestazione Flusso 2 non valorizzato",
      bloccante = NON_BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleKaKrKp: Rule = Rule( //21
    ruleName = "ruleKaKrKp",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      Set("PDO2G", "SNM2G", "SNM", "PNO2G", "PNO", "VNO2G", "VNO", "RFO2G", "RSN2G", "RSN", "RNO2G", "RNO", "RNV2G", "RNV").contains(fileXmlWIthMeta.codFlusso) &&
        ((datiPod \\ Ka).isEmpty || (datiPod \\ Kr).isEmpty || (datiPod \\ Kp).isEmpty)
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- “ka”, “kr”, “kp” non valorizzati",
      bloccante = NON_BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleMotivazioneStima: Rule = Rule( //22
    ruleName = "ruleMotivazioneStima",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      isFlusso(fileXmlWIthMeta.codFlusso, flusso1List) &&
        (datiPod \\ TipoDato).text == "S" &&
        !isValued(datiPod \\ MotivazioneStima)
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- Motivazione stima non valorizzato",
      bloccante = NON_BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleTrattamento: Rule = Rule( //23
    ruleName = "ruleTrattamento",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      Set("PNO2G", "VNO").contains(fileXmlWIthMeta.codFlusso) &&
        (datiPod \\ Trattamento).text.equalsIgnoreCase("O")
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- Trattamento non coerente con codice flusso",
      bloccante = NON_BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleCodFlussoRaccolta: Rule = Rule( //24
    ruleName = "ruleCodFlussoRaccolta",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      isFlusso(fileXmlWIthMeta.codFlusso, flusso1List) &&
        (Set("VNO", "VNO2G").contains(fileXmlWIthMeta.codFlusso) &&
          !(datiPod \\ Raccolta).text.equals("V")) ||
        (Set("SNM", "SNM2G").contains(fileXmlWIthMeta.codFlusso) &&
          !(datiPod \\ Raccolta).text.equals("S"))
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- Raccolta",
      bloccante = NON_BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleCodFlussoTipoRettifica: Rule = Rule( //25
    ruleName = "ruleCodFlussoTipoRettifica",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      isFlusso(fileXmlWIthMeta.codFlusso, flusso2List) &&
        (Set("RNV", "RNV2G").contains(fileXmlWIthMeta.codFlusso) &&
          !(datiPod \\ TipoRettifica).text.equals("V")) ||
        (Set("RSN", "RSN2G").contains(fileXmlWIthMeta.codFlusso) &&
          !(datiPod \\ TipoRettifica).text.equals("S"))
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- TipoRettifica",
      bloccante = NON_BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleCodFlussoPotMax: Rule = Rule( //26
    ruleName = "ruleCodFlussoPotMax",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {

      val interestedFlows = Set("PDO", "“PDO2G", "PNO2G", "VNO2G", "RFO", "RFO2G", "RNO2G", "RNV2G")
      val rettInterestedFlows = flusso2List.map(_.toUpperCase).intersect(interestedFlows.toList)

      // the rule is applied for all the interested flows except if they are rectification flows with "motivazione" field
      // equals to 3
      if (rettInterestedFlows.contains(fileXmlWIthMeta.codFlusso.toUpperCase)
        && isValued(datiPod \\ Motivazione)
        && (datiPod \\ Motivazione).text.trim.equalsIgnoreCase("3")) false
      else interestedFlows.contains(fileXmlWIthMeta.codFlusso.toUpperCase) && (!isValued(datiPod \\ PotMax))
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- “PotMax” non presente",
      bloccante = NON_BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleEaEr: Rule = Rule( //27
    ruleName = "ruleEaEr",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {

      //val dst1AttribMandatoryList: List[String] = ((1 to 8).toList ++ (13 to 96).toList).map(el => s"@E$el")
      val dst1AttribNullList: List[String] = (9 to 12).toList.map(el => s"@E$el")

      val dst2AttribList: List[String] = (1 to 12).toList.map(el => s"@E$el")
      val dst2AttribNullList: List[String] = (13 to 96).toList.map(el => s"@E$el")

      val dst3AttribList = (9 to 96).toList.map(el => s"@E$el")
      val dst3AttribNullList: List[String] = (1 to 8).toList.map(el => s"@E$el")

      val errorEaList = (datiPod \\ Ea).map(node => {
        (node \ Dst).text match {
          //error if all mandatory are not valued or all the null-field are not null
          case "1" => //(!dst1AttribMandatoryList.forall(eAttribute => isValued(node \ eAttribute))) ||
            dst1AttribNullList.exists(eAttribute => isValued(node \ eAttribute))

          case "2" => !dst2AttribList.forall(eAttribute => isValued(node \ eAttribute)) ||
            dst2AttribNullList.exists(eAttribute => isValued(node \ eAttribute))

          case "3" => !dst3AttribList.forall(eAttribute => isValued(node \ eAttribute)) ||
            dst3AttribNullList.exists(eAttribute => isValued(node \ eAttribute))

          case _ => false
        }
      })

      val errorEa = if (errorEaList.isEmpty) false else errorEaList.reduce(_ || _)

      val errorErList = (datiPod \\ Er).map(node => {
        (node \ Dst).text match {
          //error if all mandatory are not valued or all the null-field are not null
          case "1" => //(!dst1AttribMandatoryList.forall(eAttribute => isValued(node \ eAttribute))) ||
            dst1AttribNullList.exists(eAttribute => isValued(node \ eAttribute))

          case "2" => !dst2AttribList.forall(eAttribute => isValued(node \ eAttribute)) ||
            dst2AttribNullList.exists(eAttribute => isValued(node \ eAttribute))

          case "3" => !dst3AttribList.forall(eAttribute => isValued(node \ eAttribute)) ||
            dst3AttribNullList.exists(eAttribute => isValued(node \ eAttribute))

          case _ => false
        }
      })

      val errorEr = if (errorErList.isEmpty) false else errorErList.reduce(_ || _)

      errorEa || errorEr

    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- passaggio ora – tag Dst",
      bloccante = BLOCCANTE,
      ammissibilita = POD
    )
  )


  val ruleTrattamentoConsumo: Rule = Rule( //29
    ruleName = "ruleTrattamentoConsumo",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      /*
  * Nel FLUSSO 2 se Trattamento risulta pari a “O” e se risulta erroneamente valorizzata esclusivamente la sezione “Consumo”
  *  */
      isFlusso(fileXmlWIthMeta.codFlusso, flusso2List) &&
        (datiPod \\ Trattamento).text.equals("O") &&
        isValued(datiPod \\ Consumo)

    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- sezione “Consumo” con trattamento =”O” valorizzato",
      bloccante = BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleTrattamentoConsumoCurva: Rule = Rule( //30
    ruleName = "ruleTrattamentoConsumoCurva",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      //println(f"${(datiPod \\ Trattamento).text}\t${(datiPod \\ Trattamento).text.equals("O")}\n${  (datiPod \\ Consumo).nonEmpty}\t${  (datiPod \\ Consumo).text}")
      isFlusso(fileXmlWIthMeta.codFlusso, flusso2List) &&
        (datiPod \\ Trattamento).text.equals("O") &&
        !(datiPod \\ Consumo).text.equals("") &&
        (datiPod \\ Consumo).nonEmpty &&
        !(datiPod \\ Curva).text.equals("") &&
        (datiPod \\ Curva).nonEmpty

    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- sezione “Consumo” con trattamento =”O” valorizzato",
      bloccante = NON_BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleForfaitConsumo: Rule = Rule( //31
    ruleName = "ruleForfaitConsumo",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {
      (isFlusso(fileXmlWIthMeta.codFlusso, flusso1List) || isFlusso(fileXmlWIthMeta.codFlusso, flusso2List)) &&
        (datiPod \\ Forfait).text.equals("SI") &&
        ((datiPod \\ Consumo).isEmpty || (datiPod \\ Consumo).text.equals(""))
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- sezione “Consumo” non valorizzata",
      bloccante = NON_BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleMotivazioneMisuraConsumo: Rule = Rule( //32
    ruleName = "ruleMotivazioneMisuraConsumo",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {

      isFlusso(fileXmlWIthMeta.codFlusso, flusso2List) &&
        (datiPod \\ Motivazione).text.equals("3") &&
        (isValued(datiPod \\ Consumo) || isValued(datiPod \\ Misura))

    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- sezione valorizzata non coerente tag motivazione",
      bloccante = NON_BLOCCANTE,
      ammissibilita = POD
    )
  )

  val ruleTipoRettificaMotivazione: Rule = Rule( //33
    ruleName = "ruleTipoRettificaMotivazione",
    condition = (datiPod: NodeSeq, fileXmlWIthMeta: XMLMetadata, _) => {

      isFlusso(fileXmlWIthMeta.codFlusso, flusso2List) &&
        Set("RSN2G", "RNV2G").contains(fileXmlWIthMeta.codFlusso) &&
        ((datiPod \\ TipoRettifica).text.equals("S") || (datiPod \\ TipoRettifica).text.equals("V")) &&
        !(datiPod \\ Motivazione).text.equals("1") &&
        !(datiPod \\ Motivazione).text.equals("2") &&
        !(datiPod \\ Motivazione).text.equals("6")
    },
    message = ReportEsitoPODMessage(
      codiceInamissibilita = COD010,
      descrizione = ERROR_COMPILATION_FIELD + "- campo Motivazione",
      bloccante = NON_BLOCCANTE,
      ammissibilita = POD
    )
  )
}
