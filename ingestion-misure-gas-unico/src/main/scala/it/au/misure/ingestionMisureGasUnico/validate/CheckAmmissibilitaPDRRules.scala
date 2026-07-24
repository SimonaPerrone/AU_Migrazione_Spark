package it.au.misure.ingestionMisureGasUnico.validate

import it.au.misure.ingestionMisureGasUnico.model.{GasMetadata, GasXmlMetadata}
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.MisuraXMLSchema
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.MisuraXMLSchema._
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.RettificaXMLSchema.{DatiLetturaRett, DatiTecnPdrRett, LettureGiornaliereRett, fine_periodo, ini_periodo, mot_ret_lett, periodo_ric, tipo_rettifica, vol_annuo_rettificato, vol_ric}
import it.au.misure.ingestionMisureGasUnico.model.validate.{ReportEsitoPDRMessage, ReportMessage, Rule, RuleParameters}
import it.au.misure.ingestionMisureGasUnico.utility.Constants._
import it.au.misure.ingestionMisureGasUnico.utility.DateTimeUtility

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, YearMonth}
import scala.util.Try
import scala.xml.{Node, NodeSeq}

class CheckAmmissibilitaPDRRules extends CheckAmmissibilitaRules[ReportEsitoPDRMessage] {
  override def ammissibilitaType = "pdr"

  override def addFileInfoToMessage(message: ReportMessage, xml: NodeSeq, fileXmlWIthMeta: GasMetadata): ReportEsitoPDRMessage = {
    message.asInstanceOf[ReportEsitoPDRMessage].copy(cartellaCloud = fileXmlWIthMeta.file.getParent, nomeFile = fileXmlWIthMeta.file.getName, pdr = (xml \\ MisuraXMLSchema.cod_pdr).text)
  }

  override def okMessage: ReportMessage = ReportEsitoPDRMessage()

  def rules: List[Rule] = List(
    ruleMeseComp, // 11
    ruleDataCompLettureGiornaliere, // 11bis
    ruleDataRaccDataComp, // 12
    ruleDataRaccLettureGiornaliereRett, // 12bis
    ruleDataPrest, // 13
    ruleUDDDataPrest, // 14
    ruleTrattamento1, // 15
    ruleTrattamento2, // 16
    ruleTrattamentoMesecomp, // 17
    ruleCodFlusso1DataRacc, // 18
    ruleCodFlusso1DataComp, // 19
    ruleCodFlusso2DataRaccRGL, // 20
    ruleCodFlusso2DataRaccRML, // 21
    ruleRaccoltaCodPratSII, // 22
    ruleRaccoltaDataPrest, // 23
    ruleTipoRettificaDataPrest, // 24
    ruleCodFlussoRaccolta, // 25
    ruleCodFlussoTipoRettifica, // 26
    ruleCodFlussoEsitoRaccolta, // 27
    ruleCodFlussoFreqLetFlusso1, // 28
    ruleCodFlussoFreqLetFlusso2, // 29
    ruleMotRetLetLetTotPrel, // 30
    ruleMotRetLetLetTotPrelRGL, // 31
    rulePreConvGruppoMisInt, // 32
    rulePreConvLetTotConv, // 33
    rulePresConvLetTotConvTGL, // 34
    ruleCodFlussoVolAnnuoRettificato, // 35
    ruleCodFlussoMotRetLetRML, // 36
    ruleCodFlussoMotRetLetRGL, // 37
    ruleRMLMotRetLet, // 38
    ruleRGLMotRetLet, // 39
    ruleClasseGruppoMisFlusso1, // 40
    ruleFlusso2MotRetLet, // 41
    ruleCoeffCorr, // 49
    ruleIGMGIGMRMatchRGLRML //50
  )

  val ruleMeseComp: Rule = Rule( // 11
    ruleName = "ruleMeseComp",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {

      val meseCompYM = Try(YearMonth.parse((pdrNode \ mese_comp).text, DateTimeFormatter.ofPattern(MONTH_YEAR_PATTERN)))

      val annoMeseFile = DateTimeUtility.getYearMonthOrNull(fileXmlWIthMeta.meseRiferimento + "/" + fileXmlWIthMeta.annoRiferimento, MONTH_YEAR_PATTERN)

        meseCompYM.isSuccess &&
        !meseCompYM.get.equals(annoMeseFile)

    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD906,
      descrizione = ERROR_MONTH_MISURA,
      bloccante = NON_BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleDataCompLettureGiornaliere: Rule = Rule( // 11bis
    ruleName = "ruleDataCompLettureGiornaliere",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {

      val flusso = fileXmlWIthMeta.flusso.toLowerCase()
      val trattamento = (pdrNode \ DatiTecnPdr \ Trattamento).text
      val meseCompYM = Try(YearMonth.parse((pdrNode \ mese_comp).text, DateTimeFormatter.ofPattern(MONTH_YEAR_PATTERN)))

      val dataCompCheck = (dataComp: Node) => {
        val parsedDataComp = Try(LocalDate.parse(dataComp.text, DateTimeFormatter.ofPattern(ITALIAN_DATE_PATTERN)))
        parsedDataComp.isSuccess &&
          parsedDataComp.get.getMonth.equals(meseCompYM.get.getMonth) &&
          parsedDataComp.get.getYear.equals(meseCompYM.get.getYear)
      }

      flusso1List.contains(flusso) &&
        trattamento.equals("G") &&
        meseCompYM.isSuccess &&
        !(pdrNode \ LettureGiornaliere \ data_comp).forall(dataCompCheck)

    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = ERROR_DATA_COMP,
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleDataRaccDataComp: Rule = Rule( //12
    ruleName = "ruleDataRaccDataComp",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val flusso = fileXmlWIthMeta.flusso.toLowerCase()
      val flussiWhiteList = Set(TML, TAL, TAS, TAV)

      val yearMonthFile = DateTimeUtility.getYearMonthOrNull(fileXmlWIthMeta.meseRiferimento + "/" + fileXmlWIthMeta.annoRiferimento, MONTH_YEAR_PATTERN)

      val dataRacc = Try(LocalDate.parse((pdrNode \ DatiLettura \ data_racc).text, DateTimeFormatter.ofPattern(ITALIAN_DATE_PATTERN)))


      val dataCompTGLCheck = (dataComp: Node) => {
        val parsedDataComp = Try(LocalDate.parse(dataComp.text, DateTimeFormatter.ofPattern(ITALIAN_DATE_PATTERN)))
        parsedDataComp.isSuccess &&
          parsedDataComp.get.getMonth.equals(yearMonthFile.getMonth) &&
          parsedDataComp.get.getYear.equals(yearMonthFile.getYear)
      }

      flusso1List.contains(flusso) && (
        (flussiWhiteList.contains(flusso) && dataRacc.isSuccess && !(dataRacc.get.getMonth.equals(yearMonthFile.getMonth) && dataRacc.get.getYear.equals(yearMonthFile.getYear))) ||
          (flusso.equalsIgnoreCase(TGL) && !(pdrNode \\ data_comp).forall(dataCompTGLCheck))
        )
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD906,
      descrizione = ERROR_MONTH_MISURA,
      bloccante = NON_BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleDataRaccLettureGiornaliereRett: Rule = Rule( // 12bis
    ruleName = "ruleDataRaccLettureGiornaliereRett",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {

      val flusso = fileXmlWIthMeta.flusso.toLowerCase()
      val trattamento = (pdrNode \ DatiTecnPdrRett \ Trattamento).text
      val meseCompYM = Try(YearMonth.parse((pdrNode \ mese_comp).text, DateTimeFormatter.ofPattern(MONTH_YEAR_PATTERN)))

      val dataRaccCheck = (dataRacc: Node) => {
        val parsedDataRacc = Try(LocalDate.parse(dataRacc.text, DateTimeFormatter.ofPattern(ITALIAN_DATE_PATTERN)))
        parsedDataRacc.isSuccess &&
          parsedDataRacc.get.getMonth.equals(meseCompYM.get.getMonth) &&
          parsedDataRacc.get.getYear.equals(meseCompYM.get.getYear)
      }

      flusso2List.contains(flusso) &&
        trattamento.equals("G") &&
        meseCompYM.isSuccess &&
        !(pdrNode \ LettureGiornaliereRett \ data_racc).forall(dataRaccCheck)

    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = ERROR_DATA_RACC,
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleDataPrest: Rule = Rule( // id 13
    ruleName = "ruleDataPrest",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val flussiWhiteList = Set(SWG1, FUI, FDD)

      val dataPrest = Try(LocalDate.parse((pdrNode \ data_prest).text, DateTimeFormatter.ofPattern(ITALIAN_DATE_PATTERN)))

      val yearMonthFile = DateTimeUtility.getYearMonthOrNull(fileXmlWIthMeta.meseRiferimento + "/" + fileXmlWIthMeta.annoRiferimento, MONTH_YEAR_PATTERN)

      flusso1List.contains(fileXmlWIthMeta.flusso.toLowerCase()) &&
        flussiWhiteList.contains(fileXmlWIthMeta.flusso.toLowerCase()) &&
        !(dataPrest.isSuccess && dataPrest.get.getMonth.equals(yearMonthFile.getMonth) && dataPrest.get.getYear.equals(yearMonthFile.getYear))

    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD906,
      descrizione = ERROR_MONTH_MISURA,
      bloccante = NON_BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleUDDDataPrest: Rule = Rule( //14
    ruleName = "ruleUDDDataPrest",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {

      val dataPrest = Try(LocalDate.parse((pdrNode \ data_prest).text, DateTimeFormatter.ofPattern(ITALIAN_DATE_PATTERN)))

      val fileYearMonth = DateTimeUtility.getYearMonthOrNull(s"${fileXmlWIthMeta.meseRiferimento}/${fileXmlWIthMeta.annoRiferimento}", MONTH_YEAR_PATTERN)

      flusso1List.contains(fileXmlWIthMeta.flusso.toLowerCase) &&
        (fileXmlWIthMeta.flusso.equalsIgnoreCase(TGL) || fileXmlWIthMeta.flusso.equalsIgnoreCase(TML)) &&
        (pdrNode \ DatiTecnPdr \ Raccolta).text.equalsIgnoreCase("S") &&
        dataPrest.isSuccess &&
        !(dataPrest.get.minusMonths(1).getYear.equals(fileYearMonth.getYear) &&
          dataPrest.get.minusMonths(1).getMonth.equals(fileYearMonth.getMonth))

    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD906,
      descrizione = ERROR_MONTH_MISURA,
      bloccante = NON_BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleTrattamento1: Rule = Rule( //15
    ruleName = "ruleTrattamento1",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {

      (fileXmlWIthMeta.flusso.equalsIgnoreCase(TGL) && !(pdrNode \ DatiTecnPdr \ Trattamento).text.equalsIgnoreCase("G")) ||
        (fileXmlWIthMeta.flusso.equalsIgnoreCase(RGL) && !(pdrNode \ DatiTecnPdrRett \ Trattamento).text.equalsIgnoreCase("G"))
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(campo Trattamento incoerente rispetto al codice flusso)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleTrattamento2: Rule = Rule( //16
    ruleName = "ruleTrattamento2",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {

      (fileXmlWIthMeta.flusso.equalsIgnoreCase(TML) &&
        (pdrNode \ DatiTecnPdr \ Raccolta).text.equalsIgnoreCase("P") &&
        (!(pdrNode \ DatiTecnPdr \ Trattamento).text.equalsIgnoreCase("M") &&
          !(pdrNode \ DatiTecnPdr \ Trattamento).text.equalsIgnoreCase("Y"))
        ) ||
        (fileXmlWIthMeta.flusso.equalsIgnoreCase(RML) &&
          (pdrNode \ DatiTecnPdrRett \ tipo_rettifica).text.equalsIgnoreCase("P") &&
          (!(pdrNode \ DatiTecnPdrRett \ Trattamento).text.equalsIgnoreCase("M") &&
            !(pdrNode \ DatiTecnPdrRett \ Trattamento).text.equalsIgnoreCase("Y"))
          )
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(campo Trattamento incoerente rispetto al codice flusso)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleTrattamentoMesecomp: Rule = Rule( //17
    ruleName = "ruleTrattamentoMesecomp",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {

      (flusso1List.contains(fileXmlWIthMeta.flusso.toLowerCase()) &&
        (pdrNode \ DatiTecnPdr \ Trattamento).text.equalsIgnoreCase("G") &&
        !isValued(pdrNode \ mese_comp)) ||
        (flusso2List.contains(fileXmlWIthMeta.flusso.toLowerCase()) &&
          (pdrNode \ DatiTecnPdrRett \ Trattamento).text.equalsIgnoreCase("G") &&
          !isValued(pdrNode \ mese_comp))
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(campo mesecomp non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleCodFlusso1DataRacc: Rule = Rule( //18
    ruleName = "ruleCodFlusso1DataRacc",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso
      val flussiWhiteList = Set(TML, TMV, TAL, TAS, TAV, SWG1, FUI, FDD)

      flusso1List.contains(codFlusso.toLowerCase) &&
        flussiWhiteList.contains(codFlusso.toLowerCase()) &&
        !isValued(pdrNode \ DatiLettura \ data_racc)
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(tag data_racc non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleCodFlusso1DataComp: Rule = Rule( //19
    ruleName = "ruleCodFlusso1DataComp",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {

      val codFlusso = fileXmlWIthMeta.flusso

      flusso1List.contains(codFlusso.toLowerCase()) &&
        codFlusso.equalsIgnoreCase(TGL) &&
        !(pdrNode \\ LettureGiornaliere).forall(node => isValued(node \ data_comp))
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(tag data_comp non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleCodFlusso2DataRaccRGL: Rule = Rule( //20
    ruleName = "ruleCodFlusso2DataRaccRGL",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso

      flusso2List.contains(codFlusso.toLowerCase()) &&
        codFlusso.equalsIgnoreCase(RGL) &&
        !(pdrNode \\ LettureGiornaliereRett).forall(node => isValued(node \ data_racc))
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(tag data_racc non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleCodFlusso2DataRaccRML: Rule = Rule( //21
    ruleName = "ruleCodFlusso2DataRaccRML",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso

      flusso2List.contains(codFlusso.toLowerCase()) &&
        codFlusso.equalsIgnoreCase(RML) &&
        !isValued(pdrNode \ DatiLetturaRett \ data_racc)
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(tag data_racc non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleRaccoltaCodPratSII: Rule = Rule( //22
    ruleName = "ruleRaccoltaCodPratSII",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {

      val raccolta = (pdrNode \ DatiTecnPdr \ Raccolta).text

      flusso1List.contains(fileXmlWIthMeta.flusso.toLowerCase()) &&
        (raccolta.equalsIgnoreCase("S") ||
          raccolta.equalsIgnoreCase("V")) &&
        !isValued(pdrNode \ CodPrat_SII)

    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(tag CodPrat_SII non coerente)",
      bloccante = NON_BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleRaccoltaDataPrest: Rule = Rule( //23
    ruleName = "ruleRaccoltaDataPrest",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {

      val raccolta = (pdrNode \ DatiTecnPdr \ Raccolta).text

      flusso1List.contains(fileXmlWIthMeta.flusso.toLowerCase()) &&
        (raccolta.equalsIgnoreCase("S") ||
          raccolta.equalsIgnoreCase("V") ||
          raccolta.equalsIgnoreCase("T")) &&
        !isValued(pdrNode \ data_prest)
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(tag DataPrest non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleTipoRettificaDataPrest: Rule = Rule( //24
    ruleName = "ruleTipoRettificaDataPrest",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {

      val tipoRettifica = (pdrNode \ tipo_rettifica).text

      flusso2List.contains(fileXmlWIthMeta.flusso.toLowerCase()) &&
        (tipoRettifica.equalsIgnoreCase("S") ||
          tipoRettifica.equalsIgnoreCase("V") ||
          tipoRettifica.equalsIgnoreCase("T")) &&
        !isValued(pdrNode \ data_prest)

    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(tag DataPrest non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleCodFlussoRaccolta: Rule = Rule( //25
    ruleName = "ruleCodFlussoRaccolta",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso
      val raccolta = (pdrNode \ DatiTecnPdr \ Raccolta).text
      val whiteListS = Set(SWG1, FUI, FDD)
      val whiteListAC = Set(TAL, TAS, TAV)
      val whiteListT = Set(D01, D02, R01, A40, S40, R40, A01, A02, S02, V01, M01, V02, SM1, SM2, AD2, AD3, AD4, AD5)

      flusso1List.contains(codFlusso.toLowerCase()) && (
        (codFlusso.equalsIgnoreCase(TMV) && !raccolta.equalsIgnoreCase("V")) ||
          (whiteListS.contains(codFlusso.toLowerCase()) && !raccolta.equalsIgnoreCase("S")) ||
          (whiteListAC.contains(codFlusso.toLowerCase()) && !raccolta.equalsIgnoreCase("AC")) ||
          (whiteListT.contains(codFlusso.toLowerCase()) && !raccolta.equalsIgnoreCase("T"))
        )
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(tag Raccolta non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )


  val ruleCodFlussoTipoRettifica: Rule = Rule( //26
    ruleName = "ruleCodFlussoTipoRettifica",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso
      val tipoRettifica = (pdrNode \ tipo_rettifica).text
      val whiteListT = Set(D01R, D02R, R01R, A40R, S40R, R40R, A01R, A02R, S02R, V01R, M01R, V02R, SM1R, SM2R, AD2R, AD3R, AD4R, AD5R)

      flusso2List.contains(codFlusso.toLowerCase()) && (
        (codFlusso.equalsIgnoreCase(RMV) && !tipoRettifica.equalsIgnoreCase("V")) ||
          (codFlusso.equalsIgnoreCase(RSL) && !tipoRettifica.equalsIgnoreCase("S")) ||
          (whiteListT.contains(codFlusso.toLowerCase()) && !tipoRettifica.equalsIgnoreCase("T"))
        )

    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(tag TipoRettifica non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleCodFlussoEsitoRaccolta: Rule = Rule( //27
    ruleName = "ruleCodFlussoEsitoRaccolta",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso

      flusso1List.contains(codFlusso.toLowerCase()) &&
        (codFlusso.equalsIgnoreCase(TML) ||
          codFlusso.equalsIgnoreCase(TGL)) &&
        (pdrNode \ DatiTecnPdr \ Raccolta).text.equalsIgnoreCase("P") &&
        !isValued(pdrNode \ DatiTecnPdr \ esito_raccolta)

    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(tag esitoRaccolta non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleCodFlussoFreqLetFlusso1: Rule = Rule( //28
    ruleName = "ruleCodFlussoFreqLetFlusso1",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso
      val freqLetAllowed = Set("1", "2", "3", "4", "5", "6", "7")

      flusso1List.contains(codFlusso.toLowerCase) &&
        codFlusso.equalsIgnoreCase(TML) &&
        !freqLetAllowed.contains((pdrNode \\ freq_let).text)
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(tag freq_let non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleCodFlussoFreqLetFlusso2: Rule = Rule( //29
    ruleName = "ruleCodFlussoFreqLetFlusso2",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso
      val freqLetAllowed = Set("1", "2", "3", "4", "5", "6", "7")

      flusso2List.contains(codFlusso.toLowerCase) &&
        codFlusso.equalsIgnoreCase(RML) &&
        !freqLetAllowed.contains((pdrNode \\ freq_let).text)
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(tag freq_let non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleMotRetLetLetTotPrel: Rule = Rule( //30
    ruleName = "ruleMotRetLetLetTotPrel",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso
      val flussoWhiteList = Set(RML, RMV, RSL, D01R, D02R, R01R, A40R, S40R, R40R, A01R, A02R, S02R, V01R, M01R, V02R, SM1R, SM2R, AD2R, AD3R, AD4R, AD5R)
      val morRetLettWhiteList = Set("1", "2", "4", "5")

      flusso2List.contains(codFlusso.toLowerCase) &&
        flussoWhiteList.contains(codFlusso.toLowerCase) &&
        morRetLettWhiteList.contains((pdrNode \ mot_ret_lett).text) &&
        !isValued(pdrNode \ DatiLetturaRett \ let_tot_prel)
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(let_tot_prel non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleMotRetLetLetTotPrelRGL: Rule = Rule( //31
    ruleName = "ruleMotRetLetLetTotPrelRGL",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso

      flusso2List.contains(codFlusso.toLowerCase) &&
        codFlusso.equalsIgnoreCase(RGL) &&
        Set("1", "2", "4", "5").contains((pdrNode \ mot_ret_lett).text) &&
        !isValued(pdrNode \ LettureGiornaliereRett \ let_tot_prel)
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(let_tot_prel non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val rulePreConvGruppoMisInt: Rule = Rule( //32
    ruleName = "rulePreConvGruppoMisInt",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso
      val codFlussoWhiteList = Set(TMV, SWG1, FUI, FDD, A01, A02, A40, S02, S40, AD2, AD3, AD4, AD5)
      val preConv = (pdrNode \\ pre_conv).text

      flusso1List.contains(fileXmlWIthMeta.flusso.toLowerCase) &&
        preConv != null && preConv.equalsIgnoreCase("NO") &&
        codFlussoWhiteList.contains(codFlusso.toLowerCase()) &&
        !isValued(pdrNode \\ gruppo_mis_int)
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(gruppo_mis_int non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val rulePreConvLetTotConv: Rule = Rule( //33
    ruleName = "rulePreConvLetTotConv",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso
      val codFlussoWhiteList = Set(TML, TAL, TAS, TAV, TMV, SWG1, FUI, FDD, D01, R01, A40, S40, R40, A01, A02, S02, V01, M01, V02, SM1, SM2, AD2, AD3, AD4, AD5)

      flusso1List.contains(codFlusso.toLowerCase) &&
        (pdrNode \\ pre_conv).text.equalsIgnoreCase("SI") &&
        codFlussoWhiteList.contains(codFlusso.toLowerCase()) &&
        !isValued(pdrNode \ DatiLettura \ let_tot_conv)
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(let_tot_conv non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val rulePresConvLetTotConvTGL: Rule = Rule( //34
    ruleName = "rulePresConvLetTotConvTGL",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso

      flusso1List.contains(fileXmlWIthMeta.flusso.toLowerCase) &&
        (pdrNode \\ pre_conv).text.equalsIgnoreCase("SI") &&
        codFlusso.equalsIgnoreCase(TGL) &&
        !isValued(pdrNode \ LettureGiornaliere \ let_tot_conv)
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(let_tot_conv non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleCodFlussoVolAnnuoRettificato: Rule = Rule( //35
    ruleName = "ruleCodFlussoVolAnnuoRettificato",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso

      flusso2List.contains(codFlusso.toLowerCase) &&
        (codFlusso.equalsIgnoreCase(RMV) || codFlusso.equalsIgnoreCase(RSL)) &&
        !isValued(pdrNode \ DatiTecnPdrRett \ vol_annuo_rettificato)

    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(Vol_annuo_rettificato non coerente)",
      bloccante = NON_BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleCodFlussoMotRetLetRML: Rule = Rule( //36
    ruleName = "ruleCodFlussoMotRetLetRML",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso
      val motRetLet = (pdrNode \ mot_ret_lett).text

      flusso2List.contains(codFlusso.toLowerCase) &&
        codFlusso.equalsIgnoreCase(RML) &&
        (motRetLet.equalsIgnoreCase("3") || motRetLet.equalsIgnoreCase("6")) &&
        !isValued(pdrNode \ DatiLetturaRett \ data_racc)

    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(sezione DatiLetturaRett valorizzata non coerente con la motivazione rettificata)",
      bloccante = NON_BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleCodFlussoMotRetLetRGL: Rule = Rule( //37
    ruleName = "ruleCodFlussoMotRetLetRGL",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso
      val motRetLet = (pdrNode \ mot_ret_lett).text

      flusso2List.contains(codFlusso.toLowerCase) &&
        codFlusso.equalsIgnoreCase(RGL) &&
        (motRetLet.equalsIgnoreCase("3") || motRetLet.equalsIgnoreCase("6")) &&
        !isValued(pdrNode \ LettureGiornaliereRett \ data_racc)
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(sezione LettureGiornaliereRett valorizzata non coerente con la motivazione rettificata)",
      bloccante = NON_BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleRMLMotRetLet: Rule = Rule( //38
    ruleName = "rule1MotRetLet",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val motRetLet = (pdrNode \ mot_ret_lett).text

      fileXmlWIthMeta.flusso.toLowerCase.equals(RML) &&
        (motRetLet.equalsIgnoreCase("4") || motRetLet.equalsIgnoreCase("5")) &&
        (!isValued(pdrNode \ DatiLetturaRett \ vol_ric) ||
          !isValued(pdrNode \ DatiLetturaRett \ ini_periodo) ||
          !isValued(pdrNode \ DatiLetturaRett \ fine_periodo))

    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(campi relativi alla ricostruzione dei consumi non coerenti)",
      bloccante = NON_BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleRGLMotRetLet: Rule = Rule( //39
    ruleName = "rule2MotRetLet",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val motRetLet = (pdrNode \ mot_ret_lett).text

      fileXmlWIthMeta.flusso.toLowerCase.equals(RGL) &&
        (motRetLet.equalsIgnoreCase("4") || motRetLet.equalsIgnoreCase("5")) &&
        (!isValued(pdrNode \ LettureGiornaliereRett \ vol_ric) || !isValued(pdrNode \ LettureGiornaliereRett \ periodo_ric))


    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(campi relativi alla ricostruzione dei consumi non coerenti)",
      bloccante = NON_BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleClasseGruppoMisFlusso1: Rule = Rule( //40
    ruleName = "ruleClasseGruppoMisFlusso1",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {

      flusso1List.contains(fileXmlWIthMeta.flusso.toLowerCase) &&
        isValued(pdrNode \\ classe_gruppo_mis) &&
        (!classeGruppoMisLegalValues.contains((pdrNode \\ classe_gruppo_mis).text.toLowerCase))
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(codifica classe_gruppo_mis non coerente)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleFlusso2MotRetLet: Rule = Rule( //41
    ruleName = "ruleFlusso2MotRetLet",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {

      flusso2List.contains(fileXmlWIthMeta.flusso.toLowerCase) &&
        (pdrNode \\ mot_ret_lett).text.equals("7") &&
        !fileXmlWIthMeta.flusso.toLowerCase.equals(RGL)

    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "(codifica mot_rett_lett non coerente con cod_flusso)",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleCoeffCorr : Rule = Rule( // 49
    ruleName = "ruleCoeffCorr",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val XMinimo = if (params.isDefined) params.get.parameters("XMinimo").toDouble else .0
      val XMassimo = if (params.isDefined) params.get.parameters("XMassimo").toDouble else COEFF_CORR_MAX_VALUE

      ((flusso1List.contains(fileXmlWIthMeta.flusso.toLowerCase)  &&
        (!isValued(pdrNode \ DatiTecnPdr \ coeff_corr) ||
          Try((pdrNode \ DatiTecnPdr \ coeff_corr).text.toDouble).getOrElse(-1D) < XMinimo ||
            Try((pdrNode \ DatiTecnPdr \ coeff_corr).text.toDouble).getOrElse(-1D) > XMassimo)) ||

      (flusso2List.contains(fileXmlWIthMeta.flusso.toLowerCase)  &&
        (!isValued(pdrNode \ DatiTecnPdrRett \ coeff_corr) ||
            Try((pdrNode \ DatiTecnPdrRett \ coeff_corr).text.toDouble).getOrElse(-1D) < XMinimo ||
              Try((pdrNode \ DatiTecnPdrRett \ coeff_corr).text.toDouble).getOrElse(-1D) > XMassimo)))
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD011,
      descrizione = ERROR_COEFF_CORR,
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val ruleIGMGIGMRMatchRGLRML: Rule = Rule( //50
    ruleName = "ruleIGMGIGMRMatchRGLRML",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, _) => {
      val codFlusso = fileXmlWIthMeta.flusso

      flusso2List.contains(codFlusso.toLowerCase) &&
        (codFlusso.toLowerCase.equalsIgnoreCase(RGL) || codFlusso.toLowerCase.equalsIgnoreCase(RML)) &&
        ((pdrNode \\ mot_ret_lett).text.equals("4") || (pdrNode \\ mot_ret_lett).text.equals("5")) &&
        (fileXmlWIthMeta.asInstanceOf[GasXmlMetadata].igmgMatch.equalsIgnoreCase("RELATIVO_IGMG_PRESENTE")
          || fileXmlWIthMeta.asInstanceOf[GasXmlMetadata].igmrMatch.equalsIgnoreCase("RELATIVO_IGMR_PRESENTE"))
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = IGMG_IGMR_MATCH_ERROR_RGL_RML,
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  printRules()
}
