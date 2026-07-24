package it.au.misure.ingestionMisureGasUnico.validate

import it.au.misure.ingestionMisureGasUnico.model.schema.IGMRXMLSchema
import it.au.misure.ingestionMisureGasUnico.model.{GasMetadata, GasXmlMetadata}
import it.au.misure.ingestionMisureGasUnico.model.schema.IGMRXMLSchema._
import it.au.misure.ingestionMisureGasUnico.model.validate.{ReportEsitoPDRMessage, ReportMessage, Rule, RuleParameters}
import it.au.misure.ingestionMisureGasUnico.utility.Constants.{BLOCCANTE, COD011, COD10, COD904, COEFF_CORR_MAX_VALUE, ERROR_COEFF_CORR, ERROR_FILE_STRUCTURE, MANDATORY_FIELDS_ERROR, NON_BLOCCANTE, PDR, classeGruppoMisLegalValues}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Try
import scala.xml.NodeSeq

class CheckAmmissibilitaPDRRulesIGMR extends CheckAmmissibilitaRules[ReportEsitoPDRMessage]{
  override def ammissibilitaType: String = "pdr"

  override def okMessage: ReportMessage = ReportEsitoPDRMessage()

  override def addFileInfoToMessage(message: ReportMessage, xml: NodeSeq, gasXmlMetadata: GasMetadata): ReportEsitoPDRMessage = {
    message.asInstanceOf[ReportEsitoPDRMessage].copy(cartellaCloud = gasXmlMetadata.file.getParent, nomeFile = gasXmlMetadata.file.getName, pdr = (xml \\ IGMRXMLSchema.cod_PdR).text)
  }

  override def rules: List[Rule] = List(
    ruleRCUGAS1IGMR // 18
    , ruleRCUGAS2IGMR // 19
    , ruleIgmgIgmrMatch //20
    , rulePreIntPostIntMotRetLett2 // 21
    , rulePreIntPostIntMotRetLett3 // 22
    , ruleCauIntMisPreIntMatrMis // 23
    , rulePreIntPreConvNoGruppoMisInt // 24
    , rulePreIntPreConvGruppoMisIntNo // 25
    , rulePreIntPreConvSiLetCorr // 26
    , rulePreIntCoeffCorrIGMR // 27
    , rulePostIntCoeffCorrIGMR // 28
    , rulePreIntPreConvSiCauIntCorr // 29
    , rulePostInitCauIntMisMatrMisIGMR // 30
    , rulePostInitCauIntMisMatrMis2IGMR // 31
    , rulePostInitCauIntMisTipoMisIGMR // 32
    , rulePostInitTipoMisCauInitMisTelegestioneIGMR // 33
    , rulePostInitPreConvGruppoMisIGMR // 34
    , rulePostInitPreConvGruppoMisIntIGMR // 35
    , rulePostInitPreConvLetCorrettoreIGMR // 36
    , rulePostInitPreConvMatrConvIGMR // 37
    , rulePostInitCauIntMisClasseGruppoMisIGMR // 38
    , rulePostInitClasseGruppoMisIGMR // 39
    , rulePostInitCauIntMisClasseGruppoMisCodeIGMR // 40
    , ruleCauIntMisPostIntAccMis // 41
    , ruleCoeffCorrIGMR // 42
    , rulePreConvCauIntCorMotRettIGMR // 43
  )

  val ruleRCUGAS1IGMR: Rule = Rule( //ID: 18
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {
      !fileXmlWithMeta.asInstanceOf[GasXmlMetadata].pdrRcuExist
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + " PdR inesistente nel RCUGAS",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "ruleRCUGAS1IGMR"
  )

  val ruleRCUGAS2IGMR: Rule = Rule( // ID: 19
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {
      val dataMisuraPdr = Try(LocalDate.parse((pdrNode \ data_misura).text, DateTimeFormatter.ofPattern(ITALIAN_DATE_PATTERN)))
      val pdrStartDate = fileXmlWithMeta.asInstanceOf[GasXmlMetadata].pdrValidFrom
      val pdrEndDate = fileXmlWithMeta.asInstanceOf[GasXmlMetadata].pdrValidTo

      dataMisuraPdr.isFailure ||
        (dataMisuraPdr.get.isBefore(pdrStartDate.toLocalDate) || dataMisuraPdr.get.isAfter(pdrEndDate.toLocalDate))
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + " PdR non attivo nel RCUGAS",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "ruleRCUGAS2IGMR"
  )

  val ruleIgmgIgmrMatch: Rule = Rule( //ID: 20
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = !fileXmlWithMeta.asInstanceOf[GasXmlMetadata].igmgMatch.equalsIgnoreCase("RELATIVO_IGMG_PRESENTE")

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD904,
      descrizione = MANDATORY_FIELDS_ERROR + " Non risulta presente a sistema un precedente IGMG",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "ruleIgmgIgmrMatch"
  )

  val rulePreIntPostIntMotRetLett2: Rule = Rule( //ID: 21
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        (!isValued(pdrNode \ Pre_int) || !isValued(pdrNode \ Post_int))

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " I dati all’interno delle sezioni Pre-Int e Post-Int non sono correttamente compilati",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePreIntPostIntMotRetLett2"
  )

  val rulePreIntPostIntMotRetLett3: Rule = Rule( //ID: 22
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("3") &&
        (isValued(pdrNode \ Pre_int) || isValued(pdrNode \ Post_int))

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " I dati all’interno delle sezioni Pre-Int e Post-Int non devono essere compilati",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePreIntPostIntMotRetLett3"
  )

  val ruleCauIntMisPreIntMatrMis: Rule = Rule( //ID: 23
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        isValued(pdrNode \ cau_int_mis) &&
        !isValued(pdrNode \ Pre_int \ matr_mis)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " matr_mis pre intervento non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "ruleCauIntMisPreIntMatrMis"
  )

  val rulePreIntPreConvNoGruppoMisInt: Rule = Rule( //ID: 24
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        (pdrNode \ Pre_int \ pre_conv).text.equalsIgnoreCase("NO") &&
        !isValued(pdrNode \ Pre_int \ gruppo_mis_int)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " pre_conv e gruppo_mis_int non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePreIntPreConvNoGruppoMisInt"
  )

  val rulePreIntPreConvGruppoMisIntNo: Rule = Rule( //ID: 25
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        !isValued(pdrNode \ Pre_int \ pre_conv) &&
        (pdrNode \ Pre_int \ gruppo_mis_int).text.equalsIgnoreCase("NO")

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " pre_conv e gruppo_mis_int non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePreIntPreConvGruppoMisIntNo"
  )

  val rulePreIntPreConvSiLetCorr: Rule = Rule( //ID: 26
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        (pdrNode \ Pre_int \ pre_conv).text.equalsIgnoreCase("SI") &&
        !isValued(pdrNode \ Pre_int \ let_correttore)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " let_correttore pre intervento non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePreIntPreConvGruppoMisIntNo"
  )

  val rulePreIntCoeffCorrIGMR: Rule = Rule( //ID: 27
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val X = if (params.isDefined) params.get.parameters("X").toDouble else .0

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        (pdrNode \ Pre_int \ coeff_corr).text.toDouble > X

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " coeff_corr pre intervento non coerente",
      bloccante = NON_BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePreIntCoeffCorrIGMR"
  )

  val rulePostIntCoeffCorrIGMR: Rule = Rule( //ID: 28
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val X = if (params.isDefined) params.get.parameters("X").toDouble else .0

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        (pdrNode \ Post_int \ coeff_corr).text.toDouble > X

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " coeff_corr post intervento non coerente",
      bloccante = NON_BLOCCANTE,
      ammissibilita = PDR
    )
    , ruleName = "rulePostIntCoeffCorrIGMR"
  )

  val rulePreIntPreConvSiCauIntCorr: Rule = Rule( //ID: 29
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        (pdrNode \ Pre_int \ pre_conv).text.equalsIgnoreCase("SI") &&
        isValued(pdrNode \ Pre_int \ cau_int_cor) &&
        !isValued(pdrNode \ Pre_int \ matr_conv)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " Matr_conv post intervento non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePreIntPreConvSiCauIntCorr"
  )

  val rulePostInitCauIntMisMatrMisIGMR: Rule = Rule( //ID 30
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        !isValued(pdrNode \ cau_int_mis) &&
        isValued(pdrNode \ Post_int \ matr_mis)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " Matr_mis post intervento non coerente",
      bloccante = NON_BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePostInitCauIntMisMatrMisIGMR"
  )

  val rulePostInitCauIntMisMatrMis2IGMR: Rule = Rule( //ID 31
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        isValued(pdrNode \ cau_int_mis) &&
        !isValued(pdrNode \ Post_int \ matr_mis)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " Matr_mis post intervento non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePostInitCauIntMisMatrMis2IGMR"
  )

  val rulePostInitCauIntMisTipoMisIGMR: Rule = Rule( //ID 32
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        !isValued(pdrNode \ cau_int_mis) &&
        isValued(pdrNode \ Post_int \ tipo_mis)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " tipo_mis post intervento non coerente",
      bloccante = NON_BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePostInitCauIntMisTipoMisIGMR"
  )

  val rulePostInitTipoMisCauInitMisTelegestioneIGMR: Rule = Rule( //ID 33
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        (pdrNode \ Post_int \ tipo_mis).text.equalsIgnoreCase("02") &&
        isValued(pdrNode \ cau_int_mis) &&
        !isValued(pdrNode \ Post_int \ telegestione)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " telegestione non coerente",
      bloccante = NON_BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePostInitTipoMisCauInitMisTelegestioneIGMR"
  )

  val rulePostInitPreConvGruppoMisIGMR: Rule = Rule( //ID 34
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        !isValued(pdrNode \ Post_int \ pre_conv) &&
        (pdrNode \ Post_int \ gruppo_mis_int).text.equalsIgnoreCase("NO")

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " pre_conv e gruppo_mis_int non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePostInitPreConvGruppoMisIGMR"
  )

  val rulePostInitPreConvGruppoMisIntIGMR: Rule = Rule( //ID 35
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        (pdrNode \ Post_int \ pre_conv).text.equalsIgnoreCase("NO") &&
        !isValued(pdrNode \ Post_int \ gruppo_mis_int)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " pre_conv e gruppo_mis_int non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePostInitPreConvGruppoMisIntIGMR"
  )


  val rulePostInitPreConvLetCorrettoreIGMR: Rule = Rule( //ID 36
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        (pdrNode \ Post_int \ pre_conv).text.equalsIgnoreCase("SI") &&
        !isValued(pdrNode \ Post_int \ let_correttore)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " let_correttore post intervento non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePostInitPreConvLetCorrettoreIGMR"
  )

  val rulePostInitPreConvMatrConvIGMR: Rule = Rule( //ID 37
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        (pdrNode \ Post_int \ pre_conv).text.equalsIgnoreCase("SI") &&
        !isValued(pdrNode \ Post_int \ matr_conv)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " Matr_conv post intervento non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePostInitPreConvMatrConvIGMR"
  )

  val rulePostInitCauIntMisClasseGruppoMisIGMR: Rule = Rule( //ID 38
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        !isValued(pdrNode \ cau_int_mis) &&
        isValued(pdrNode \ Post_int \ classe_gruppo_mis)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " classe_gruppo_mis non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePostInitCauIntMisClasseGruppoMisIGMR"
  )

  val rulePostInitClasseGruppoMisIGMR: Rule = Rule( //ID 39
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        isValued(pdrNode \ Post_int \ classe_gruppo_mis) &&
        (!classeGruppoMisLegalValues.contains((pdrNode \ Post_int \ classe_gruppo_mis).text.toLowerCase))

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " codifica classe_gruppo_mis non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePostInitClasseGruppoMisIGMR"
  )

  val rulePostInitCauIntMisClasseGruppoMisCodeIGMR: Rule = Rule( //ID 40
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        isValued(pdrNode \ cau_int_mis) &&
        !isValuedText(pdrNode \ Post_int \ classe_gruppo_mis)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "codifica classe_gruppo_mis non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePostInitCauIntMisClasseGruppoMisCodeIGMR"
  )

  val ruleCauIntMisPostIntAccMis: Rule = Rule( //ID 41
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = (pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&
        !isValued(pdrNode \ cau_int_mis) &&
        isValued(pdrNode \ Post_int \ acc_mis)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " acc_mis post intervento non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePostInitCauIntMisAccMis"
  )

  val ruleCoeffCorrIGMR: Rule = Rule( //ID 42  - DA APPLICARE SIA A PRE-INT CHE A POST-INT
    ruleName = "ruleCoeffCorrIGMR",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, params: Option[RuleParameters]) => {

      val XMinimo = if (params.isDefined) params.get.parameters("XMinimo").toDouble else .0
      val XMassimo = if (params.isDefined) params.get.parameters("XMassimo").toDouble else COEFF_CORR_MAX_VALUE

      ((pdrNode \ mot_ret_lett).text.equalsIgnoreCase("2") &&

        ((!isValued(pdrNode \ Pre_int \ coeff_corr)) ||

        (!isValued(pdrNode \ Post_int \ coeff_corr)) ||

        (isValued(pdrNode \ Pre_int \ coeff_corr) &&
          (Try((pdrNode \ Pre_int \ coeff_corr).text.toDouble).getOrElse(-1D) < XMinimo ||
            Try((pdrNode \ Pre_int \ coeff_corr).text.toDouble).getOrElse(-1D) > XMassimo)) ||

        (isValued(pdrNode \ Post_int \ coeff_corr) &&
          (Try((pdrNode \ Post_int \ coeff_corr).text.toDouble).getOrElse(-1D) < XMinimo ||
            Try((pdrNode \ Post_int \ coeff_corr).text.toDouble).getOrElse(-1D) > XMassimo))))
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD011,
      descrizione = ERROR_COEFF_CORR,
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val rulePreConvCauIntCorMotRettIGMR: Rule = Rule( //ID 43
    ruleName = "rulePreConvCauIntCorMotRettIGMR",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, params: Option[RuleParameters]) => {

      (
          (pdrNode \ Pre_int \ pre_conv).text.equalsIgnoreCase("NO") &&
            isValued(pdrNode \ cau_int_cor) &&
            (!(pdrNode \ mot_ret_lett).text.equalsIgnoreCase("3"))
        )||(
          (pdrNode \ Post_int \ pre_conv).text.equalsIgnoreCase("NO") &&
            isValued(pdrNode \ cau_int_cor) &&
            (!(pdrNode \ mot_ret_lett).text.equalsIgnoreCase("3"))
      )

    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " impossibile eseguire aggiornamento sul convertitore",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  printRules()

}
