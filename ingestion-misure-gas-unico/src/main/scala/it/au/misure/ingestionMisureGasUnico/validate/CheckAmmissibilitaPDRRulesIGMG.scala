package it.au.misure.ingestionMisureGasUnico.validate

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import it.au.misure.ingestionMisureGasUnico.model.{GasMetadata, GasXmlMetadata}
import it.au.misure.ingestionMisureGasUnico.model.schema.IGMGXMLSchema
import it.au.misure.ingestionMisureGasUnico.model.schema.IGMGXMLSchema._
import it.au.misure.ingestionMisureGasUnico.model.validate.{ReportEsitoPDRMessage, ReportMessage, Rule, RuleParameters}
import it.au.misure.ingestionMisureGasUnico.utility.Constants._

import scala.util.Try
import scala.xml.NodeSeq

class CheckAmmissibilitaPDRRulesIGMG extends CheckAmmissibilitaRules[ReportEsitoPDRMessage]{
  override def ammissibilitaType: String = "pdr"
  override def okMessage: ReportMessage = ReportEsitoPDRMessage()

  override def addFileInfoToMessage(message: ReportMessage, xml: NodeSeq, gasXmlMetadata: GasMetadata): ReportEsitoPDRMessage = {
    message.asInstanceOf[ReportEsitoPDRMessage].copy(cartellaCloud = gasXmlMetadata.file.getParent, nomeFile = gasXmlMetadata.file.getName, pdr = (xml \\ IGMGXMLSchema.cod_PdR).text)
  }

  override def rules: List[Rule] = List(
    ruleRCUGAS1 // 9
    , ruleRCUGAS2 // 10
    , ruleCauIntMisMatrMis // 11
    , rulePreIntPreConvGruppoMisInt // 12
    , rulePreIntPreConvGruppoMisInt2 // 13
    , rulePreIntPreConvLetCorrettore // 14
    , rulePreIntCoeffCorr // 15
    , rulePostIntCoeffCorr // 16
    , rulePreIntPreConvCauIntMatrConv // 17
    , rulePostInitCauIntMisMatrMis // 18
    , rulePostInitCauIntMisMatrMis2 // 19
    , rulePostInitCauIntMisTipoMis // 20
    , rulePostInitTipoMisCauInitMisTelegestione // 21
    , rulePostInitPreConvGruppoMis // 22
    , rulePostInitPreConvGruppoMisInt // 23
    , rulePostInitPreConvLetCorrettore // 24
    , rulePostInitPreConvMatrConv // 25
    , rulePostInitCauIntMisClasseGruppoMis // 26
    , rulePostInitClasseGruppoMis // 27
    , rulePostInitCauIntMisAccMis // 28
    , ruleCoeffCorrIGMG // 29
    , rulePostInitCauIntMisClasseGruppoMisCode // 31
    , rulePreConvCauIntCorIGMG //32
  )

  val ruleRCUGAS1:Rule = Rule( //ID: 9
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {
      !fileXmlWithMeta.asInstanceOf[GasXmlMetadata].pdrRcuExist
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + " PdR inesistente nel RCUGAS" ,
      bloccante = BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "ruleRCUGAS1"
  )

  val ruleRCUGAS2:Rule = Rule( // ID: 10
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {
      val dataMisuraPdr = Try(LocalDate.parse( (pdrNode\data_misura).text, DateTimeFormatter.ofPattern(ITALIAN_DATE_PATTERN) ) )
      val pdrStartDate = fileXmlWithMeta.asInstanceOf[GasXmlMetadata].pdrValidFrom
      val pdrEndDate = fileXmlWithMeta.asInstanceOf[GasXmlMetadata].pdrValidTo

      (dataMisuraPdr.isFailure) ||
        (dataMisuraPdr.get.isBefore(pdrStartDate.toLocalDate) || dataMisuraPdr.get.isAfter(pdrEndDate.toLocalDate) )
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD904,
      descrizione = ERROR_FILE_STRUCTURE + " PdR non attivo nel RCUGAS" ,
      bloccante = BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "ruleRCUGAS2"
  )

  val ruleCauIntMisMatrMis:Rule = Rule( //ID: 11
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

       isValued(pdrNode \ cau_int_mis) && !isValued(pdrNode \ Pre_int \ matr_mis)
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " matr_mis pre intervento non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "ruleCauIntMisMatrMis"
  )

  val rulePreIntPreConvGruppoMisInt:Rule = Rule( //ID: 12
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val errorCondition = (pdrNode \ Pre_int \ pre_conv).text.equalsIgnoreCase("NO") &&
        !isValued(pdrNode \ Pre_int \ gruppo_mis_int)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " pre_conv e gruppo_mis_int non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePreIntPreConvGruppoMisInt"
  )

  val rulePreIntPreConvGruppoMisInt2:Rule = Rule( //ID: 13
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val errorCondition = !isValued(pdrNode \ Pre_int \ pre_conv) &&
        (pdrNode \ Pre_int \ gruppo_mis_int).text.equalsIgnoreCase("NO")

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " pre_conv e gruppo_mis_int non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePreIntPreConvGruppoMisInt2"
  )

  val rulePreIntPreConvLetCorrettore:Rule = Rule( //ID: 14
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val errorCondition = (pdrNode \ Pre_int \ pre_conv).text.equalsIgnoreCase("SI") &&
        !isValued(pdrNode \ Pre_int \ let_correttore)

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " let_correttore pre intervento non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePreIntPreConvLetCorrettore"
  )

  val rulePreIntCoeffCorr:Rule = Rule( //ID: 15
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val X = if (params.isDefined) params.get.parameters("X").toDouble else .0

      val errorCondition = (pdrNode \ Pre_int \ coeff_corr).text.toDouble > X

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " coeff_corr pre intervento non coerente" ,
      bloccante = NON_BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePreIntCoeffCorr"
  )

  val rulePostIntCoeffCorr:Rule = Rule( //ID: 16
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val X = if (params.isDefined) params.get.parameters("X").toDouble else .0

      val errorCondition = (pdrNode \ Post_int \ coeff_corr).text.toDouble > X

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " coeff_corr pre intervento non coerente" ,
      bloccante = NON_BLOCCANTE,
      ammissibilita= PDR
    )
    , ruleName = "rulePostIntCoeffCorr"
  )


  val rulePreIntPreConvCauIntMatrConv:Rule = Rule( //ID:17
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val errorCondition = (pdrNode \ Pre_int \ pre_conv).text.equalsIgnoreCase("SI") &&
        isValued(pdrNode \ cau_int_cor) &&
        !isValued(pdrNode \ Pre_int \ matr_conv)

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " Matr_conv post intervento non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePreIntPreConvCauIntMatrConv"
  )

  val rulePostInitCauIntMisMatrMis:Rule = Rule( //ID:18
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val errorCondition = !isValued(pdrNode \ cau_int_mis) && isValued(pdrNode \ Post_int \ matr_mis)

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " Matr_mis post intervento non coerente" ,
      bloccante = NON_BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePostInitCauIntMisMatrMis"
  )

  val rulePostInitCauIntMisMatrMis2:Rule = Rule( //ID 19
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val errorCondition = isValued(pdrNode \ cau_int_mis) && !isValued(pdrNode \ Post_int \ matr_mis)

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " Matr_mis post intervento non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePostInitCauIntMisMatrMis2"
  )

  val rulePostInitCauIntMisTipoMis:Rule = Rule( //ID 20
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val errorCondition = !isValued(pdrNode \ cau_int_mis) && isValued(pdrNode \ Post_int \ tipo_mis)

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " tipo_mis post intervento non coerente" ,
      bloccante = NON_BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePostInitCauIntMisTipoMis"
  )

  val rulePostInitTipoMisCauInitMisTelegestione:Rule = Rule( //ID 21
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val errorCondition = (pdrNode \ Post_int \ tipo_mis).text.equalsIgnoreCase("02") &&
        isValued(pdrNode \ cau_int_mis) &&
        !isValued(pdrNode \ Post_int \ telegestione )

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " telegestione non coerente" ,
      bloccante = NON_BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePostInitTipoMisCauInitMisTelegestione"
  )

  val rulePostInitPreConvGruppoMis:Rule = Rule( //ID 22
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val errorCondition = !isValued(pdrNode \ Post_int \ pre_conv) &&
        (pdrNode \ Post_int \ gruppo_mis_int).text.equalsIgnoreCase("NO")

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " pre_conv e gruppo_mis_int non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePostInitPreConvGruppoMis"
  )

  val rulePostInitPreConvGruppoMisInt:Rule = Rule( //ID 23
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val errorCondition = (pdrNode \ Post_int \ pre_conv).text.equalsIgnoreCase("NO") &&
        !isValued(pdrNode \ Post_int \ gruppo_mis_int)

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " pre_conv e gruppo_mis_int non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePostInitPreConvGruppoMisInt"
  )


  val rulePostInitPreConvLetCorrettore:Rule = Rule( //ID 24
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val errorCondition = (pdrNode \ Post_int \ pre_conv).text.equalsIgnoreCase("SI") &&
        !isValued(pdrNode \ Post_int \ let_correttore)

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " let_correttore post intervento non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePostInitPreConvLetCorrettore"
  )

  val rulePostInitPreConvMatrConv:Rule = Rule( //ID 25
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val errorCondition = (pdrNode \ Post_int \ pre_conv).text.equalsIgnoreCase("SI") &&
        !isValued(pdrNode \ Post_int \ matr_conv)

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " Matr_conv post intervento non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePostInitPreConvMatrConv"
  )

  val rulePostInitCauIntMisClasseGruppoMis:Rule = Rule( //ID 26
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val errorCondition = !isValued(pdrNode \ cau_int_mis ) &&
        isValued(pdrNode \ Post_int \ classe_gruppo_mis)

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " classe_gruppo_mis non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePostInitCauIntMisClasseGruppoMis"
  )

  val rulePostInitClasseGruppoMis:Rule = Rule( //ID 27
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val errorCondition =isValued((pdrNode \ Post_int \ classe_gruppo_mis )) && ( !classeGruppoMisLegalValues.contains(
        (pdrNode \ Post_int \ classe_gruppo_mis ).text.toLowerCase ))

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " codifica classe_gruppo_mis non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePostInitClasseGruppoMis"
  )

  val rulePostInitCauIntMisAccMis:Rule = Rule( //ID 28
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val errorCondition = !isValued(pdrNode \ cau_int_mis) &&
        isValued(pdrNode \ Post_int \ acc_mis)

       errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + " acc_mis post intervento non coerente" ,
      bloccante = BLOCCANTE,
      ammissibilita= PDR
    ),
    ruleName = "rulePostInitCauIntMisAccMis"
  )

  val ruleCoeffCorrIGMG : Rule = Rule( // 29 - DA APPLICARE SIA A PRE-INT CHE A POST-INT
    ruleName = "ruleCoeffCorrIGMG",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, params: Option[RuleParameters] ) => {

      val XMinimo = if (params.isDefined) params.get.parameters("XMinimo").toDouble else .0
      val XMassimo = if (params.isDefined) params.get.parameters("XMassimo").toDouble else COEFF_CORR_MAX_VALUE

      ((!isValued(pdrNode \ Pre_int \ coeff_corr)) ||

          (!isValued(pdrNode \ Post_int \ coeff_corr)) ||

            (isValued(pdrNode \ Pre_int \ coeff_corr) &&
            (Try((pdrNode \ Pre_int \ coeff_corr).text.toDouble).getOrElse(-1D) < XMinimo ||
              Try((pdrNode \ Pre_int \ coeff_corr).text.toDouble).getOrElse(-1D) > XMassimo)) ||

                  (isValued(pdrNode \ Post_int \ coeff_corr)  &&
                    (Try((pdrNode \ Post_int \ coeff_corr).text.toDouble).getOrElse(-1D) < XMinimo ||
                      Try((pdrNode \ Post_int \ coeff_corr).text.toDouble).getOrElse(-1D) > XMassimo)))
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD011,
      descrizione = ERROR_COEFF_CORR,
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    )
  )

  val rulePostInitCauIntMisClasseGruppoMisCode: Rule = Rule( //ID 31
    condition = (pdrNode: NodeSeq, fileXmlWithMeta: GasMetadata, params: Option[RuleParameters]) => {

      val errorCondition = isValued(pdrNode \ cau_int_mis) &&
        !isValuedText(pdrNode \ Post_int \ classe_gruppo_mis)

      errorCondition
    },
    message = ReportEsitoPDRMessage(
      codiceInamissibilita = COD10,
      descrizione = MANDATORY_FIELDS_ERROR + "codifica classe_gruppo_mis non coerente",
      bloccante = BLOCCANTE,
      ammissibilita = PDR
    ),
    ruleName = "rulePostInitCauIntMisClasseGruppoMisCode"
  )

  val rulePreConvCauIntCorIGMG: Rule = Rule( //ID 32
    ruleName = "rulePreConvCauIntCorIGMG",
    condition = (pdrNode: NodeSeq, fileXmlWIthMeta: GasMetadata, params: Option[RuleParameters]) => {

      (
        (pdrNode \ Pre_int \ pre_conv).text.equalsIgnoreCase("NO") &&
        isValued(pdrNode \ cau_int_cor)
        ) || (
        (pdrNode \ Post_int \ pre_conv).text.equalsIgnoreCase("NO") &&
        isValued(pdrNode \ cau_int_cor)
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
