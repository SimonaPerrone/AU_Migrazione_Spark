package it.au.misure.ingestionMisureGasUnico.validate

import it.au.misure.ingestionMisureGasUnico.model.GasMetadata
import it.au.misure.ingestionMisureGasUnico.model.validate.{ReportMessage, Rule, RuleParameters}
import it.au.misure.ingestionMisureGasUnico.utility.Constants.{BLOCCANTE, NON_BLOCCANTE}
import it.au.misure.ingestionMisureGasUnico.utility.PropertyUtility
import org.apache.log4j.Logger

import scala.xml.NodeSeq

trait CheckAmmissibilitaRules[T <: ReportMessage] extends Serializable {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def check(xml: NodeSeq, gasXmlMetadata: GasMetadata): T = {
    val errorRule = rulesWithParameters.find(ruleWithParameters => {
      ruleWithParameters.isActive && ruleWithParameters.condition(xml, gasXmlMetadata, ruleWithParameters.parameter)
    })

    val message = if(errorRule.isDefined) {
      val errorRuleBloccante = rulesWithParameters.find(rule => {
        rule.isActive && rule.message.bloccante.equals(BLOCCANTE) && rule.condition(xml, gasXmlMetadata, rule.parameter)
      })
      if (errorRuleBloccante.isDefined)
        errorRuleBloccante.get.message
      else
        errorRule.get.message
    } else
      okMessage

    addFileInfoToMessage(message, xml, gasXmlMetadata)
  }

  def okMessage: ReportMessage
  def addFileInfoToMessage(message: ReportMessage, xml: NodeSeq, gasXmlMetadata: GasMetadata): T

  val MONTH_YEAR_PATTERN = "MM/yyyy"
  val ITALIAN_DATE_PATTERN = "dd/MM/yyyy"
  val FILE_TIMESTAMP_PATTERN="yyyyMMddHHmmss"

  def isFlusso(flussoName: String, typeFLussoList: Set[String]): Boolean = typeFLussoList.contains(flussoName.toLowerCase)
  def isValued(node: NodeSeq): Boolean = node.nonEmpty && node.text.nonEmpty
  def isValuedText(node: NodeSeq): Boolean = node.text.nonEmpty

  lazy val rulesWithParameters: List[Rule] = {
    rules.map(rule => {
      val ruleParameter = rulesParameters.get(rule.ruleName)
      if(ruleParameter.isDefined) {
          if(ruleParameter.get.bloccante) rule.message.bloccante = BLOCCANTE else rule.message.bloccante = NON_BLOCCANTE
          rule.copy(isActive = ruleParameter.get.isActive, parameter = ruleParameter)
        } else rule
    })
  }

  def rules: List[Rule]

  def ammissibilitaType: String

  val rulesParameters: Map[String, RuleParameters] = PropertyUtility.getParametersMap(ammissibilitaType)

  def printRules(): Unit = logger.warn(s"rules: ${rulesWithParameters.mkString("\n")}")

  logger.warn(s"rulesParameters: ${rulesParameters.mkString("\n")}")
}
