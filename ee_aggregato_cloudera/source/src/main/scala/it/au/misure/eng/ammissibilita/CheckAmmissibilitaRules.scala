package it.au.misure.eng.ammissibilita

import it.au.misure.eng.model.{ReportMessage, Rule, RuleParameters, XMLMetadata}
import it.au.misure.eng.utility.Constants._
import it.au.misure.eng.utility.PropertyUtility
import it.au.misure.util.LoggingSupport

import scala.xml.NodeSeq

trait CheckAmmissibilitaRules[T <: ReportMessage] extends LoggingSupport {
  def check(xml: NodeSeq, fileXmlWIthMeta: XMLMetadata): T = {
    val errorRule = rulesWithParameters.find(ruleWithParameters => {
      ruleWithParameters.isActive && ruleWithParameters.condition(xml, fileXmlWIthMeta, ruleWithParameters.parameter)
    })

    val errorRuleBloccante = rulesWithParameters.find(rule => {
      rule.isActive && rule.message.bloccante.equals(BLOCCANTE) && rule.condition(xml, fileXmlWIthMeta, rule.parameter)
    })


    val message = if(errorRule.isDefined) {
      if (errorRuleBloccante.isDefined)
        errorRuleBloccante.get.message
      else
        errorRule.get.message

    } else
      okMessage

    addFileInfoToMessage(message, xml, fileXmlWIthMeta)
  }

  def okMessage: ReportMessage
  def addFileInfoToMessage(message: ReportMessage, xml: NodeSeq, fileXmlWIthMeta: XMLMetadata): T

  val MONTH_YEAR_PATTERN = "MM/yyyy"
  val ITALIAN_DATE_PATTERN = "dd/MM/yyyy"
  val FILE_TIMESTAMP_PATTERN="yyyyMMddHHmmss"

  val flusso1List = List("pdo", "pno", "pdo2g", "pno2g", "vno", "vno2g", "snm", "snm2g")
  val flusso2List = List("rfo", "rfo2g", "rno", "rno2g", "rnv", "rnv2g", "rsn", "rsn2g")

  def isFlusso(flussoName: String, typeFLussoList: List[String]): Boolean = typeFLussoList.exists(flussoName.toLowerCase().contains(_))
  def isValued(node: NodeSeq): Boolean = node.nonEmpty && node.text.nonEmpty

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

  def printRules(): Unit = log.info(s"rules: ${rulesWithParameters.mkString("\n")}")

}
