package it.eng.au.ammissibilitaRendiconti.utility.environment

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment

object Properties {
  def isRecoveryMode: Boolean = Environment.getProperty("recovery.mode").equalsIgnoreCase("true")
  def getRecoveryCsvPath: String = Environment.getProperty("recovery.csv.path")

  def getRzg1ZipInputPath: String = Environment.getProperty("rzg1Zip.input.path")
  def getRzg1AmmOutputPath: String = Environment.getProperty("rzg1Amm.output.path")
  def getCigAggregatoTotaleTableName: String = Environment.getProperty("cig.aggregatoTotale.tableName")

  def getReportAmmissibilitaRzg1Path: String = Environment.getProperty("cig.reportAmmissibilita.rzg1.basepath")
  def getReportAmmissibilitaRzg1TableName: String = Environment.getProperty("cig.reportAmmissibilita.rzg1.tableName")

  def getCigIndennizziRzg2Path: String = Environment.getProperty("cig.indennizziRzg2.basepath")
  def getCigIndennizziRzg2TableName: String = Environment.getProperty("cig.indennizziRzg2.tableName")

  def getCigDeltaEuroPath: String = Environment.getProperty("cig.deltaEuro.basepath")
  def getCigDeltaEuroTableName: String = Environment.getProperty("cig.deltaEuro.tableName")

  def getCurrentYear: String = Environment.getProperty("current.year")
  def getCurrentMonth: String = Environment.getProperty("current.month")

  def isRuleAlreadyComputedEnabled: Boolean = Environment.getProperty("rule.zip.alreadyComputed.enabled").equalsIgnoreCase("true")
  def isRuleAlreadyTransmittedEnabled: Boolean = Environment.getProperty("rule.zip.alreadyTransmitted.enabled").equalsIgnoreCase("true")
  def isRuleCheckZipAndCsvNamesEnabled: Boolean = Environment.getProperty("rule.zip.checkZipAndCsvNames.enabled").equalsIgnoreCase("true")
  def isRuleCheckPivaEnabled: Boolean = Environment.getProperty("rule.zip.checkPiva.enabled").equalsIgnoreCase("true")
  def isRuleCheckCsvEncodingEnabled: Boolean = Environment.getProperty("rule.zip.checkCsvEncoding.enabled").equalsIgnoreCase("true")
  def isRuleCheckNumberOfCsvFieldsEnabled: Boolean = Environment.getProperty("rule.zip.checkNumberOfCsvFields.enabled").equalsIgnoreCase("true")

  def isRuleCheckHeaderEnabled: Boolean = Environment.getProperty("rule.csv.checkHeader.enabled").equalsIgnoreCase("true")
  def isRuleCheckDateEnabled: Boolean = Environment.getProperty("rule.csv.checkDate.enabled").equalsIgnoreCase("true")
  def isRuleCheckIdIndennizzoEnabled: Boolean = Environment.getProperty("rule.csv.checkIdIndennizzo.enabled").equalsIgnoreCase("true")
  def isRuleCheckPivaIdEnabled: Boolean = Environment.getProperty("rule.csv.checkPivaId.enabled").equalsIgnoreCase("true")
  def isRuleCheckPivaUddEnabled: Boolean = Environment.getProperty("rule.csv.checkPivaUdd.enabled").equalsIgnoreCase("true")
  def isRuleCheckRagSocIdEnabled: Boolean = Environment.getProperty("rule.csv.checkRagSocId.enabled").equalsIgnoreCase("true")
  def isRuleCheckRagSocUddEnabled: Boolean = Environment.getProperty("rule.csv.checkRagSocUdd.enabled").equalsIgnoreCase("true")
  def isRuleCheckPivaIdConsistencyEnabled: Boolean = Environment.getProperty("rule.csv.checkPivaIdConsistency.enabled").equalsIgnoreCase("true")
  def isRuleCheckPivaUddConsistencyEnabled: Boolean = Environment.getProperty("rule.csv.checkPivaUddConsistency.enabled").equalsIgnoreCase("true")

  def isRuleValidateOM1Enabled: Boolean = Environment.getProperty("rule.validateOM1.enabled").equalsIgnoreCase("true")
  def isRuleValidateOM2Enabled: Boolean = Environment.getProperty("rule.validateOM2.enabled").equalsIgnoreCase("true")
  def isRuleValidateOM3Enabled: Boolean = Environment.getProperty("rule.validateOM3.enabled").equalsIgnoreCase("true")
  def isRuleValidateIdIndennizzoEnabled: Boolean = Environment.getProperty("rule.validateIdIndennizzo.enabled").equalsIgnoreCase("true")
  def isRuleCheckAtLeastOneOMIsValuedEnabled: Boolean = Environment.getProperty("rule.checkAtLeastOneOMIsValued.enabled").equalsIgnoreCase("true")
}