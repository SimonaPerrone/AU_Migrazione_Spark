package it.eng.au.calcoloIndennizzi.utility

import it.eng.au.indennizziMisureGasCommon.utility.date.DateUtility.convertStringToPeriod
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.joda.time.base.BaseSingleFieldPeriod

object Properties {
  // Run configuration
  def isRecoveryMode: Boolean = Environment.getProperty("recovery.mode").equalsIgnoreCase("true")
  def getMonthDifferenceTimeBack: BaseSingleFieldPeriod = convertStringToPeriod(Environment.getProperty("monthDifference.timeBack"))
  def getRecoveryCsvPath: String = Environment.getProperty("recovery.csv.path")
  def getYearMonth: String = Environment.getProperty("year.month")
  def getDaysInMonth: String = Environment.getProperty("days.in.month")
  def getTglDayOfMonthThreshold: String = Environment.getProperty("tgl.dayOfMonth.threshold")
  def isOM1Enabled: Boolean = Environment.getProperty("om1.isEnabled").equalsIgnoreCase("true")
  def isOM2Enabled: Boolean = Environment.getProperty("om2.isEnabled").equalsIgnoreCase("true")
  def isOM3Enabled: Boolean = Environment.getProperty("om3.isEnabled").equalsIgnoreCase("true")
  def getOM2LowerBound: String = Environment.getProperty("om2.lowerBound")
  def getOM2UpperBound: String = Environment.getProperty("om2.upperBound")
  def getOM3LowerBound: String = Environment.getProperty("om3.lowerBound")
  def getOM3UpperBound: String = Environment.getProperty("om3.upperBound")
  def getOM1TargetPercent: String = Environment.getProperty("om1.target.percent")
  def getOM2TargetPercent: String = Environment.getProperty("om2.target.percent")
  def getOM3TargetPercent: String = Environment.getProperty("om3.target.percent")
  def getOM1EuroFee: String = Environment.getProperty("om1.euro.fee")
  def getOM2EuroFee: String = Environment.getProperty("om2.euro.fee")
  def getOM3EuroFee: String = Environment.getProperty("om3.euro.fee")

  // Rcu/rcugas tables
  def getRcugasMassivoPath: String = Environment.getProperty("rcugasMassivoP.basepath")
  def getRcugasVarMisuratorePath: String = Environment.getProperty("rcugasVarMisuratoreP.baepath")
  def getRcugasFornituraPath: String = Environment.getProperty("rcugasFornituraP.basepath")
  def getRcugasUddPath: String = Environment.getProperty("rcugasUdd.basepath")
  def getRcugasConnessioniDistr2Path: String = Environment.getProperty("rcugasConnessioniDistr2.basepath")
  def getRcugasSospensioniPath: String = Environment.getProperty("rcugasSospensioni.basepath")
  def getRcugasVarTrattamentoPath: String = Environment.getProperty("rcuGasVarTrattamentoP.basepath")
  def getRcuAziendaPath: String = Environment.getProperty("rcuAzienda.basepath")

  // Output tables
  def getAggregatoTotaleTableName: String = Environment.getProperty("aggregatoTotale.tableName")
  def getAggregatoTotalePath: String = Environment.getProperty("aggregatoTotale.basepath")
  def getPdrTotaleTableName: String = Environment.getProperty("pdrTotale.tableName")
  def getPdrTotalePath: String = Environment.getProperty("pdrTotale.basepath")
  def getDettaglioPdrTableName: String = Environment.getProperty("dettaglioPdr.tableName")
  def getDettaglioPdrPath: String = Environment.getProperty("dettaglioPdr.basepath")
  def getDettaglioOM1TableName: String = Environment.getProperty("dettaglioOM1.tableName")
  def getDettaglioOM1Path: String = Environment.getProperty("dettaglioOM1.basepath")
  def getDettaglioOM2TableName: String = Environment.getProperty("dettaglioOM2.tableName")
  def getDettaglioOM2Path: String = Environment.getProperty("dettaglioOM2.basepath")
  def getDettaglioOM3TableName: String = Environment.getProperty("dettaglioOM3.tableName")
  def getDettaglioOM3Path: String = Environment.getProperty("dettaglioOM3.basepath")

  // Tgl table
  def getTglTableName: String = Environment.getProperty("flow.dataset.tgl.tableName")

  // Exclusion filter
  def isExclusionFilterEnabled: Boolean = Environment.getProperty("filter.exclusion.enabled").equalsIgnoreCase("true")
  def getExclusionFilterPath: String = Environment.getProperty("filter.exclusion.file.path")

  // Duplicate filter
  def isDuplicateFilterEnabled: Boolean = Environment.getProperty("filter.duplicateMeasures.enable").equalsIgnoreCase("true")
  def isDuplicateFilterGroupByFilePathEnabled: Boolean = Environment.getProperty("filter.duplicateMeasures.groupByFilePath.enable").equalsIgnoreCase("true")
  def isDuplicateFilterGroupByTimestampEnabled: Boolean = Environment.getProperty("filter.duplicateMeasures.groupByTimestamp.enable").equalsIgnoreCase("true")
  def isDuplicateFilterGroupByFileNameEnabled: Boolean = Environment.getProperty("filter.duplicateMeasures.groupByFileName.enable").equalsIgnoreCase("true")
}
