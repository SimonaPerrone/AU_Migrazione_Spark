package it.eng.au.pubblicazioneRendiconti.utility.properties

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment

object Properties {
  def isRecoveryMode: Boolean = Environment.getProperty("recovery.mode").equalsIgnoreCase("true")

  def getCigIndennizziRzg2TableName: String = Environment.getProperty("cig.indennizziRzg2.tableName")
  def getCigIndennizziRzg2ExecutionId: String = Environment.getProperty("cig.indennizziRzg2.executionId")

  def getReportPubblicazioneRzg2Path: String = Environment.getProperty("cig.reportPubblicazioneRzg2.basepath")
  def getReportPubblicazioneRzg2TableName: String = Environment.getProperty("cig.reportPubblicazioneRzg2.tableName")
  def getIsilonBasepathTmp: String = Environment.getProperty("isilon.basepath.tmp")
  def getIsilonBasepathOut: String = Environment.getProperty("isilon.basepath.out")

  def getMaxNumRowFile: String = Environment.getProperty("maxNumRowFile")
}
