package it.eng.au.pubblicazioneIndennizzi.utility

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment

object Properties {
  def isRecoveryMode: Boolean = Environment.getProperty("recovery.mode").equalsIgnoreCase("true")
  def getAggregatoTotaleTableName: String = Environment.getProperty("cig.aggregatoTotale.tableName")
  def getDettaglioPdrTableName: String = Environment.getProperty("cig.dettaglioPdr.tableName")
  def getInputTableExecutionId: String = Environment.getProperty("input.table.executionid")
  def getMaxNumRowFile: String = Environment.getProperty("maxNumRowFile")
  def getIsilonBasepathTmp: String = Environment.getProperty("isilon.basepath.tmp")
  def getIsilonBasepathOut: String = Environment.getProperty("isilon.basepath.out")
  def getMaxSizeThresholdZip: String = Environment.getProperty("maxSizeThresholdZip")
  def getPublicationType: String = Environment.getProperty("publication.type") // CIG.
  def getPubblicazioneIndennizziTableName: String = Environment.getProperty("cig.pubblicazioneIndennizzi.tableName")
  def getPubblicazioneIndennizziParquetPath: String = Environment.getProperty("cig.pubblicazioneIndennizzi.basepath")
}
