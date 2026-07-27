package it.eng.au.calcoloSettlementGas.utility

import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment

object Properties {
  def getMonthDifferenceTimeBack: String = Environment.getProperty("monthDifference.timeBack")

  def getAnnoMese: String = Environment.getProperty("annomese")
  def getDaysInMonth: String = Environment.getProperty("days.in.month")

  def getTSG2TabProfiliGiornStdPercPath: String = Environment.getProperty("tsg.TabProfiliGiornStdPerc.basepath")
  def getTSG2TabProfiliGiornStdPercTableName: String = Environment.getProperty("tsg.TabProfiliGiornStdPerc.tableName")

  def getTSG2TabParametriCaratteristiciProfPrelPath: String = Environment.getProperty("tsg.TabParametriCaratteristiciProfPrel.basepath")
  def getTSG2TabParametriCaratteristiciProfPrelTableName: String = Environment.getProperty("tsg.TabParametriCaratteristiciProfPrel.tableName")

  def getTSG2TSGTFCTableName: String = Environment.getProperty("tsg.TSGTFC.tableName")
  def getTSG2TSGVPGTableName: String = Environment.getProperty("tsg.TSGVPG.tableName")
  def getTSG2TSGQKRIUDTableName: String = Environment.getProperty("tsg.TSGQKRIUD.tableName")

  def getTSG2AtgTabProfiliGiornStdPercTableName: String = Environment.getProperty("tsg.AtgTabProfiliGiornStdPerc.tableName")
  def getTSG2AtgTabProfiliGiornStdPercPath: String = Environment.getProperty("tsg.AtgTabProfiliGiornStdPerc.basepath")

  def getTSG2AtgTabProfiliGiornStdPercBkpTableName: String = Environment.getProperty("tsg.AtgTabProfiliGiornStdPercBkp.tableName")
  def getTSG2AtgTabProfiliGiornStdPercBkpPath: String = Environment.getProperty("tsg.AtgTabProfiliGiornStdPercBkp.basepath")

  def getRcugasRemiStatoPTableName: String = Environment.getProperty("rcugas.remiStato.tableName")
  def getRcugasRemiTableName: String = Environment.getProperty("rcugas.remi.tableName")

}
