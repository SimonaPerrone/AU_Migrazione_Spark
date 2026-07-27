package it.au.misure.ee_switching.utility

import com.typesafe.config.{Config, ConfigFactory}

object PropertyUtility {
  val config: Config = ConfigFactory.load

  def getTimeZone: String = config.getString("timeZone")
  def getMaxNPodsPerXmlFileFunzionali: Int = config.getInt("xml.maxNPods.funzionali")
  def getMaxNPodsPerXmlFileStorici: Int = config.getInt("xml.maxNPods.storici")
  def getHiveDb: String = config.getString("hive.db")
  def getTableFunzionali: String = config.getString("hive.table.funzionali")
  def getTableStorici: String = config.getString("hive.table.storici")
  def getXmlTmpRootPath: String = config.getString("xml.tmpRootPath")
  def getzipOutputFileRootPath: String = config.getString("zip.outputRootPath")
  def getXSDFunzionali: String = config.getString("xml.xsdPath.funzionali")
  def getXSDStorici: String = config.getString("xml.xsdPath.storici")
  def getReportTable: String  = config.getString("hive.table.report")

}
