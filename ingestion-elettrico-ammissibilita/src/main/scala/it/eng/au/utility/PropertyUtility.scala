package it.eng.au.utility

import com.typesafe.config.{Config, ConfigFactory}
import it.eng.au.model.RuleParameters

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.Try

object PropertyUtility {
  val config: Config = ConfigFactory.load

  def getTmp1GAmmissibilita: String = config.getString("tmp.1GAmmissibilita")
  def getTmp2GAmmissibilita: String = config.getString("tmp.2GAmmissibilita")

  def getOutput1GAmmissibilita: String = config.getString("output.1GAmmissibilita")
  def getOutput2GAmmissibilita: String = config.getString("output.2GAmmissibilita")
  def getAmmissibilitaPodFileMaxLength: Long = config.getLong("output.ammissibilitaPodFileMaxLength")

  def getXsdPeriodicoPath: String = config.getString("xsd.periodico")
  def getXsdRettificaPath: String = config.getString("xsd.rettifica")
  def getXsdSMISPath: String = config.getString("xsd.smis")

  def getAmmissibilitaPodLogTable: String = config.getString("hive.table.ammissibilitaPod")
  def getAmmissibilitaFileLogTable: String = config.getString("hive.table.ammissibilitaFile")

  def getRcuAziendaPTable: String = config.getString("hive.table.rcuAziendaP")
  def getRcuUddPTable: String = config.getString("hive.table.rcuUDDP")
  def getUnzipReportTable: String = config.getString("hive.table.reportDecompressione")
  def getRcuPodStatoPTable: String = config.getString("hive.table.rcuPodStatoP")
  def getRcuPodDistrPTable: String = config.getString("hive.table.rcuPodDistrP")
  def getRcuDistrPTable: String = config.getString("hive.table.rcuDistrP")
  def getRcuEmtPTable: String = config.getString("hive.table.rcuEmtP")
  def getRcuPodPTable: String = config.getString("hive.table.rcuPodP")
  def getRcuPodUddPTable: String = config.getString("hive.table.rcuPodUddP")
  def getRcusUddPTable: String = config.getString("hive.table.rcusUddP")
  def getRcusPodUddPTable: String = config.getString("hive.table.rcusPodUddP")
  def getRcusPodPTable: String = config.getString("hive.table.rcusPodP")
  def getRcusPodDistrPTable: String = config.getString("hive.table.rcusPodDistrP")
  def getCheckSqoop: Boolean = Try(config.getString("hive.table.checkSqoop")).getOrElse("false") == "true"

  def getParametersMap(ammissibilitaType: String): Map[String, RuleParameters] = {
    val paramsString = s"parameters.$ammissibilitaType"
    PropertyUtility.config.getObject(paramsString).unwrapped().asScala.keys.map(key => {
      (key, RuleParameters(
        isActive = PropertyUtility.config.getBoolean(s"$paramsString.$key.active"),
        bloccante = PropertyUtility.config.getBoolean(s"$paramsString.$key.bloccante"),
        parameters = PropertyUtility.config.getObject(s"$paramsString.$key.variables").unwrapped().asScala.toMap.asInstanceOf[Map[String,String]]
      )
      )
    }).toMap
  }
}
