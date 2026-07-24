package it.au.misure.ingestionMisureGasUnico.utility

import com.typesafe.config.{Config, ConfigFactory}
import it.au.misure.ingestionMisureGasUnico.model.validate.RuleParameters

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.Try

object PropertyUtility {
  val config: Config = ConfigFactory.load

  def getAmmissibilitaPdrFileMaxLength: Long = config.getLong("ammissibilita.fileMaxLength")

  def getXsdPeriodicoPath: String = config.getString("ammissibilita.xsd.periodico")
  def getXsdRettificaPath: String = config.getString("ammissibilita.xsd.rettifica")
  def getXsdIgmgPath: String = config.getString("ammissibilita.xsd.igmg")
  def getXsdIgmrPath: String = config.getString("ammissibilita.xsd.igmr")

  def getTmpOutputFolder: String = config.getString("tempRootPath")
  def getTmpOutputFolderOld: String = config.getString("tempRootPathOld")
  def getUnzipInputPath: String = config.getString("unzip.inputPath")

  def getTimeZone: String = config.getString("timeZone")

  def getCmgGasDb: String = config.getString("hive.db.cmg_gas")

  def getUnzipLogTable: String = config.getString("hive.table.unzipLog")
  def getFlowLogTable: String = config.getString("hive.table.flowLog")
  def getAmmissibilitaFileLogTable: String = config.getString("hive.table.ammissibilitaFile")
  def getRcuAziendaPTable: String = config.getString("hive.table.rcuAziendaP")
  def getRcugasUDDPTable: String = config.getString("hive.table.rcugasUDDP")
  def getAmmissibilitaPdrLogTable: String = config.getString("hive.table.ammissibilitaPdr")
  def getRcugasPdrTable: String = config.getString("hive.table.rcuGasPdr")
  def getRcugasPdrStatoTable: String = config.getString("hive.table.rcuGasPdrStato")
  def getCheckSqoop: Boolean = Try(config.getString("hive.table.checkSqoop")).getOrElse("false") == "true"

  def getRecoveryCsvPath: String = config.getString("fileRecovery.path")

  def getAmmissibilitaStandardPath: String = config.getString("ammissibilita.standard.path")
  def getAmmissibilitaIgmgPath: String = config.getString("ammissibilita.igmg.path")
  def getAmmissibilitaIgmrPath: String = config.getString("ammissibilita.igmr.path")


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
