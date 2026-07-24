package it.eng.au.cceCalcolo.utility.property

import com.typesafe.config.{Config, ConfigFactory}

object Properties {
  private var config: Config = ConfigFactory.load

  def setProperty(key: String, value: String): Unit = config = ConfigFactory.parseString(s"$key=$value").withFallback(config)

  // TABLES
  // flussi misure
  def getFlussoMisureEstensioneQuartiTablePath: String = config.getString("hive.input.flussoMisureEstensioneQuarti.path")
  def getFlussoMisureEstensioneQuartiTableName: String = config.getString("hive.input.flussoMisureEstensioneQuarti.table")
  def getFlussoMisureQuartiTablePath: String = config.getString("hive.input.flussoMisureQuarti.path")
  def getFlussoMisureQuartiTableName: String = config.getString("hive.input.flussoMisureQuarti.table")
  def getFlussoMisureEstensioneQuartiInTablePath: String = config.getString("hive.input.flussoMisureEstensioneQuartiIn.path")
  def getFlussoMisureEstensioneQuartiInTableName: String = config.getString("hive.input.flussoMisureEstensioneQuartiIn.table")
  def getFlussoMisureQuartiInTablePath: String = config.getString("hive.input.flussoMisureQuartiIn.path")
  def getFlussoMisureQuartiInTableName: String = config.getString("hive.input.flussoMisureQuartiIn.table")

  // Report ammissibilita
  def getReportAmmissibilitaPodPath: String = config.getString("hive.input.reportAmmissibilitaPod.path")
  def getReportAmmissibilitaPodTable: String = config.getString("hive.input.reportAmmissibilitaPod.table")
  def getReportAmmissibilitaFilePath: String = config.getString("hive.input.reportAmmissibilitaFile.path")
  def getReportAmmissibilitaFileTable: String = config.getString("hive.input.reportAmmissibilitaFile.table")

  // rcu
  def getRcuAziendaPTablePath: String = config.getString("hive.input.rcuAziendaP.path")
  def getRcuAziendaPTableName: String = config.getString("hive.input.rcuAziendaP.table")
  def getRcuFornituraPTablePath: String = config.getString("hive.input.rcuFornituraP.path")
  def getRcuFornituraPTableName: String = config.getString("hive.input.rcuFornituraP.table")
  def getRcuPodDistrPTablePath: String = config.getString("hive.input.rcuPodDistrP.path")
  def getRcuPodDistrPTableName: String = config.getString("hive.input.rcuPodDistrP.table")
  def getRcuPodMisurePTablePath: String = config.getString("hive.input.rcuPodMisureP.path")
  def getRcuPodMisurePTableName: String = config.getString("hive.input.rcuPodMisureP.table")
  def getRcuPodPTablePath: String = config.getString("hive.input.rcuPodP.path")
  def getRcuPodPTableName: String = config.getString("hive.input.rcuPodP.table")
  def getRcuPodTecnPTablePath: String = config.getString("hive.input.rcuPodTecnP.path")
  def getRcuPodTecnPTableName: String = config.getString("hive.input.rcuPodTecnP.table")
  def getRcuPodUddPTablePath: String = config.getString("hive.input.rcuPodUddP.path")
  def getRcuPodUddPTableName: String = config.getString("hive.input.rcuPodUddP.table")
  def getRcuTariffaPTablePath: String = config.getString("hive.input.rcuTariffaP.path")
  def getRcuTariffaPTableName: String = config.getString("hive.input.rcuTariffaP.table")
  def getRcuUddPTablePath: String = config.getString("hive.input.rcuUddP.path")
  def getRcuUddPTableName: String = config.getString("hive.input.rcuUddP.table")
  // rcus
  def getRcusFornuturaPTablePath: String = config.getString("hive.input.rcusFornituraP.path")
  def getRcusFornuturaPTableName: String = config.getString("hive.input.rcusFornituraP.table")
  def getRcusPodMisurePTablePath: String = config.getString("hive.input.rcusPodMisureP.path")
  def getRcusPodMisurePTableName: String = config.getString("hive.input.rcusPodMisureP.table")
  def getRcusPodPTablePath: String = config.getString("hive.input.rcusPodP.path")
  def getRcusPodPTableName: String = config.getString("hive.input.rcusPodP.table")
  def getRcusPodUddPTablePath: String = config.getString("hive.input.rcusPodUddP.path")
  def getRcusPodUddPTableName: String = config.getString("hive.input.rcusPodUddP.table")

  //ghigliottina
  def getGhigliottinaTablePath: String = config.getString("hive.input.ghigliottina.path")
  def getGhigliottinaTableName: String = config.getString("hive.input.ghigliottina.table")

  //output annullamenti
  def getCceMoAnnullate1gTablePath: String = config.getString("hive.output.cceMoAnnullate1g.path")
  def getCceMoAnnullate1gTableName: String = config.getString("hive.output.cceMoAnnullate1g.table")
  def getCceMoAnnullate2gTablePath: String = config.getString("hive.output.cceMoAnnullate2g.path")
  def getCceMoAnnullate2gTableName: String = config.getString("hive.output.cceMoAnnullate2g.table")
  def getCceMoAnnullate1gEinTablePath: String = config.getString("hive.output.cceMoAnnullate1gEin.path")
  def getCceMoAnnullate1gEinTableName: String = config.getString("hive.output.cceMoAnnullate1gEin.table")
  def getCceMoAnnullate2gEinTablePath: String = config.getString("hive.output.cceMoAnnullate2gEin.path")
  def getCceMoAnnullate2gEinTableName: String = config.getString("hive.output.cceMoAnnullate2gEin.table")

  //output calcolo
  def getCceCalcoloAnagraficaTablePath: String = config.getString("hive.output.cceCalcoloAnagrafica.path")
  def getCceCalcoloAnagraficaTableName: String = config.getString("hive.output.cceCalcoloAnagrafica.table")
  def getCceCalcoloPTablePath: String = config.getString("hive.output.cceCalcoloP.path")
  def getCceCalcoloPTableName: String = config.getString("hive.output.cceCalcoloP.table")
  def getCceCalcoloPeinTablePath: String = config.getString("hive.output.cceCalcoloPein.path")
  def getCceCalcoloPeinTableName: String = config.getString("hive.output.cceCalcoloPein.table")
  def getCceCalcoloPRTablePath: String = config.getString("hive.output.cceCalcoloPR.path")
  def getCceCalcoloPRTableName: String = config.getString("hive.output.cceCalcoloPR.table")
  def getCceCalcoloPReinTablePath: String = config.getString("hive.output.cceCalcoloPRein.path")
  def getCceCalcoloPReinTableName: String = config.getString("hive.output.cceCalcoloPRein.table")
  def getCceCalcoloTrattamentoTablePath: String = config.getString("hive.output.cceCalcoloTrattamento.path")
  def getCceCalcoloTrattamentoTableName: String = config.getString("hive.output.cceCalcoloTrattamento.table")
  def getCceCalcTrackTablePath: String = config.getString("hive.output.cceCalcTrack.path")
  def getCceCalcTrackTableName: String = config.getString("hive.output.cceCalcTrack.table")

}
