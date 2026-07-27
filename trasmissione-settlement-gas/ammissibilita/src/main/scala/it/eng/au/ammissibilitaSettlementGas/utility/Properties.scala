package it.eng.au.ammissibilitaSettlementGas.utility

import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment


object Properties {
  def getInputVPGPath: String = Environment.getProperty("vpg.csv.path")
  def getInputTFCPath: String = Environment.getProperty("tfc.csv.path")
  def getInputQKRIUDPath: String = Environment.getProperty("tfc.csv.path")

  def isRuleAlreadyComputedTFCEnabled: Boolean = Environment.getProperty("rule.csv.alreadyComputedTFC.enabled").equalsIgnoreCase("true")
  def isRuleAlreadyComputedVPGEnabled: Boolean = Environment.getProperty("rule.csv.alreadyComputedVPG.enabled").equalsIgnoreCase("true")
  def isRuleAlreadyComputedQKRIUDEnabled: Boolean = Environment.getProperty("rule.csv.alreadyComputedQKRIUD.enabled").equalsIgnoreCase("true")

  def isRuleAlreadyTransmittedTFCEnabled: Boolean = Environment.getProperty("rule.csv.alreadyTransmittedTFC.enabled").equalsIgnoreCase("true")
  def isRuleAlreadyTransmittedVPGEnabled: Boolean = Environment.getProperty("rule.csv.alreadyTransmittedVPG.enabled").equalsIgnoreCase("true")
  def isRuleAlreadyTransmittedQKRIUDEnabled: Boolean = Environment.getProperty("rule.csv.alreadyTransmittedQKRIUD.enabled").equalsIgnoreCase("true")

  def isRuleCheckTFCFileNameEnabled:   Boolean = Environment.getProperty("rule.csv.checkTFCFileName.enabled").equalsIgnoreCase("true")
  def isRuleCheckVPGFileNameEnabled:   Boolean = Environment.getProperty("rule.csv.checkVPGFileName.enabled").equalsIgnoreCase("true")

  def isRuleCheckTFCPivaRdbEnabled:    Boolean = Environment.getProperty("rule.csv.checkTFCPivaRdb.enabled").equalsIgnoreCase("true")
  def isRuleCheckVPGPivaRdbEnabled:    Boolean = Environment.getProperty("rule.csv.checkVPGPivaRdb.enabled").equalsIgnoreCase("true")
  def isRuleCheckQKRIUDPivaRdbEnabled:    Boolean = Environment.getProperty("rule.csv.checkQKRIUDPivaRdb.enabled").equalsIgnoreCase("true")

  def isRuleCheckVPGFileHeaderEnabled: Boolean = Environment.getProperty("rule.csv.checkVPGFileHeader.enabled").equalsIgnoreCase("true")
  def isRuleCheckTFCFileHeaderEnabled: Boolean = Environment.getProperty("rule.csv.checkTFCFileHeader.enabled").equalsIgnoreCase("true")
  def isRuleCheckQKRIUDFileHeaderEnabled: Boolean = Environment.getProperty("rule.csv.checkQKRIUDFileHeader.enabled").equalsIgnoreCase("true")

  def isRuleCheckVPGFileExtensionEnabled: Boolean = Environment.getProperty("rule.csv.checkVPGFileExtension.enabled").equalsIgnoreCase("true")
  def isRuleCheckTFCFileExtensionEnabled: Boolean = Environment.getProperty("rule.csv.checkTFCFileExtension.enabled").equalsIgnoreCase("true")
  def isRuleCheckQKRIUDFileExtensionEnabled: Boolean = Environment.getProperty("rule.csv.checkQKRIUDFileExtension.enabled").equalsIgnoreCase("true")

  def isRuleCheckTFCFieldsNumberEnabled: Boolean = Environment.getProperty("rule.csv.checkTFCFieldsNumber.enabled").equalsIgnoreCase("true")
  def isRuleCheckVPGFieldsNumberEnabled: Boolean = Environment.getProperty("rule.csv.checkVPGFieldsNumber.enabled").equalsIgnoreCase("true")
  def isRuleCheckQKRIUDCodRemiFormatEnabled: Boolean = Environment.getProperty("rule.csv.checkQKRIUDCodRemiFormat.enabled").equalsIgnoreCase("true")
  def isRuleCheckQKRIUDfieldFormatEnabled: Boolean = Environment.getProperty("rule.csv.checkQKRIUDQKRIUDfieldFormat.enabled").equalsIgnoreCase("true")

  def isRuleCheckTFCFileIntegrityEnabled: Boolean = Environment.getProperty("rule.csv.checkTFCFileIntegrity.enabled").equalsIgnoreCase("true")
  def isRuleCheckVPGFileIntegrityEnabled: Boolean = Environment.getProperty("rule.csv.checkVPGFileIntegrity.enabled").equalsIgnoreCase("true")
  def isRuleCheckQKRIUDFileIntegrityEnabled: Boolean = Environment.getProperty("rule.csv.checkQKRIUDFileIntegrity.enabled").equalsIgnoreCase("true")

  def isRuleCheckTFCDateEnabled: Boolean = Environment.getProperty("rule.csv.checkTFCDate.enabled").equalsIgnoreCase("true")
  def isRuleCheckTFCDateFormatEnabled: Boolean = Environment.getProperty("rule.csv.checkTFCDateFormat.enabled").equalsIgnoreCase("true")
  def isRuleCheckTFCDateConsistenceEnabled: Boolean = Environment.getProperty("rule.csv.checkTFCDateConsistence.enabled").equalsIgnoreCase("true")

  def isRuleCheckTFCregClimMandatoryEnabled: Boolean = Environment.getProperty("rule.csv.checkTFCRegClimMandatory.enabled").equalsIgnoreCase("true")
  def isRuleCheckTFCRegClimMismatchEnabled: Boolean = Environment.getProperty("rule.csv.checkTFCRegClimMismatch.enabled").equalsIgnoreCase("true")

  def isRuleCheckTFCWKRDefinedEnabled: Boolean = Environment.getProperty("rule.csv.checkTFCWKRDefined.enabled").equalsIgnoreCase("true")
  def isRuleCheckTFCWKRFormatEnabled: Boolean = Environment.getProperty("rule.csv.checkTFCWKRFormat.enabled").equalsIgnoreCase("true")


  def isRuleCheckVPGDateEnabled: Boolean = Environment.getProperty("rule.csv.checkVPGDate.enabled").equalsIgnoreCase("true")
  def isRuleCheckVPGDateConsistenceEnabled: Boolean = Environment.getProperty("rule.csv.checkVPGDateConsistence.enabled").equalsIgnoreCase("true")
  def isRuleCheckVPGMandatoryFieldsEnabled: Boolean = Environment.getProperty("rule.csv.checkVPGMandatoryFields.enabled").equalsIgnoreCase("true")
  def isRuleCheckVPGFieldsFormatEnabled: Boolean = Environment.getProperty("rule.csv.checkVPGFieldsFormat.enabled").equalsIgnoreCase("true")
  def isRuleCheckVPGFieldsEnabled: Boolean = Environment.getProperty("rule.csv.checkVPGFields.enabled").equalsIgnoreCase("true")

  def isRuleCheckQKRIUDDateEnabled: Boolean = Environment.getProperty("rule.csv.checkQKRIUDDate.enabled").equalsIgnoreCase("true")
  def isRuleCheckQKRIUDDateConsistenceEnabled: Boolean = Environment.getProperty("rule.csv.checkQKRIUDDateConsistence.enabled").equalsIgnoreCase("true")
  def isRuleCheckQKRIUDfieldMandatoryEnabled: Boolean = Environment.getProperty("rule.csv.checkQKRIUDMandatoryField.enabled").equalsIgnoreCase("true")
  def isRuleCheckQKRIUDDateFormatEnabled: Boolean = Environment.getProperty("rule.csv.checkQKRIUDDateFormat.enabled").equalsIgnoreCase("true")
  def isRuleCheckQKRIUDcodRemiMandatoryEnabled: Boolean = Environment.getProperty("rule.csv.checkQKRIUDcodRemi.enabled").equalsIgnoreCase("true")
  def isRuleCheckQKRIUDDateExistanceEnabled: Boolean = Environment.getProperty("rule.csv.checkQKRIUDDateExistance.enabled").equalsIgnoreCase("true")
  def isRuleCheckCodRemiExistanceRcugasEnabled: Boolean = Environment.getProperty("rule.csv.checkCodRemiExistanceRcugas.enabled").equalsIgnoreCase("true")

  def getCsvInputPath: String = Environment.getProperty("csv.input.path")
  def getCurrentYear: String = Environment.getProperty("current.year")
  def getCurrentMonth: String = Environment.getProperty("current.month")
  def getCsvAmmissibilitaOutputPath: String = Environment.getProperty("csvAmmissibilita.output.path")

  def getTSG2TFCFilePath: String = Environment.getProperty("tsg.TFCFile.basepath")
  def getTSG2TFCFileTableName: String = Environment.getProperty("tsg.TFCFile.tableName")
  def getTSG2VPGFilePath: String = Environment.getProperty("tsg.VPGFile.basepath")
  def getTSG2VPGFileTableName: String = Environment.getProperty("tsg.VPGFile.tableName")
  def getTSG2QKRIUDFilePath: String = Environment.getProperty("tsg.QKRIUDFile.basepath")
  def getTSG2QKRIUDFileTableName: String = Environment.getProperty("tsg.QKRIUDFile.tableName")

  def getTSG2ReportAmmissibilitaTFCPath: String = Environment.getProperty("tsg.ReportAmmissibilitaTFC.basepath")
  def getTSG2ReportAmmissibilitaTFCTableName: String = Environment.getProperty("tsg.ReportAmmissibilitaTFC.tableName")
  def getTSG2ReportAmmissibilitaVPGPath: String = Environment.getProperty("tsg.ReportAmmissibilitaVPG.basepath")
  def getTSG2ReportAmmissibilitaVPGTableName: String = Environment.getProperty("tsg.ReportAmmissibilitaVPG.tableName")
  def getTSG2ReportAmmissibilitaQKRIUDPath: String = Environment.getProperty("tsg.ReportAmmissibilitaQKRIUD.basepath")
  def getTSG2ReportAmmissibilitaQKRIUDTableName: String = Environment.getProperty("tsg.ReportAmmissibilitaQKRIUD.tableName")

  def getTSG2PubblicazioneAmmissibilitaTFCPath: String = Environment.getProperty("tsg.PubblicazioneAmmissibilitaTFC.basepath")
  def getTSG2PubblicazioneAmmissibilitaTFCTableName: String = Environment.getProperty("tsg.PubblicazioneAmmissibilitaTFC.tableName")
  def getTSG2PubblicazioneAmmissibilitaVPGPath: String = Environment.getProperty("tsg.PubblicazioneAmmissibilitaVPG.basepath")
  def getTSG2PubblicazioneAmmissibilitaVPGTableName: String = Environment.getProperty("tsg.PubblicazioneAmmissibilitaVPG.tableName")
  def getTSG2PubblicazioneAmmissibilitaQKRIUDPath: String = Environment.getProperty("tsg.PubblicazioneAmmissibilitaQKRIUD.basepath")
  def getTSG2PubblicazioneAmmissibilitaQKRIUDTableName: String = Environment.getProperty("tsg.PubblicazioneAmmissibilitaQKRIUD.tableName")

  def getTSG2TSGTFCPath: String = Environment.getProperty("tsg.TSGTFC.basepath")
  def getTSG2TSGTFCTableName: String = Environment.getProperty("tsg.TSGTFC.tableName")
  def getTSG2TSGVPGPath: String = Environment.getProperty("tsg.TSGVPG.basepath")
  def getTSG2TSGVPGTableName: String = Environment.getProperty("tsg.TSGVPG.tableName")
  def getTSG2TSGQKRIUDPath: String = Environment.getProperty("tsg.TSGQKRIUD.basepath")
  def getTSG2TSGQKRIUDTableName: String = Environment.getProperty("tsg.TSGQKRIUD.tableName")

  def getRcugasRemiStatoPTableName: String = Environment.getProperty("rcugas.remiStato.tableName")
  def getRcugasRemiTableName: String = Environment.getProperty("rcugas.remi.tableName")

  /*
  def getCigIndennizziRzg2Path: String = Environment.getProperty("cig.indennizziRzg2.basepath")
  def getCigIndennizziRzg2TableName: String = Environment.getProperty("cig.indennizziRzg2.tableName")
   */
}
