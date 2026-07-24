package it.au.misure.ingestionMisureGasUnico.validate

import it.au.misure.ingestionMisureGasUnico.utility.PropertyUtility

object ValidateFileIGMG extends ValidateFile {
  override val rcuAziendaPTableName: String = PropertyUtility.getRcuAziendaPTable
  override val rcuGasUDDPTableName: String = PropertyUtility.getRcugasUDDPTable
  override val ammissibilitaLogTableName: String = PropertyUtility.getAmmissibilitaFileLogTable
  override val csvReportFileName: String = "ReportAmmissibilitàFileIGMG.txt"
  override val checkAmm = new CheckAmmissibilitaFileRulesIGMG
  override val ammissibilitaFolder: String = PropertyUtility.getAmmissibilitaIgmgPath
}
