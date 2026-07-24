package it.au.misure.ingestionMisureGasUnico.validate

import it.au.misure.ingestionMisureGasUnico.utility.PropertyUtility

object ValidateFileStandard extends ValidateFile {
  override val rcuAziendaPTableName: String = PropertyUtility.getRcuAziendaPTable
  override val rcuGasUDDPTableName: String = PropertyUtility.getRcugasUDDPTable
  override val ammissibilitaLogTableName: String = PropertyUtility.getAmmissibilitaFileLogTable
  override val csvReportFileName: String = "ReportAmmissibilitàFileGAS.txt"
  override val checkAmm = new CheckAmmissibilitaFileRules
  override val ammissibilitaFolder: String = PropertyUtility.getAmmissibilitaStandardPath
}
