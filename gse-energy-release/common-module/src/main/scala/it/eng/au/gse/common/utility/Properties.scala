package it.eng.au.gse.common.utility

import it.eng.au.gse.common.utility.environment.Environment

object Properties {
  // Calcolo Mensile
  def getYearMonth: String = Environment.getProperty("year.month")
  def gsePerimetroTableName: String = Environment.getProperty("gse.perimetro_er_ee.tablename")
  def gseRichiesteMensiliTableName: String = Environment.getProperty("gse.richiesta_er_m.tablename")
  def gseAggrMTableName: String = Environment.getProperty("gse.gse_aggr_m.tablename")
  def gseAggrMBasePath: String = Environment.getProperty("gse.gse_aggr_m.basepath")
  def gseAggrMExportTableName: String = Environment.getProperty("gse.gse_aggr_m_export.tablename")
  def gseAggrMExportBasePath: String = Environment.getProperty("gse.gse_aggr_m_expor.basepath")
  def dwhConsumiMensileTableName: String = Environment.getProperty("dwh_consumi.mensile.tablename")

  // Calcolo Annuale
  def getYear: String = Environment.getProperty("year")
  def gseRichiesteAnnualiTableName: String = Environment.getProperty("gse.richiesta_er_a.tablename")
  def gseAggrATableName: String = Environment.getProperty("gse.gse_aggr_a.tablename")
  def gseAggrABasePath: String = Environment.getProperty("gse.gse_aggr_a.basepath")
  def gseAggrAExportTableName: String = Environment.getProperty("gse.gse_aggr_a_export.tablename")
  def gseAggrAExportBasePath: String = Environment.getProperty("gse.gse_aggr_a_expor.basepath")
  def dwhConsumiAnnualeTableName: String = Environment.getProperty("dwh_consumi.annuale.tablename")
}
