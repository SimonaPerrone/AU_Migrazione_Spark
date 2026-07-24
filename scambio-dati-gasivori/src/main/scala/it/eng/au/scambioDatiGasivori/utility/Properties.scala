package it.eng.au.scambioDatiGasivori.utility

object Properties {
  def getOutputFileModes: String = Environment.getProperty("output.file.modes")
  def getIsilonBasepathTmp: String = Environment.getProperty("isilon.basepath.tmp")
  def getIsilonBasepathOut: String = Environment.getProperty("isilon.basepath.out")
  def getInfoLogBasepath: String = Environment.getProperty("hdfs.infoLog.basepath")
  def getInfoLogTableName: String = Environment.getProperty("hdfs.infoLog.tableName")

  def getPublicationType: String = Environment.getProperty("publication.type")
  def getYearMonth: String = Environment.getProperty("year.month")
  def getDateRun: String = Environment.getProperty("daterun")

  def getMaxNumRowFile: String = Environment.getProperty("maxNumRowFile")
  def getMaxSizeThresholdZip: String = Environment.getProperty("maxSizeThresholdZip")

  def getGasivoriFilieraCcTableName: String = Environment.getProperty("gasivoriFiliera.cc.tableName")
  def getGasivoriFilieraCseaTableName: String = Environment.getProperty("gasivoriFiliera.csea.tableName")
  def getGasivoriFilieraIdTableName: String = Environment.getProperty("gasivoriFiliera.id.tableName")
  def getGasivoriFilieraUdbTableName: String = Environment.getProperty("gasivoriFiliera.udb.tableName")
  def getGasivoriFilieraUddTableName: String = Environment.getProperty("gasivoriFiliera.udd.tableName")
  def getGasivoriFilieraCcExecutionId: String = Environment.getProperty("gasivoriFiliera.cc.executionId")
  def getGasivoriFilieraCseaExecutionId: String = Environment.getProperty("gasivoriFiliera.csea.executionId")
  def getGasivoriFilieraIdExecutionId: String = Environment.getProperty("gasivoriFiliera.id.executionId")
  def getGasivoriFilieraUdbExecutionId: String = Environment.getProperty("gasivoriFiliera.udb.executionId")
  def getGasivoriFilieraUddExecutionId: String = Environment.getProperty("gasivoriFiliera.udd.executionId")
  def getGasivoriPerimetroAmmTableName: String = Environment.getProperty("gasivoriPerimetro.amm.tableName")
  def getAmmIdGasivoriFile: String = Environment.getProperty("amm.nIdGasivoriFile")
}
