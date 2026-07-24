package it.au.misure.calcolo_capacita.component.hdao.model

import it.au.misure.calcolo_capacita.component.contract.HDao

case class RCUGasMassivoPHDao() extends HDao {

  override def getTableIdentifier: String = "hive.table.rcugasmassivop.name"

  override def getDbIdentifier: String = "hive.table.rcugasmassivop.db"

  override def getPathHdfsIdentifier: Option[String] = None
}
