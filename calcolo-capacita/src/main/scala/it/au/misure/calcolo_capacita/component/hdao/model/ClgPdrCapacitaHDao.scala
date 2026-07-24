package it.au.misure.calcolo_capacita.component.hdao.model

import it.au.misure.calcolo_capacita.component.contract.HDao

case class ClgPdrCapacitaHDao() extends HDao {

  override def getTableIdentifier: String = "hive.table.result.name"

  override def getDbIdentifier: String = "hive.table.result.db"

  override def getPathHdfsIdentifier: Option[String] = None
}
