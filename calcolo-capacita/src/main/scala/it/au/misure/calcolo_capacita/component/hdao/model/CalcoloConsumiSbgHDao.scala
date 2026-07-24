package it.au.misure.calcolo_capacita.component.hdao.model

import it.au.misure.calcolo_capacita.component.contract.HDao

case class CalcoloConsumiSbgHDao() extends HDao {

  override def getTableIdentifier: String = "hive.table.sbgmisure.name"

  override def getDbIdentifier: String = "hive.table.sbgmisure.db"

  override def getPathHdfsIdentifier: Option[String] = None
}
