package it.au.misure.calcolo_capacita.component.hdao.model

import it.au.misure.calcolo_capacita.component.contract.HDao

case class PerimetroPdrHDao() extends HDao {

  override def getTableIdentifier: String = "hive.table.perimetropdr.name"

  override def getDbIdentifier: String = "hive.table.perimetropdr.db"

  override def getPathHdfsIdentifier: Option[String] = None
}
