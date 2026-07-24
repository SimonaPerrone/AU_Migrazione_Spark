package it.au.misure.calcolo_capacita.component.hdao.model

import it.au.misure.calcolo_capacita.component.contract.HDao

case class AnagraficaHDao() extends HDao {

  override def getTableIdentifier: String = "hive.table.anagrafica.name"

  override def getDbIdentifier: String = "hive.table.anagrafica.db"

  override def getPathHdfsIdentifier: Option[String] = None
}
