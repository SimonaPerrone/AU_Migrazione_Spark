package it.eng.au.calcoloIndennizzi.dao.output

import it.eng.au.calcoloIndennizzi.schema.cig.PdrTotaleSchema
import it.eng.au.calcoloIndennizzi.utility.Properties

class PdrTotaleDAO extends OutputDAO {
  val tableName: String = Properties.getPdrTotaleTableName
  val parquetPath: String = Properties.getPdrTotalePath
  val columns: List[String] = PdrTotaleSchema.getValues
  val partitionColumns: List[String] = List(PdrTotaleSchema.annomese, PdrTotaleSchema.executionid)
}