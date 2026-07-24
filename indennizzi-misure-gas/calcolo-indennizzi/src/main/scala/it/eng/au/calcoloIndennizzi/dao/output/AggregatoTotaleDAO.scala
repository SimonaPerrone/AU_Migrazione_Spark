package it.eng.au.calcoloIndennizzi.dao.output

import it.eng.au.calcoloIndennizzi.utility.Properties
import it.eng.au.indennizziMisureGasCommon.schema.AggregatoTotaleSchema

class AggregatoTotaleDAO extends OutputDAO {
  val tableName: String = Properties.getAggregatoTotaleTableName
  val parquetPath: String = Properties.getAggregatoTotalePath
  val columns: List[String] = AggregatoTotaleSchema.getValues
  val partitionColumns: List[String] = List(AggregatoTotaleSchema.annomese, AggregatoTotaleSchema.executionid)
}