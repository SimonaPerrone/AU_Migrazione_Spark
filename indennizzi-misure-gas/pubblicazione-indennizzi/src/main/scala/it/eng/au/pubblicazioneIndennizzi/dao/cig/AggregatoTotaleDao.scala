package it.eng.au.pubblicazioneIndennizzi.dao.cig

import it.eng.au.indennizziMisureGasCommon.schema.AggregatoTotaleSchema
import it.eng.au.pubblicazioneIndennizzi.dao.Dao
import it.eng.au.pubblicazioneIndennizzi.utility.Properties

class AggregatoTotaleDao extends Dao {
  override val tableName: String = Properties.getAggregatoTotaleTableName
  override val columns: List[String] = AggregatoTotaleSchema.getValues
  override val partitionColumn: String = AggregatoTotaleSchema.executionid
}
