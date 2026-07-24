package it.eng.au.pubblicazioneIndennizzi.dao.cig

import it.eng.au.indennizziMisureGasCommon.schema.DettaglioPdrSchema
import it.eng.au.pubblicazioneIndennizzi.dao.Dao
import it.eng.au.pubblicazioneIndennizzi.utility.Properties

class DettaglioPdrDao extends Dao {
  override val tableName: String = Properties.getDettaglioPdrTableName
  override val columns: List[String] = DettaglioPdrSchema.getValues
  override val partitionColumn: String = DettaglioPdrSchema.executionid
}
