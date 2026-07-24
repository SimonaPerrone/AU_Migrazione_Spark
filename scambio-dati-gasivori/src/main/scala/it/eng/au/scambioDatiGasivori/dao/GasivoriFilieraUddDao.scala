package it.eng.au.scambioDatiGasivori.dao

import it.eng.au.scambioDatiGasivori.dao.traits.Dao
import it.eng.au.scambioDatiGasivori.schema.gasivori.GasivoriFilieraUddSchema
import it.eng.au.scambioDatiGasivori.utility.Properties

class GasivoriFilieraUddDao extends Dao {
  override val tableName: String = Properties.getGasivoriFilieraUddTableName
  override val columns: List[String] = GasivoriFilieraUddSchema.getValues
  override val partitionColumn: Option[String] = Some(GasivoriFilieraUddSchema.t_executionid)
}
