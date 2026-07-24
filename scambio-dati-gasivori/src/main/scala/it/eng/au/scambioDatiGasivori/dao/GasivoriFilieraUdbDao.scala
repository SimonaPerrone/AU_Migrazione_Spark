package it.eng.au.scambioDatiGasivori.dao

import it.eng.au.scambioDatiGasivori.dao.traits.Dao
import it.eng.au.scambioDatiGasivori.schema.gasivori.GasivoriFilieraUdbSchema
import it.eng.au.scambioDatiGasivori.utility.Properties

class GasivoriFilieraUdbDao extends Dao {
  override val tableName: String = Properties.getGasivoriFilieraUdbTableName
  override val columns: List[String] = GasivoriFilieraUdbSchema.getValues
  override val partitionColumn: Option[String] = Some(GasivoriFilieraUdbSchema.t_executionid)
}
