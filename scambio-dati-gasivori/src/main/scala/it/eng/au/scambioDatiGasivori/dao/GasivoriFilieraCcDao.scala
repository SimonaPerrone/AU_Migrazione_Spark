package it.eng.au.scambioDatiGasivori.dao

import it.eng.au.scambioDatiGasivori.dao.traits.Dao
import it.eng.au.scambioDatiGasivori.schema.gasivori.GasivoriFilieraCcSchema
import it.eng.au.scambioDatiGasivori.utility.Properties

class GasivoriFilieraCcDao extends Dao {
  override val tableName: String = Properties.getGasivoriFilieraCcTableName
  override val columns: List[String] = GasivoriFilieraCcSchema.getValues
  override val partitionColumn: Option[String] = Some(GasivoriFilieraCcSchema.t_executionid)
}
