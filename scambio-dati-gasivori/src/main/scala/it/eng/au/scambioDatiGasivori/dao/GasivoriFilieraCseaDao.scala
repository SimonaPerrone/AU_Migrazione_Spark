package it.eng.au.scambioDatiGasivori.dao

import it.eng.au.scambioDatiGasivori.dao.traits.Dao
import it.eng.au.scambioDatiGasivori.schema.gasivori.GasivoriFilieraCseaSchema
import it.eng.au.scambioDatiGasivori.utility.Properties

class GasivoriFilieraCseaDao extends Dao {
  override val tableName: String = Properties.getGasivoriFilieraCseaTableName
  override val columns: List[String] = GasivoriFilieraCseaSchema.getValues
  override val partitionColumn: Option[String] = Some(GasivoriFilieraCseaSchema.t_executionid)
}
