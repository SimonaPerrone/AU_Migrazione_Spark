package it.eng.au.scambioDatiGasivori.dao

import it.eng.au.scambioDatiGasivori.dao.traits.Dao
import it.eng.au.scambioDatiGasivori.schema.gasivori.GasivoriPerimetroAmmSchema
import it.eng.au.scambioDatiGasivori.utility.Properties

class GasivoriPerimetroAmmDao extends Dao {
  override val tableName: String = Properties.getGasivoriPerimetroAmmTableName
  override val columns: List[String] = GasivoriPerimetroAmmSchema.getValues
}
