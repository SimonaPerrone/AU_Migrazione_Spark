package it.eng.au.scambioDatiGasivori.dao

import it.eng.au.scambioDatiGasivori.dao.traits.Dao
import it.eng.au.scambioDatiGasivori.schema.gasivori.GasivoriFilieraIdSchema
import it.eng.au.scambioDatiGasivori.utility.Properties

class GasivoriFilieraIdDao extends Dao {
  override val tableName: String = Properties.getGasivoriFilieraIdTableName
  override val columns: List[String] = GasivoriFilieraIdSchema.getValues
  override val partitionColumn: Option[String] = Some(GasivoriFilieraIdSchema.t_executionid)
}
