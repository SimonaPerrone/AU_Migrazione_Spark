package it.eng.au.mid.dao.hive.rcu

import it.eng.au.mid.dao.hive.HiveDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.hive.rcu.RcuAziendaPModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.hive.rcu.RcuAziendaPSchema

class RcuAziendaPDao extends HiveDao[RcuAziendaPModel] {
  override val tableName: String = Environment.getProperty("hive.table.rcu_azienda_p")
  override val schema: SchemaEnum = RcuAziendaPSchema
}
