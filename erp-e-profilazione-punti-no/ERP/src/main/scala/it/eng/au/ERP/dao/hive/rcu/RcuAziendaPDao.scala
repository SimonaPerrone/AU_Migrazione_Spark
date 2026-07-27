package it.eng.au.ERP.dao.hive.rcu

import it.eng.au.ERP.dao.hive.HiveDao
import it.eng.au.ERP.model.rcu.RcuAziendaPModel
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.rcu.RcuAziendaPSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class RcuAziendaPDao extends HiveDao[RcuAziendaPModel]{
  override val tableName: String = PropertyUtility.rcuAziendaPTable
  override val schema: SchemaEnum = RcuAziendaPSchema
}
