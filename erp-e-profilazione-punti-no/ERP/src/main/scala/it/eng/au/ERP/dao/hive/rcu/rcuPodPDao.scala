package it.eng.au.ERP.dao.hive.rcu

import it.eng.au.ERP.dao.hive.HiveDao
import it.eng.au.ERP.model.rcu.rcuPodPModel
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.rcu.rcuPodPSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class rcuPodPDao extends HiveDao[rcuPodPModel]{

  override val tableName: String = PropertyUtility.rcuPodPTable
  override val schema: SchemaEnum = rcuPodPSchema
}
