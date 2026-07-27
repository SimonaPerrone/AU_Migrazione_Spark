package it.eng.au.ERP.dao.hive.rcu

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.rcu.rcuPodDistrPSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class rcuPodDistrPDao extends HDao {
  override val tableName: String = PropertyUtility.rcuPodDistrPTable
  override val schema: rcuPodDistrPSchema.type = rcuPodDistrPSchema
  override val writeEnabled: Boolean = false
}
