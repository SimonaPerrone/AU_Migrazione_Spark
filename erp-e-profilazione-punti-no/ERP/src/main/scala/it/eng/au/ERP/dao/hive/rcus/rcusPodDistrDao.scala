package it.eng.au.ERP.dao.hive.rcus

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.rcus.rcusPodDistrSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class rcusPodDistrDao extends HDao {
  override val tableName: String = PropertyUtility.rcusPodDistrTable
  override val schema: rcusPodDistrSchema.type = rcusPodDistrSchema
  override val writeEnabled: Boolean = false
}
