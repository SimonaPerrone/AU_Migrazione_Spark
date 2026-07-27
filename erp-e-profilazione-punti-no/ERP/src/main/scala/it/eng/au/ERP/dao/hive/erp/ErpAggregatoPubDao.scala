package it.eng.au.ERP.dao.hive.erp

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.erp.erpAggregatoPubSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class ErpAggregatoPubDao extends HDao {
  override val tableName: String = PropertyUtility.erpAggregatoPub
  override val schema: SchemaEnum = erpAggregatoPubSchema
}

