package it.eng.au.ERP.dao.hive.erp

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.erp.erpDailyNoSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class ErpDailyNoDao extends HDao {
  override val tableName: String = PropertyUtility.erpDailyNo
  override val schema: SchemaEnum = erpDailyNoSchema
}