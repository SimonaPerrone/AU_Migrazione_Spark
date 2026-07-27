package it.eng.au.ERP.dao.hive.erp

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.erp.erpAggregatoNoSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class ErpAggregatoNoDao extends HDao {
  override val tableName: String = PropertyUtility.erpAggregatoNo
  override val schema: SchemaEnum = erpAggregatoNoSchema
}
