package it.eng.au.ERP.dao.hive.erp

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.erp.erpAggregatoOSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class ErpAggregatoODao extends HDao{
  override val tableName: String = PropertyUtility.erpAggregatoOTable
  override val schema: SchemaEnum = erpAggregatoOSchema
}
