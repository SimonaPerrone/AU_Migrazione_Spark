package it.eng.au.ERP.dao.hive.erp

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.erp.erpAggregatoIPOSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class ErpAggregatoIPODao extends HDao{
  override val tableName: String = PropertyUtility.erpAggregatoIPOTable
  override val schema: SchemaEnum = erpAggregatoIPOSchema
}
