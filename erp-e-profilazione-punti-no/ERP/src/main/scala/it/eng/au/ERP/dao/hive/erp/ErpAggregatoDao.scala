package it.eng.au.ERP.dao.hive.erp

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.erp.erpAggregatoSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class ErpAggregatoDao extends HDao {
  override val tableName: String = PropertyUtility.erpAggregato
  override val schema: SchemaEnum = erpAggregatoSchema
}

