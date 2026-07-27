package it.eng.au.ERP.dao.hive.erp

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.erp.{erpAggregatoINTSchema, erpValidatedIntSchema}
import it.eng.au.ERP.utility.setting.PropertyUtility

class ErpAggregatoIntDao extends HDao{
  override val tableName: String = PropertyUtility.erpAggregatoInt
  override val schema: SchemaEnum = erpAggregatoINTSchema
}
