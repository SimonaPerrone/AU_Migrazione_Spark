package it.eng.au.ERP.dao.hive.erp

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.erp.{erpAggregatoOSchema, erpValidatedIntSchema}
import it.eng.au.ERP.utility.setting.PropertyUtility

class ErpValidatedIntDao extends HDao{
  override val tableName: String = PropertyUtility.erpValidatedInt
  override val schema: SchemaEnum = erpValidatedIntSchema
}
