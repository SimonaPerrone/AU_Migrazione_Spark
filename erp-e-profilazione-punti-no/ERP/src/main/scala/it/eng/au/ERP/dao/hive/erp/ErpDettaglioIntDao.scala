package it.eng.au.ERP.dao.hive.erp

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.erp.{erpDettaglioINTSchema, erpValidatedIntSchema}
import it.eng.au.ERP.utility.setting.PropertyUtility

class ErpDettaglioIntDao extends HDao{
  override val tableName: String = PropertyUtility.erpDettaglioInt
  override val schema: SchemaEnum = erpDettaglioINTSchema
}
