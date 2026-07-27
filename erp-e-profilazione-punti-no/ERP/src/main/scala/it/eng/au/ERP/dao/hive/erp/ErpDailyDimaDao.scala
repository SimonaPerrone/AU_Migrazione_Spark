package it.eng.au.ERP.dao.hive.erp

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.erp.{erpDailyDimaSchema, erpDailyNoSchema}
import it.eng.au.ERP.utility.setting.PropertyUtility

class ErpDailyDimaDao extends HDao {
  override val tableName: String = PropertyUtility.erpDailyDima
  override val schema: SchemaEnum = erpDailyDimaSchema
}