package it.eng.au.ERP.dao.hive.erp

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class ErpDetPodIntPubDao extends HDao {
  override val tableName: String = PropertyUtility.erpDetPodIntPub
  override val schema: SchemaEnum = erpDetPodIntPubSchema
}
